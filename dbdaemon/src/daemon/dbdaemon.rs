use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use futures::StreamExt;
use parking_lot::RwLock;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use tracing::instrument;

use dbschema::{
    Anchor, Compatibility, DbTableId, DualVersionedValue, Filter, FilterPath, Identified, ObjectId,
    SingleVersionedValue, TimeRange, Timeline,
};

use crate::database::{backend::Database, elastic::ElasticId};
use dbdaemon_api::{BackendDbService, VerificationId, VerificationMsg, VersionProblem};
use dbdaemon_types::Operation;

#[cfg(feature = "elastic")]
use crate::database::elastic;
// #[cfg(feature = "mariadb")]
// use crate::database::mariadb;

use super::{
    filters::range_filter,
    schema_table::TableInfo,
    state::State,
    table_mapping::TableMapping,
    table_state::{TableNonOperationalState, TableOperationalState},
    Error,
};

pub struct DbDaemon {
    elastic: Arc<elastic::Database>,
    state: State,
    verification: RwLock<
        HashMap<VerificationId, Arc<AsyncMutex<tokio::sync::mpsc::Receiver<VerificationMsg>>>>,
    >,
}

impl DbDaemon {
    pub async fn new(config: elastic::DatabaseConfig) -> Result<DbDaemon, Error> {
        let elastic = Arc::new(elastic::Database::new(config).await?);
        let state = State::load(&elastic).await?;
        Ok(DbDaemon {
            elastic,
            state,
            verification: RwLock::new(HashMap::new()),
        })
    }
}

impl BackendDbService for DbDaemon {
    type Error = Error;

    /* Setup and configuration. */

    #[instrument(skip(self))]
    async fn wait_for_databases(&self) -> Result<(), Error> {
        self.elastic.wait_for_database().await?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn verify_databases(&self) -> Result<(), Error> {
        self.elastic.verify_database().await?;
        Ok(())
    }

    /* Schema manipulation. */

    #[instrument(skip(self))]
    async fn register_table(
        &self,
        table_id: dbschema::DbTableId,
        definition: dbschema::DbTable,
    ) -> Result<(), Self::Error> {
        let (schemas, mut table) = self
            .state
            .write_table(
                &table_id,
                "register_table",
                TableNonOperationalState::Registering,
                true,
            )
            .await?;

        let updated = match table.as_mut() {
            Some(table) if table.mapping.table == definition => None,
            Some(table) => match definition.verify_compatibility(&table.mapping.table)? {
                Compatibility::Compatible => {
                    self.elastic.update_table(&table_id, &definition).await?;
                    table.mapping = TableMapping::new(definition);
                    Some(table)
                }
                Compatibility::NeedsReindex => {
                    self.elastic
                        .reindex_table(&table_id, &table.mapping.table, &definition)
                        .await?;
                    table.mapping = TableMapping::new(definition);
                    Some(table)
                }
            },
            None => {
                if let Err(e) = self.elastic.create_table(&table_id, &definition).await {
                    log::warn!("Failed to create table for new schema: {e}");
                }

                let state =
                    TableOperationalState::load(&self.elastic, &table_id, definition).await?;

                Some(table.or_insert_with(|| state))
            }
        };

        if let Some(updated) = updated {
            let object_id = ObjectId::from(table_id.to_string());
            let new_value = serde_json::to_value(TableInfo {
                schema: updated.mapping.table.clone(),
            })?;

            let updates = {
                let mut data = schemas.write_data_single_versioned(Utc::now())?;
                data.insert(&object_id, new_value);
                data.commit()
            };

            updates.run(&self.elastic).await?;
        }

        Ok(())
    }

    /// Warning: this removes all table data!
    /* TODO: Remove in release builds? */
    #[instrument(skip(self))]
    async fn unregister_table(&self, table_id: dbschema::DbTableId) -> Result<(), Self::Error> {
        let (schemas, mut table) = self
            .state
            .write_table(
                &table_id,
                "unregister_table",
                TableNonOperationalState::Unregistering,
                false,
            )
            .await?;
        if table.as_mut().is_some() {
            let object_id = ObjectId::from(table_id.to_string());
            let updates = {
                let mut data = schemas.write_data_single_versioned(Utc::now())?;
                data.remove(&object_id);
                data.commit()
            };

            updates.run(&self.elastic).await?;
            self.elastic.remove_table(&table_id).await?;
            table.remove();
        }
        Ok(())
    }

    #[instrument(skip(self))]
    async fn get_table_ids(
        &self,
    ) -> Result<std::collections::HashSet<dbschema::DbTableId>, Self::Error> {
        Ok(self.state.0.read().keys().cloned().collect())
    }

    #[instrument(skip(self))]
    async fn get_table_definitions(
        &self,
    ) -> Result<HashMap<dbschema::DbTableId, dbschema::DbTable>, Self::Error> {
        let tables = self.state.0.read().clone();
        Ok(futures::stream::iter(tables)
            .filter_map(|(k, v)| async move {
                Some((
                    k.clone(),
                    v.read().await.read(&k).ok()?.mapping.table.clone(),
                ))
            })
            .collect()
            .await)
    }

    #[instrument(skip(self))]
    async fn get_table_definition(
        &self,
        id: dbschema::DbTableId,
    ) -> Result<dbschema::DbTable, Self::Error> {
        let table = self.state.read_table(&id, "get_table_definition").await?;
        Ok(table.mapping.table.clone())
    }

    async fn verify_table_data_start(
        &self,
        table_id: DbTableId,
        range: Option<TimeRange>,
    ) -> Result<VerificationId, Self::Error> {
        let verification_id = VerificationId::new();
        let (sender, receiver) = tokio::sync::mpsc::channel(10);

        let table = self
            .state
            .read_table_owned(table_id.clone(), "verify_table_data")
            .await?;

        tokio::spawn({
            macro_rules! send_msg {
                ($sender:ident, $msg:expr) => {
                    if $sender.send($msg).await.is_err() {
                        return;
                    }
                };
            }

            macro_rules! handle_err {
                ($sender:ident, $expr:expr) => {
                    match $expr {
                        Ok(r) => r,
                        Err(e) => {
                            send_msg!($sender, VerificationMsg::Error(e.to_string()));
                            return;
                        }
                    }
                };
            }

            let elastic = self.elastic.clone();

            async move {
                let mut objects = HashMap::<ObjectId, (String, Option<DateTime<Utc>>)>::new();
                let sort = json!([{ "@active.from": { "order": "asc"} }]);
                let filter = match range {
                    Some(range) => FilterPath::new()
                        .field("value")
                        .field("version")
                        .field("active")
                        .filter(range_filter(range)),
                    None => Filter::All(Vec::new()),
                };

                let mut n = 0;
                let (mut docs, mut next) = handle_err!(
                    sender,
                    elastic
                        .query_objects_first::<Identified<SingleVersionedValue>>(
                            &table_id,
                            &table.mapping.table_schema,
                            &filter,
                            &sort,
                            std::time::Duration::from_secs(60),
                            Some(1000),
                        )
                        .await
                );

                loop {
                    n += docs.len();
                    for (elastic_id, _version, doc) in docs {
                        let Anchor { from, to, .. } = doc.value.version.active;
                        match objects.entry(doc.object_id.clone()) {
                            Entry::Occupied(mut ent) => {
                                let (prev_elastic_id, prev_to) = ent.get();
                                let version_problem = || VersionProblem {
                                    object_id: doc.object_id.clone(),
                                    prev_version_id: prev_elastic_id.to_string(),
                                    cur_version_id: elastic_id.to_string(),
                                    prev_to: *prev_to,
                                    cur_from: from,
                                };
                                if prev_to.as_ref().is_none_or(|prev_to| prev_to > &from) {
                                    send_msg!(
                                        sender,
                                        VerificationMsg::Overlap(Box::new(version_problem()))
                                    );
                                } else if prev_to
                                    .as_ref()
                                    .is_some_and(|prev_to| prev_to < &doc.value.version.active.from)
                                {
                                    send_msg!(
                                        sender,
                                        VerificationMsg::Gap(Box::new(version_problem()))
                                    );
                                }

                                ent.insert((elastic_id.to_string(), to));
                            }
                            Entry::Vacant(ent) => {
                                ent.insert((elastic_id.to_string(), to));
                            }
                        }
                    }

                    send_msg!(sender, VerificationMsg::Progress(n as u64));

                    match next {
                        Some(query_state) => {
                            (docs, next) =
                                handle_err!(sender, elastic.query_objects_next(query_state).await);
                        }
                        None => break,
                    }
                }
            }
        });

        self.verification
            .write()
            .insert(verification_id, Arc::new(AsyncMutex::new(receiver)));
        Ok(verification_id)
    }

    async fn verify_table_data_next(
        &self,
        verification_id: VerificationId,
    ) -> Result<Option<Vec<VerificationMsg>>, Self::Error> {
        const BATCH_SIZE: usize = 10;
        let receiver = self
            .verification
            .read()
            .get(&verification_id)
            .ok_or_else(|| Error::NoSuchVerificationWorker(verification_id))?
            .clone();
        let mut msgs = Vec::with_capacity(BATCH_SIZE);
        let n = receiver.lock().await.recv_many(&mut msgs, BATCH_SIZE).await;
        Ok((n > 0).then_some(msgs))
    }

    /* Metric (timestamped) data manipulation. */

    #[instrument(skip(self))]
    async fn bulk_insert_timestamped_objects(
        &self,
        table_id: DbTableId,
        values: Vec<Value>,
    ) -> Result<(), Self::Error> {
        /* Get and verify table. */
        let table = self
            .state
            .read_table(&table_id, "bulk_insert_timestamped_objects")
            .await?;

        /* Verify all values. */
        // values
        //     .iter()
        //     .try_for_each(|v| table.table_schema.verify_value(v))?;

        /* Prepare bulk request */

        self.elastic
            .bulk_update(
                &table_id,
                &table.mapping.table_schema,
                values.into_iter().map(|v| (ElasticId::new(), 0, v)),
            )
            .await?;

        Ok(())
    }

    /* Discovery object (single-versioned) manipulation. */

    #[instrument(skip(self))]
    async fn create_discovery_object(
        &self,
        table_id: DbTableId,
        value: Value,
    ) -> Result<ObjectId, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "create_discovery_object")
            .await?;

        table.mapping.verify_value(&value)?;

        let object_id = ObjectId::new();
        let updates = {
            let mut data = table.write_data_single_versioned(Utc::now())?;
            data.create(&object_id, value);
            data.commit()
        };

        updates.run(&self.elastic).await?;
        Ok(object_id)
    }

    #[instrument(skip(self))]
    async fn create_discovery_object_with_id(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        value: Value,
    ) -> Result<(), Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "create_discovery_object_with_id")
            .await?;

        table.mapping.verify_value(&value)?;

        let updates = {
            let mut data = table.write_data_single_versioned(Utc::now())?;
            data.create(&object_id, value)
                .then_some(())
                .ok_or_else(|| Error::ObjectIdAlreadyExists(table_id.clone(), object_id.clone()))?;
            data.commit()
        };

        updates.run(&self.elastic).await
    }

    #[instrument(skip(self))]
    async fn create_or_update_discovery_object(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        value: Value,
    ) -> Result<(), Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "create_or_update_discovery_object")
            .await?;

        table.mapping.verify_value(&value)?;

        let updates = {
            let mut data = table.write_data_single_versioned(Utc::now())?;
            data.insert(&object_id, value);
            data.commit()
        };

        updates.run(&self.elastic).await
    }

    #[instrument(skip(self))]
    async fn update_discovery_object(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        value: Value,
    ) -> Result<(), Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "update_discovery_object")
            .await?;

        table.mapping.verify_value(&value)?;

        let updates = {
            let mut data = table.write_data_single_versioned(Utc::now())?;
            data.update(&object_id, value)
                .then_some(())
                .ok_or_else(|| Error::ObjectDoesNotExist(table_id.clone(), object_id.clone()))?;
            data.commit()
        };

        updates.run(&self.elastic).await
    }

    #[instrument(skip(self))]
    async fn remove_discovery_object(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
    ) -> Result<(), Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "remove_discovery_object")
            .await?;

        let updates = {
            let mut data = table.write_data_single_versioned(Utc::now())?;
            data.remove(&object_id)
                .then_some(())
                .ok_or_else(|| Error::ObjectDoesNotExist(table_id.clone(), object_id.clone()))?;
            data.commit()
        };

        updates.run(&self.elastic).await
    }

    #[instrument(skip(self))]
    async fn bulk_update_discovery_objects(
        &self,
        table_id: DbTableId,
        updates: HashMap<ObjectId, Operation>,
    ) -> Result<(), Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "bulk_update_discovery_objects")
            .await?;

        updates.values().try_for_each(|op| match op {
            Operation::Create(v) | Operation::Update(v) | Operation::CreateOrUpdate(v) => {
                table.mapping.verify_value(v)
            }
            Operation::Remove => Ok(()),
        })?;

        let updates = {
            let mut data = table.write_data_single_versioned(Utc::now())?;
            updates
                .into_iter()
                .try_for_each(|(object_id, op)| match op {
                    Operation::Create(value) => {
                        data.create(&object_id, value).then_some(()).ok_or_else(|| {
                            Error::ObjectIdAlreadyExists(table_id.clone(), object_id.clone())
                        })
                    }
                    Operation::Update(value) => {
                        data.update(&object_id, value).then_some(()).ok_or_else(|| {
                            Error::ObjectDoesNotExist(table_id.clone(), object_id.clone())
                        })
                    }
                    Operation::CreateOrUpdate(value) => {
                        data.insert(&object_id, value);
                        Ok(())
                    }
                    Operation::Remove => data.remove(&object_id).then_some(()).ok_or_else(|| {
                        Error::ObjectDoesNotExist(table_id.clone(), object_id.clone())
                    }),
                })?;
            data.commit()
        };

        updates.run(&self.elastic).await

        // self.elastic
        //     .bulk_update(&table_id, &table.table_schema, req)
        //     .await?;
        // Ok(())
    }

    #[instrument(skip(self))]
    async fn read_discovery_object(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
    ) -> Result<SingleVersionedValue, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "read_discovery_object")
            .await?;

        let data = table.read_data_single_versioned()?;
        data.get(&object_id)
            .cloned()
            .ok_or_else(|| Error::ObjectDoesNotExist(table_id.clone(), object_id.clone()))
    }

    #[instrument(skip(self))]
    async fn read_discovery_object_maybe(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
    ) -> Result<Option<SingleVersionedValue>, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "read_discovery_object_maybe")
            .await?;

        let data = table.read_data_single_versioned()?;
        Ok(data.get(&object_id).cloned())
    }

    #[instrument(skip(self))]
    async fn read_discovery_objects(
        &self,
        table_id: DbTableId,
        object_ids: HashSet<ObjectId>,
    ) -> Result<HashMap<ObjectId, SingleVersionedValue>, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "read_discovery_objects")
            .await?;

        let data = table.read_data_single_versioned()?;
        Ok(object_ids
            .into_iter()
            .filter_map(|object_id| {
                let value = data.get(&object_id)?.clone();
                Some((object_id, value))
            })
            .collect())
    }

    #[instrument(skip(self))]
    async fn read_discovery_object_history(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        range: TimeRange,
    ) -> Result<Vec<SingleVersionedValue>, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "read_discovery_object_history")
            .await?;

        let filter = FilterPath::new()
            .field("object_id")
            .eq(json!(object_id))
            .and(Filter::at(
                FilterPath::new()
                    .field("value")
                    .field("version")
                    .field("active"),
                range_filter(range),
            ));
        Ok(self
            .elastic
            .query_objects::<Identified<SingleVersionedValue>>(
                &table_id,
                &table.mapping.table_schema,
                &filter,
                &table.mapping.sort_fields,
                None,
            )
            .await?
            .into_iter()
            .map(|(_id, _version, value)| value.value)
            .collect())
    }

    #[instrument(skip(self))]
    async fn read_discovery_objects_history(
        &self,
        table_id: DbTableId,
        object_ids: HashSet<ObjectId>,
        range: TimeRange,
    ) -> Result<HashMap<ObjectId, Vec<SingleVersionedValue>>, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "read_discovery_objects_history")
            .await?;

        let filter = FilterPath::new()
            .field("object_id")
            .eq_any(object_ids.into_iter().map(|id| json!(id)).collect())
            .and(Filter::at(
                FilterPath::new()
                    .field("value")
                    .field("version")
                    .field("active"),
                range_filter(range),
            ));
        Ok(self
            .elastic
            .query_objects::<Identified<SingleVersionedValue>>(
                &table_id,
                &table.mapping.table_schema,
                &filter,
                &table.mapping.sort_fields,
                None,
            )
            .await?
            .into_iter()
            .fold(
                HashMap::<_, Vec<_>>::new(),
                |mut map, (_id, _version, obj)| {
                    map.entry(obj.object_id).or_default().push(obj.value);
                    map
                },
            ))
    }

    #[instrument(skip(self))]
    async fn read_discovery_object_at(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        timestamp: DateTime<Utc>,
    ) -> Result<Option<SingleVersionedValue>, Self::Error> {
        Ok(self
            .read_discovery_object_history(table_id, object_id, TimeRange::at(timestamp))
            .await?
            .into_iter()
            .next())
    }

    #[instrument(skip(self))]
    async fn read_discovery_objects_at(
        &self,
        table_id: DbTableId,
        object_ids: HashSet<ObjectId>,
        timestamp: DateTime<Utc>,
    ) -> Result<HashMap<ObjectId, SingleVersionedValue>, Self::Error> {
        Ok(self
            .read_discovery_objects_history(table_id, object_ids, TimeRange::at(timestamp))
            .await?
            .into_iter()
            .filter_map(|(key, vals)| vals.into_iter().next().map(|val| (key, val)))
            .collect())
    }

    #[instrument(skip(self))]
    async fn query_discovery_objects(
        &self,
        table_id: DbTableId,
        filter: Filter,
    ) -> Result<HashMap<ObjectId, SingleVersionedValue>, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "query_discovery_objects")
            .await?;

        let data = table.read_data_single_versioned()?;
        data.iter()
            .map(|(object_id, value)| {
                Ok(filter
                    .matches(&table.mapping.value_schema, &value.value)?
                    .then(|| (object_id.clone(), value.clone())))
            })
            .filter_map(Result::transpose)
            .collect()
    }

    #[instrument(skip(self))]
    async fn query_discovery_objects_history(
        &self,
        table_id: DbTableId,
        filter: Filter,
        range: TimeRange,
    ) -> Result<HashMap<ObjectId, Vec<SingleVersionedValue>>, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "query_discovery_objects_history")
            .await?;

        let filter = FilterPath::new()
            .field("value")
            .field("value")
            .filter(filter)
            .and(
                FilterPath::new()
                    .field("value")
                    .field("version")
                    .field("active")
                    .filter(range_filter(range)),
            );

        Ok(self
            .elastic
            .query_objects::<Identified<SingleVersionedValue>>(
                &table_id,
                &table.mapping.table_schema,
                &filter,
                &table.mapping.sort_fields,
                None,
            )
            .await?
            .into_iter()
            .fold(
                HashMap::<_, Vec<_>>::new(),
                |mut map, (_id, _version, obj)| {
                    map.entry(obj.object_id).or_default().push(obj.value);
                    map
                },
            ))
    }

    #[instrument(skip(self))]
    async fn query_discovery_objects_at(
        &self,
        table_id: DbTableId,
        filter: Filter,
        timestamp: DateTime<Utc>,
    ) -> Result<HashMap<ObjectId, SingleVersionedValue>, Self::Error> {
        Ok(self
            .query_discovery_objects_history(table_id, filter, TimeRange::at(timestamp))
            .await?
            .into_iter()
            .filter_map(|(k, vs)| Some((k, vs.into_iter().next()?)))
            .collect())
    }

    /* Config object (dual-versioned) manipulation (singular). */

    async fn create_config_object(
        &self,
        table_id: DbTableId,
        value: Value,
        commit: bool,
    ) -> Result<ObjectId, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "create_config_object")
            .await?;

        table.mapping.verify_value(&value)?;

        let object_id = ObjectId::new();
        let updates = {
            let mut data = table.write_data_dual_versioned(Utc::now())?;
            data.create(object_id.clone(), value, commit);
            data.commit()
        };

        updates.run(&self.elastic).await?;
        Ok(object_id)
    }

    async fn create_config_object_with_id(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        value: Value,
        commit: bool,
    ) -> Result<(), Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "create_config_object")
            .await?;

        table.mapping.verify_value(&value)?;

        let updates = {
            let mut data = table.write_data_dual_versioned(Utc::now())?;
            data.create(object_id.clone(), value, commit)
                .then_some(())
                .ok_or_else(|| Error::ObjectIdAlreadyExists(table_id.clone(), object_id.clone()))?;
            data.commit()
        };

        updates.run(&self.elastic).await
    }

    async fn create_or_update_config_object(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        value: Value,
        commit: bool,
    ) -> Result<(), Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "create_or_update_config_object")
            .await?;

        table.mapping.verify_value(&value)?;

        let updates = {
            let mut data = table.write_data_dual_versioned(Utc::now())?;
            data.insert(object_id, value, commit);
            data.commit()
        };

        updates.run(&self.elastic).await
    }

    async fn update_config_object(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        value: Value,
        commit: bool,
    ) -> Result<(), Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "update_config_object")
            .await?;

        table.mapping.verify_value(&value)?;

        let updates = {
            let mut data = table.write_data_dual_versioned(Utc::now())?;
            data.update(object_id.clone(), value, commit)
                .then_some(())
                .ok_or_else(|| Error::ObjectDoesNotExist(table_id.clone(), object_id.clone()))?;
            data.commit()
        };

        updates.run(&self.elastic).await
    }

    async fn remove_config_object(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
    ) -> Result<(), Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "remove_config_object")
            .await?;

        let updates = {
            let mut data = table.write_data_dual_versioned(Utc::now())?;
            data.remove(object_id.clone())
                .then_some(())
                .ok_or_else(|| Error::ObjectDoesNotExist(table_id.clone(), object_id.clone()))?;
            data.commit()
        };

        updates.run(&self.elastic).await
    }

    async fn activate_config_object(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
    ) -> Result<(), Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "activate_config_object")
            .await?;

        let updates = {
            let mut data = table.write_data_dual_versioned(Utc::now())?;
            data.activate(object_id.clone())
                .then_some(())
                .ok_or_else(|| Error::ObjectDoesNotExist(table_id.clone(), object_id.clone()))?;
            data.commit()
        };

        updates.run(&self.elastic).await
    }

    async fn read_config_object(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        timeline: Timeline,
    ) -> Result<DualVersionedValue, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "read_config_object")
            .await?;
        let data = table.read_data_dual_versioned()?;
        data.get(&object_id, timeline)
            .cloned()
            .ok_or_else(|| Error::ObjectDoesNotExist(table_id.clone(), object_id.clone()))
    }

    async fn read_config_object_maybe(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        timeline: Timeline,
    ) -> Result<Option<DualVersionedValue>, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "read_config_object_maybe")
            .await?;
        let data = table.read_data_dual_versioned()?;
        Ok(data.get(&object_id, timeline).cloned())
    }

    async fn read_config_objects(
        &self,
        table_id: DbTableId,
        object_ids: HashSet<ObjectId>,
        timeline: Timeline,
    ) -> Result<HashMap<ObjectId, DualVersionedValue>, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "read_config_objects")
            .await?;
        let data = table.read_data_dual_versioned()?;
        Ok(object_ids
            .into_iter()
            .filter_map(|object_id| {
                let value = data.get(&object_id, timeline).cloned()?;
                Some((object_id, value))
            })
            .collect())
    }

    async fn read_config_object_history(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        timeline: Timeline,
        range: TimeRange,
    ) -> Result<Vec<DualVersionedValue>, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "read_config_object_history")
            .await?;
        let filter = FilterPath::new()
            .field("object_id")
            .eq(json!(object_id))
            .and(Filter::at(
                match timeline {
                    Timeline::Current => FilterPath::new()
                        .field("value")
                        .field("version")
                        .field("current"),
                    Timeline::Active => FilterPath::new()
                        .field("value")
                        .field("version")
                        .field("active")
                        .some(),
                },
                range_filter(range),
            ));
        Ok(self
            .elastic
            .query_objects::<Identified<DualVersionedValue>>(
                &table_id,
                &table.mapping.table_schema,
                &filter,
                &table.mapping.sort_fields,
                None,
            )
            .await?
            .into_iter()
            .map(|(_, _, value)| value.value)
            .collect())
    }

    async fn read_config_objects_history(
        &self,
        table_id: DbTableId,
        object_ids: HashSet<ObjectId>,
        timeline: Timeline,
        range: TimeRange,
    ) -> Result<HashMap<ObjectId, Vec<DualVersionedValue>>, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "read_config_objects_history")
            .await?;
        let filter = FilterPath::new()
            .field("object_id")
            .eq_any(object_ids.into_iter().map(|id| json!(id)).collect())
            .and(Filter::at(
                match timeline {
                    Timeline::Current => FilterPath::new()
                        .field("value")
                        .field("version")
                        .field("current"),
                    Timeline::Active => FilterPath::new()
                        .field("value")
                        .field("version")
                        .field("active")
                        .some(),
                },
                range_filter(range),
            ));
        Ok(self
            .elastic
            .query_objects::<Identified<DualVersionedValue>>(
                &table_id,
                &table.mapping.table_schema,
                &filter,
                &table.mapping.sort_fields,
                None,
            )
            .await?
            .into_iter()
            .fold(HashMap::<_, Vec<_>>::new(), |mut map, (_, _, obj)| {
                map.entry(obj.object_id).or_default().push(obj.value);
                map
            }))
    }

    async fn read_config_object_at(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        timeline: Timeline,
        timestamp: DateTime<Utc>,
    ) -> Result<Option<DualVersionedValue>, Self::Error> {
        Ok(self
            .read_config_object_history(table_id, object_id, timeline, TimeRange::at(timestamp))
            .await?
            .into_iter()
            .next())
    }

    async fn read_config_objects_at(
        &self,
        table_id: DbTableId,
        object_ids: HashSet<ObjectId>,
        timeline: Timeline,
        timestamp: DateTime<Utc>,
    ) -> Result<HashMap<ObjectId, DualVersionedValue>, Self::Error> {
        Ok(self
            .read_config_objects_history(table_id, object_ids, timeline, TimeRange::at(timestamp))
            .await?
            .into_iter()
            .filter_map(|(key, vals)| vals.into_iter().next().map(|val| (key, val)))
            .collect())
    }

    async fn query_config_objects(
        &self,
        table_id: DbTableId,
        filter: Filter,
        timeline: Timeline,
    ) -> Result<HashMap<ObjectId, DualVersionedValue>, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "read_config_objects")
            .await?;
        let data = table.read_data_dual_versioned()?;
        data.iter(timeline)
            .map(|(object_id, value)| {
                Ok(filter
                    .matches(&table.mapping.value_schema, &value.value)?
                    .then(|| (object_id.clone(), value.clone())))
            })
            .filter_map(Result::transpose)
            .collect()
    }

    async fn query_config_objects_history(
        &self,
        table_id: DbTableId,
        filter: Filter,
        timeline: Timeline,
        range: TimeRange,
    ) -> Result<HashMap<ObjectId, Vec<DualVersionedValue>>, Self::Error> {
        let table = self
            .state
            .read_table(&table_id, "query_config_objects_history")
            .await?;

        let filter = FilterPath::new()
            .field("value")
            .field("value")
            .filter(filter)
            .and(Filter::at(
                match timeline {
                    Timeline::Current => FilterPath::new()
                        .field("value")
                        .field("version")
                        .field("current"),
                    Timeline::Active => FilterPath::new()
                        .field("value")
                        .field("version")
                        .field("active")
                        .some(),
                },
                range_filter(range),
            ));
        Ok(self
            .elastic
            .query_objects::<Identified<DualVersionedValue>>(
                &table_id,
                &table.mapping.table_schema,
                &filter,
                &table.mapping.sort_fields,
                None,
            )
            .await?
            .into_iter()
            .fold(HashMap::<_, Vec<_>>::new(), |mut map, (_, _, obj)| {
                map.entry(obj.object_id).or_default().push(obj.value);
                map
            }))
    }

    async fn query_config_objects_at(
        &self,
        table_id: DbTableId,
        filter: Filter,
        timeline: Timeline,
        timestamp: DateTime<Utc>,
    ) -> Result<HashMap<ObjectId, DualVersionedValue>, Self::Error> {
        Ok(self
            .query_config_objects_history(table_id, filter, timeline, TimeRange::at(timestamp))
            .await?
            .into_iter()
            .filter_map(|(k, vs)| Some((k, vs.into_iter().next()?)))
            .collect())
    }
}
