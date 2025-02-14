use std::collections::hash_map::Entry;
use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use tokio::sync::{OwnedRwLockReadGuard, RwLock as AsyncRwLock};

use dbschema::{DbTableId, HasTableDef};
use tracing::instrument;

use crate::database::{elastic, Database};

use super::error::{Error, Result};
use super::schema_table::{SchemaDocument, TableInfo, SCHEMA_TABLE};
use super::table_read::TableReadGuard;
use super::table_state::{TableNonOperationalState, TableOperationalState, TableState};
use super::table_write::TableWriteGuard;

pub struct State(pub RwLock<HashMap<DbTableId, Arc<AsyncRwLock<TableState>>>>);

impl State {
    #[cfg(test)]
    pub fn new() -> Self {
        Self(RwLock::new(HashMap::from_iter([(
            SCHEMA_TABLE.clone(),
            Arc::new(AsyncRwLock::new(TableState::new(
                TableOperationalState::new(SchemaDocument::table_def()),
            ))),
        )])))
    }

    pub async fn load(elastic: &elastic::Database) -> Result<Self> {
        elastic.wait_for_database().await?;

        // Load schema table.

        let mut tables = HashMap::new();

        let schema_table_def = SchemaDocument::table_def();

        if !elastic.has_table(SCHEMA_TABLE).await? {
            elastic
                .create_table(SCHEMA_TABLE, &schema_table_def)
                .await?;
        }

        log::info!("Loading schemas...");
        let mut schema_info =
            TableOperationalState::load(elastic, SCHEMA_TABLE, schema_table_def).await?;

        let schemas = schema_info
            .get_data_single_versioned()
            .ok_or_else(|| Error::NoTimeline("load", SCHEMA_TABLE.clone()))?
            .iter()
            .map(|(schema_id, doc)| {
                let table_info = serde_json::from_value::<TableInfo>(doc.value.clone())?;
                Ok((
                    DbTableId::from_string(schema_id.to_string()),
                    table_info.schema,
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        tables.insert(
            SCHEMA_TABLE.clone(),
            Arc::new(AsyncRwLock::new(TableState::new(schema_info))),
        );

        // Load other tables

        for (table_id, table_def) in schemas {
            log::info!("Loading {table_id}...");
            let state = TableOperationalState::load(elastic, &table_id, table_def).await?;
            tables.insert(table_id, Arc::new(AsyncRwLock::new(TableState::new(state))));
        }

        Ok(Self(RwLock::new(tables)))
    }

    #[instrument(skip(self))]
    pub async fn read_table<'a>(
        &'a self,
        table_id: &'a DbTableId,
        method: &'static str,
    ) -> Result<TableReadGuard<'a>> {
        let table_state = self
            .0
            .read()
            .get(table_id)
            .ok_or_else(|| Error::TableNotFound(table_id.clone()))?
            .clone();
        let table_state = table_state.read_owned().await;
        let _ = table_state.read(table_id)?;
        let oper_state = OwnedRwLockReadGuard::map(table_state, |s| {
            s.read(table_id).unwrap() // checked above
        });
        Ok(TableReadGuard::new(table_id, method, oper_state))
    }

    #[instrument(skip(self))]
    pub async fn read_table_owned<'a>(
        &'a self,
        table_id: DbTableId,
        method: &'static str,
    ) -> Result<TableReadGuard<'static>> {
        let table_state = self
            .0
            .read()
            .get(&table_id)
            .ok_or_else(|| Error::TableNotFound(table_id.clone()))?
            .clone();
        let table_state = table_state.read_owned().await;
        let _ = table_state.read(&table_id)?;
        let oper_state = OwnedRwLockReadGuard::map(table_state, |s| {
            s.read(&table_id).unwrap() // checked above
        });
        Ok(TableReadGuard::new_owned(table_id, method, oper_state))
    }

    #[instrument(skip(self))]
    pub async fn write_table<'a>(
        &'a self,
        table_id: &'a DbTableId,
        method: &'static str,
        non_oper_state: TableNonOperationalState,
        create: bool,
    ) -> Result<(TableReadGuard<'a>, TableWriteGuard<'a>)> {
        let (schemas, table) = {
            let mut tables = self.0.write();
            let schemas = tables
                .get(SCHEMA_TABLE)
                .ok_or_else(|| Error::TableNotFound(SCHEMA_TABLE.clone()))?
                .clone();

            let table = match tables.entry(table_id.clone()) {
                Entry::Occupied(ent) => Some(ent.get().clone()),
                Entry::Vacant(ent) if create => {
                    ent.insert(Arc::new(AsyncRwLock::new(TableState::NonOperational(
                        non_oper_state,
                    ))));
                    None
                }
                Entry::Vacant(_) => None,
            };

            (schemas, table)
        };

        let schemas = schemas.read_owned().await;
        let _ = schemas.read(SCHEMA_TABLE)?;
        let schemas = OwnedRwLockReadGuard::map(
            schemas,
            |state| state.read(SCHEMA_TABLE).unwrap(), // checked above
        );

        let table = match table {
            Some(state) => Some(state.write().await.take(table_id, non_oper_state)?),
            None => None,
        };

        Ok((
            TableReadGuard::new(SCHEMA_TABLE, method, schemas),
            TableWriteGuard::new(self, table_id, method, table),
        ))
    }

    /// Used from TableWriteGuard to replace table operational state.
    pub(crate) fn insert(&self, table_id: DbTableId, oper_state: TableOperationalState) {
        let prev = self.0.write().insert(
            table_id,
            Arc::new(AsyncRwLock::new(TableState::Operational(oper_state))),
        );
        debug_assert!(prev.is_some());
    }

    /// Used from TableWriteGuard to replace table operational state.
    pub(crate) fn remove(&self, table_id: &DbTableId) {
        let prev = self.0.write().remove(table_id);
        debug_assert!(prev.is_some());
    }
}
