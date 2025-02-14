use std::collections::{hash_map::Entry, HashMap};

use chrono::{DateTime, Utc};
use dbschema::{DbSchema, DbTableId, Identified, ObjectId, SingleVersionedValue};
use parking_lot::MappedRwLockWriteGuard;
use serde_json::Value;

use crate::database::{elastic, Database};

use super::{
    data_write::Transaction, error::Result, filters::filter_active_single, modify::modify,
    table_data::ElasticDoc, table_mapping::TableMapping, updates::UpdateGuard,
};

#[derive(Debug)]
pub struct SingleVersionedData(HashMap<ObjectId, SingleVersionedDoc>);

pub struct SingleVersionedTransaction<'a> {
    data: MappedRwLockWriteGuard<'a, SingleVersionedData>,
    updates: HashMap<ObjectId, Option<Value>>,
}

type SingleVersionedDoc = ElasticDoc<SingleVersionedValue>;

impl SingleVersionedData {
    #[cfg(test)]
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub async fn load(
        elastic: &elastic::Database,
        table_id: &DbTableId,
        mapping: &TableMapping,
    ) -> Result<Self> {
        Ok(Self(
            elastic
                .query_objects::<Identified<SingleVersionedValue>>(
                    table_id,
                    &mapping.table_schema,
                    &filter_active_single(),
                    &mapping.sort_fields,
                    None,
                )
                .await?
                .into_iter()
                .map(|(elastic_id, version, doc)| {
                    (
                        doc.object_id,
                        ElasticDoc {
                            elastic_id,
                            version,
                            value: doc.value,
                        },
                    )
                })
                .collect(),
        ))
    }

    pub fn get(&self, object_id: &ObjectId) -> Option<&SingleVersionedValue> {
        Some(&self.0.get(object_id)?.value)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ObjectId, &SingleVersionedValue)> {
        self.0.iter().map(|(k, v)| (k, &v.value))
    }
}

impl<'a> SingleVersionedTransaction<'a> {
    pub fn new(data: MappedRwLockWriteGuard<'a, SingleVersionedData>) -> Self {
        Self {
            data,
            updates: HashMap::new(),
        }
    }

    // pub fn get(&self, object_id: &ObjectId) -> Option<&Value> {
    //     match self.updates.get(object_id) {
    //         Some(value) => value.as_ref(),
    //         None => Some(&self.data.get(object_id)?.value),
    //     }
    // }

    pub fn create(&mut self, object_id: &ObjectId, value: Value) -> bool {
        match self.updates.entry(object_id.clone()) {
            Entry::Vacant(ent) if !self.data.0.contains_key(object_id) => {
                ent.insert(Some(value));
                true
            }
            _ => false,
        }
    }

    pub fn update(&mut self, object_id: &ObjectId, value: Value) -> bool {
        match self.updates.entry(object_id.clone()) {
            Entry::Occupied(mut ent) => {
                ent.insert(Some(value));
                true
            }
            Entry::Vacant(ent) if self.data.0.contains_key(object_id) => {
                ent.insert(Some(value));
                true
            }
            _ => false,
        }
    }

    pub fn insert(&mut self, object_id: &ObjectId, value: Value) {
        self.updates.insert(object_id.clone(), Some(value));
    }

    pub fn remove(&mut self, object_id: &ObjectId) -> bool {
        match self.updates.entry(object_id.clone()) {
            Entry::Occupied(mut ent) if ent.get().is_some() => {
                ent.insert(None);
                true
            }
            Entry::Vacant(ent) if self.data.0.contains_key(object_id) => {
                ent.insert(None);
                true
            }
            _ => false,
        }
    }
}

impl<'a> Transaction<'a> for SingleVersionedTransaction<'a> {
    type Value = SingleVersionedValue;
    fn commit(
        mut self,
        now: DateTime<Utc>,
        force_update: bool,
        value_schema: &DbSchema,
        updates: &mut UpdateGuard<'a, Self::Value>,
    ) {
        for (object_id, update) in self.updates {
            match (self.data.0.entry(object_id), update) {
                (Entry::Occupied(mut active), Some(value))
                    if force_update
                        || !value_schema
                            .value_eq(&active.get().value.value, &value)
                            .unwrap_or_else(|e| {
                                log::warn!(
                                    "value_eq failed during transaction \
									 -- assuming value has changed: {e}"
                                );
                                false /* values should have been checked */
                            }) =>
                {
                    let object_id = active.key().clone();
                    modify(active.get_mut(), |active| {
                        updates.replace(
                            object_id,
                            active,
                            |v| v.remove(now),
                            |v| v.update(now, value),
                        )
                    });
                }
                (Entry::Occupied(active), None) => {
                    let object_id = active.key().clone();
                    updates.remove(object_id, active.remove(), |v| v.remove(now));
                }
                (Entry::Vacant(active), Some(value)) => {
                    let object_id = active.key().clone();
                    active.insert(updates.create(object_id, SingleVersionedValue::new(now, value)));
                }
                _ => {}
            }
        }
    }
}
