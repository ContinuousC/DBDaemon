use std::collections::HashMap;

use dbschema::{Identified, ObjectId};
use serde::Serialize;

use crate::database::{
    elastic::{self, ElasticId},
    Database,
};

use super::{error::Result, table_data::ElasticDoc, table_read::TableReadGuard};

pub struct UpdateGuard<'a, T>(
    &'a TableReadGuard<'a>,
    HashMap<ElasticId, (u64, Identified<T>)>,
);

impl<'a, T> UpdateGuard<'a, T> {
    pub fn new(state: &'a TableReadGuard<'a>) -> Self {
        Self(state, HashMap::new())
    }

    pub fn insert(&mut self, object_id: ObjectId, elastic_id: ElasticId, version: u64, value: T) {
        self.1
            .insert(elastic_id, (version, Identified::new_id(object_id, value)));
    }

    pub fn insert_doc(&mut self, object_id: ObjectId, doc: ElasticDoc<T>) {
        let ElasticDoc {
            elastic_id,
            version,
            value,
        } = doc;
        self.insert(object_id, elastic_id, version, value);
    }

    pub fn create(&mut self, object_id: ObjectId, value: T) -> ElasticDoc<T>
    where
        T: Clone,
    {
        let doc = ElasticDoc::new(value);
        self.insert_doc(object_id, doc.clone());
        doc
    }

    pub fn update<F>(&mut self, object_id: ObjectId, doc: ElasticDoc<T>, f: F) -> ElasticDoc<T>
    where
        F: FnOnce(T) -> T,
        T: Clone,
    {
        let doc = doc.update(f);
        self.insert_doc(object_id, doc.clone());
        doc
    }

    pub fn replace<F, G>(
        &mut self,
        object_id: ObjectId,
        doc: ElasticDoc<T>,
        prev: F,
        new: G,
    ) -> ElasticDoc<T>
    where
        F: FnOnce(T) -> T,
        G: FnOnce(T) -> T,
        T: Clone,
    {
        let (prev, new) = doc.update_new(prev, new);
        self.insert_doc(object_id.clone(), prev);
        self.insert_doc(object_id, new.clone());
        new
    }

    pub fn split<F, G>(
        &mut self,
        object_id: ObjectId,
        doc: ElasticDoc<T>,
        prev: F,
        new: G,
    ) -> (ElasticDoc<T>, ElasticDoc<T>)
    where
        F: FnOnce(T) -> T,
        G: FnOnce(T) -> T,
        T: Clone,
    {
        let (prev, new) = doc.update_new(prev, new);
        self.insert_doc(object_id.clone(), prev.clone());
        self.insert_doc(object_id, new.clone());
        (prev, new)
    }

    pub fn remove<F>(&mut self, object_id: ObjectId, doc: ElasticDoc<T>, f: F)
    where
        F: FnOnce(T) -> T,
        T: Clone,
    {
        let doc = doc.update(f);
        self.insert_doc(object_id, doc);
    }

    #[cfg(test)]
    pub fn extract(self) -> HashMap<ElasticId, (u64, Identified<T>)> {
        self.1
    }
}

impl<T: Send + Sync + Serialize> UpdateGuard<'_, T> {
    pub async fn run(self, elastic: &elastic::Database) -> Result<()> {
        // TODO: error handling!
        const CHUNK_SIZE: usize = 1000;

        let mut updates = self.1.into_iter();

        while updates.len() > 0 {
            if updates.len() > 1 {
                let chunk = (&mut updates).take(CHUNK_SIZE);
                elastic
                    .bulk_update(
                        self.0.table_id.as_ref(),
                        &self.0.mapping.table_schema,
                        chunk.map(|(id, (version, value))| (id, version, value)),
                    )
                    .await?;
            } else {
                for (elastic_id, (version, value)) in &mut updates {
                    elastic
                        .update_object(
                            self.0.table_id.as_ref(),
                            &self.0.mapping.table_schema,
                            &elastic_id,
                            version,
                            &value,
                        )
                        .await?;
                }
            }
        }

        Ok(())
    }
}
