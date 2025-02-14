/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::ops::{Deref, DerefMut};

use chrono::{DateTime, Utc};
use dbschema::DbSchema;

use super::{table_read::TableReadGuard, updates::UpdateGuard};

pub struct DataWriteGuard<'a, T> {
    state: &'a TableReadGuard<'a>,
    now: DateTime<Utc>,
    transaction: T,
}

pub trait Transaction<'a>: Sized {
    type Value;
    fn commit(
        self,
        now: DateTime<Utc>,
        force_update: bool,
        value_schema: &DbSchema,
        updates: &mut UpdateGuard<'a, Self::Value>,
    );
}

impl<'a, T: Transaction<'a>> DataWriteGuard<'a, T> {
    pub fn new(state: &'a TableReadGuard<'a>, now: DateTime<Utc>, transaction: T) -> Self {
        Self {
            state,
            now,
            transaction,
        }
    }

    pub fn commit(self) -> UpdateGuard<'a, T::Value> {
        let mut updates = UpdateGuard::new(self.state);
        self.transaction.commit(
            self.now,
            self.state.mapping.table.force_update,
            &self.state.mapping.value_schema,
            &mut updates,
        );
        updates
    }
}

impl<T> Deref for DataWriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

impl<T> DerefMut for DataWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.transaction
    }
}
