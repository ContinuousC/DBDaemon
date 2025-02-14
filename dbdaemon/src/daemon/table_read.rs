use std::{borrow::Cow, ops::Deref};

use chrono::{DateTime, Utc};
use dbschema::DbTableId;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use tokio::sync::OwnedRwLockReadGuard;

use super::{
    data_read::DataReadGuard,
    data_write::DataWriteGuard,
    dual_versioned_data::{DualVersionedData, DualVersionedTransaction},
    error::{Error, Result},
    single_versioned_data::{SingleVersionedData, SingleVersionedTransaction},
    table_data::TableData,
    table_state::{TableOperationalState, TableState},
};

pub struct TableReadGuard<'a> {
    pub table_id: Cow<'a, DbTableId>,
    pub method: &'static str,
    state: OwnedRwLockReadGuard<TableState, TableOperationalState>,
}

impl<'a> TableReadGuard<'a> {
    pub fn new(
        table_id: &'a DbTableId,
        method: &'static str,
        state: OwnedRwLockReadGuard<TableState, TableOperationalState>,
    ) -> Self {
        Self {
            table_id: Cow::Borrowed(table_id),
            method,
            state,
        }
    }

    pub fn new_owned(
        table_id: DbTableId,
        method: &'static str,
        state: OwnedRwLockReadGuard<TableState, TableOperationalState>,
    ) -> Self {
        Self {
            table_id: Cow::Owned(table_id),
            method,
            state,
        }
    }

    pub fn read_data_single_versioned(&self) -> Result<DataReadGuard<'_, SingleVersionedData>> {
        let data = RwLockReadGuard::try_map(self.data.read(), TableData::single_versioned)
            .map_err(|_| Error::NoTimeline(self.method, (*self.table_id).clone()))?;
        Ok(DataReadGuard::new(self, data))
    }

    pub fn write_data_single_versioned(
        &'a self,
        now: DateTime<Utc>,
    ) -> Result<DataWriteGuard<'a, SingleVersionedTransaction<'a>>> {
        let data = RwLockWriteGuard::try_map(self.data.write(), TableData::single_versioned_mut)
            .map_err(|_| Error::NoTimeline(self.method, (*self.table_id).clone()))?;
        let transaction = SingleVersionedTransaction::new(data);
        Ok(DataWriteGuard::new(self, now, transaction))
    }

    pub fn read_data_dual_versioned(&self) -> Result<DataReadGuard<'_, DualVersionedData>> {
        let data = RwLockReadGuard::try_map(self.data.read(), TableData::dual_versioned)
            .map_err(|_| Error::NoTimeline(self.method, (*self.table_id).clone()))?;
        Ok(DataReadGuard::new(self, data))
    }

    pub fn write_data_dual_versioned(
        &'a self,
        now: DateTime<Utc>,
    ) -> Result<DataWriteGuard<'a, DualVersionedTransaction<'a>>> {
        let data = RwLockWriteGuard::try_map(self.data.write(), TableData::dual_versioned_mut)
            .map_err(|_| Error::NoTimeline(self.method, (*self.table_id).clone()))?;
        let transaction = DualVersionedTransaction::new(data);
        Ok(DataWriteGuard::new(self, now, transaction))
    }
}

impl Deref for TableReadGuard<'_> {
    type Target = TableOperationalState;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
