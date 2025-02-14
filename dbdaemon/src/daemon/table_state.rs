use std::fmt::Display;

use dbschema::{DbTable, DbTableId, VersioningType};
use parking_lot::RwLock;

use crate::database::elastic;

use super::{
    dual_versioned_data::DualVersionedData,
    error::{Error, Result},
    single_versioned_data::SingleVersionedData,
    table_data::TableData,
    table_mapping::TableMapping,
};

#[derive(Debug)]
pub enum TableState {
    Operational(TableOperationalState),
    NonOperational(TableNonOperationalState),
}

#[derive(Debug, Clone, Copy)]
pub enum TableNonOperationalState {
    Registering,
    Updating,
    Reloading,
    Reindexing,
    Unregistering,
}

#[derive(Debug)]
pub struct TableOperationalState {
    pub mapping: TableMapping,
    pub data: RwLock<TableData>,
}

impl TableState {
    pub fn new(oper_state: TableOperationalState) -> Self {
        Self::Operational(oper_state)
    }

    pub fn take(
        &mut self,
        table_id: &DbTableId,
        non_oper_state: TableNonOperationalState,
    ) -> Result<TableOperationalState> {
        match std::mem::replace(self, TableState::NonOperational(non_oper_state)) {
            TableState::Operational(oper_state) => Ok(oper_state),
            TableState::NonOperational(non_oper_state) => {
                *self = TableState::NonOperational(non_oper_state);
                Err(Error::TableNotReady(table_id.clone(), non_oper_state))
            }
        }
    }

    pub fn read(&self, table_id: &DbTableId) -> Result<&TableOperationalState> {
        match self {
            Self::Operational(state) => Ok(state),
            Self::NonOperational(state) => Err(Error::TableNotReady(table_id.clone(), *state)),
        }
    }
}

impl TableOperationalState {
    #[cfg(test)]
    pub fn new(table: DbTable) -> Self {
        Self {
            data: RwLock::new(TableData::new(table.versioning)),
            mapping: TableMapping::new(table),
        }
    }

    pub async fn load(
        elastic: &elastic::Database,
        table_id: &DbTableId,
        table_def: DbTable,
    ) -> Result<Self> {
        let mapping = TableMapping::new(table_def);
        elastic.refresh_table(table_id).await?;

        let data = match &mapping.table.versioning {
            VersioningType::Timestamped => TableData::Timestamped,
            VersioningType::SingleTimeline => TableData::SingleTimeline(
                SingleVersionedData::load(elastic, table_id, &mapping).await?,
            ),
            VersioningType::DualTimeline => {
                TableData::DualTimeline(DualVersionedData::load(elastic, table_id, &mapping).await?)
            }
        };
        Ok(Self {
            mapping,
            data: RwLock::new(data),
        })
    }

    pub fn get_data_single_versioned(&mut self) -> Option<&mut SingleVersionedData> {
        self.data.get_mut().single_versioned_mut()
    }
}

impl Display for TableNonOperationalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Registering => write!(f, "registering"),
            Self::Updating => write!(f, "updating"),
            Self::Reloading => write!(f, "reloading"),
            Self::Reindexing => write!(f, "reindexing"),
            Self::Unregistering => write!(f, "unregistering"),
        }
    }
}
