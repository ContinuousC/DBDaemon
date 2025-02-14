use dbschema::{DbTable, DbTableId, HasSchema, Identified, SingleVersioned};
use serde::{Deserialize, Serialize};

pub const SCHEMA_TABLE: &DbTableId = &DbTableId::from_static("schemas");
pub type SchemaDocument = Identified<SingleVersioned<TableInfo>>;

#[derive(Serialize, Deserialize, HasSchema, Debug)]
pub struct TableInfo {
    #[dbschema(json)]
    pub schema: DbTable,
}
