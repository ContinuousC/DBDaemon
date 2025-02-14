/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use dbschema::{DbSchema, DbTable};
use serde_json::Value;

use super::error::Result;

#[derive(Debug)]
pub struct TableMapping {
    pub table: DbTable,
    pub table_schema: DbSchema, // Cached
    pub value_schema: DbSchema, // Cached
    pub sort_fields: Value,
}

impl TableMapping {
    pub fn new(table: DbTable) -> Self {
        Self {
            table_schema: table.schema(),
            value_schema: table.value_schema(),
            sort_fields: table.sort_fields(),
            table,
        }
    }

    pub fn verify_value(&self, value: &Value) -> Result<()> {
        Ok(self.value_schema.verify_value(value)?)
    }
}
