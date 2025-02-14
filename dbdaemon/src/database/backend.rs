use std::{
    fmt::{Debug, Display},
    time::Duration,
};

use serde::{de::DeserializeOwned, Serialize};

use dbschema::{DbSchema, DbTable, DbTableId, Filter};
use serde_json::Value;

pub(crate) trait Database: Sized {
    type Error: std::error::Error;
    type Id: Display + Debug;
    type QueryState<'a>;

    /* Setup and configuration. */

    async fn wait_for_database(&self) -> Result<(), Self::Error>;
    async fn verify_database(&self) -> Result<(), Self::Error>;

    /* Schema manipulation. */

    async fn has_table(&self, id: &DbTableId) -> Result<bool, Self::Error>;

    async fn create_table(&self, id: &DbTableId, definition: &DbTable) -> Result<(), Self::Error>;

    async fn update_table(&self, id: &DbTableId, definition: &DbTable) -> Result<(), Self::Error>;

    async fn reindex_table(
        &self,
        id: &DbTableId,
        old_definition: &DbTable,
        new_definition: &DbTable,
    ) -> Result<(), Self::Error>;

    async fn remove_table(&self, id: &DbTableId) -> Result<(), Self::Error>;

    /* Data manipulation. */

    // async fn insert_object<T: Serialize + Send + Sync>(
    //     &self,
    //     table_id: &DbTableId,
    //     schema: &DbSchema,
    //     doc_id: &Self::Id,
    //     value: T,
    // ) -> Result<(), Self::Error>;

    async fn bulk_update<T, I>(
        &self,
        table_id: &DbTableId,
        schema: &DbSchema,
        updates: I,
    ) -> Result<(), Self::Error>
    where
        T: Serialize + Send + Sync,
        I: IntoIterator<Item = (Self::Id, u64, T)> + Send + Sync;

    // async fn query_object<T: DeserializeOwned + Send + Sync>(
    //     &self,
    //     table_id: &DbTableId,
    //     schema: &DbSchema,
    //     filter: &Filter,
    //     sort: &Value,
    // ) -> Result<(Self::Id, u64, T), Self::Error>;

    async fn update_object<T: Serialize + Send + Sync>(
        &self,
        table_id: &DbTableId,
        schema: &DbSchema,
        doc_id: &Self::Id,
        version: u64,
        value: T,
    ) -> Result<(), Self::Error>;

    async fn query_objects<T: DeserializeOwned + Send + Sync>(
        &self,
        table_id: &DbTableId,
        schema: &DbSchema,
        filter: &Filter,
        sort: &Value,
        limit: Option<usize>,
    ) -> Result<Vec<(Self::Id, u64, T)>, Self::Error>;

    async fn query_objects_first<'a, T: DeserializeOwned + Send + Sync>(
        &self,
        table_id: &DbTableId,
        schema: &'a DbSchema,
        filter: &'a Filter,
        sort: &'a Value, /* should be its own type */
        keep_alive: Duration,
        limit: Option<usize>,
    ) -> Result<(Vec<(Self::Id, u64, T)>, Option<Self::QueryState<'a>>), Self::Error>;

    async fn query_objects_next<'a, T: DeserializeOwned + Send + Sync>(
        &self,
        query_state: Self::QueryState<'a>,
    ) -> Result<(Vec<(Self::Id, u64, T)>, Option<Self::QueryState<'a>>), Self::Error>;
}
