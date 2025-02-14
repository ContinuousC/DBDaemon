use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

// use chrono::{DateTime, Utc};
// use serde_json::Value;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
//use etc_base::{PackageName, PackageVersion};
//use rule_engine::{form::Rule, ItemTypeId, RuleId};
use serde_json::Value;

use dbschema::{
    DbTable, DbTableId, DualVersionedValue, Filter, ObjectId, SingleVersionedValue, TimeRange,
    Timeline,
};
use rpc::rpc;

use dbdaemon_types::Operation;
use uuid::Uuid;

pub type DbServer = rpc::AsyncServer<BackendDbProto>;
pub type DbClient = BackendDbServiceStub<
    rpc::AsyncClient<BackendDbProto, serde_json::Value, ()>,
    serde_json::Value,
>;

#[rpc(service, stub(javascript, python), log_errors)]
// TODO: prevent race conditions with locks (experation of 5-10min ==> error)
// TODO: add update schema
pub trait BackendDbService {
    /* Setup and configuration. */

    async fn wait_for_databases(&self);
    async fn verify_databases(&self);

    /* Schema manipulation and retrieval. */

    async fn register_table(&self, id: DbTableId, definition: DbTable);

    async fn unregister_table(&self, id: DbTableId);

    async fn get_table_ids(&self) -> HashSet<DbTableId>;

    async fn get_table_definitions(&self) -> HashMap<DbTableId, DbTable>;

    async fn get_table_definition(&self, id: DbTableId) -> DbTable;

    async fn verify_table_data_start(
        &self,
        table_id: DbTableId,
        range: Option<TimeRange>,
    ) -> VerificationId;

    async fn verify_table_data_next(
        &self,
        verification_id: VerificationId,
    ) -> Option<Vec<VerificationMsg>>;

    /* Metric object (timestamped) manipulation. */

    async fn bulk_insert_timestamped_objects(&self, table_id: DbTableId, values: Vec<Value>);

    // async fn create_metric(
    //     &self,
    //     table_id: DbTableId,
    //     value: Value,
    //     timestamp: Option<DateTime<Utc>>,
    // ) -> Result<(), Self::Error>;

    // async fn read_item_metrics(
    //     &self,
    //     mp_id: String,
    //     queried_item_type: String,
    //     queried_item_id: String,
    // ) -> Result<
    //     HashMap<String, Timestamped<MetricsTable<Thresholded<Value, Value>>>>,
    //     Self::Error,
    // >;

    // async fn read_table_metrics(
    //     &self,
    //     mp_id: String,
    //     table_id: String,
    // ) -> Result<
    //     HashMap<String, Timestamped<MetricsTable<Thresholded<Value, Value>>>>,
    //     Self::Error,
    // >;

    // async fn read_item_metrics_history(
    //     &self,
    //     mp_id: String,
    //     table_id: String,
    //     queried_item_id: String,
    //     grouping: Grouping,
    //     range: TimeRange,
    // ) -> Result<
    //     Vec<Timestamped<HashMap<String, Metric<Thresholded<Value, Value>>>>>,
    //     Self::Error,
    // >;

    /* Discovery object (single-versioned) manipulation. */

    async fn create_discovery_object(&self, table_id: DbTableId, value: Value) -> ObjectId;

    async fn create_discovery_object_with_id(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        value: Value,
    );

    async fn create_or_update_discovery_object(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        value: Value,
    );

    async fn update_discovery_object(&self, table_id: DbTableId, object_id: ObjectId, value: Value);

    async fn remove_discovery_object(&self, table_id: DbTableId, object_id: ObjectId);

    async fn bulk_update_discovery_objects(
        &self,
        table_id: DbTableId,
        updates: HashMap<ObjectId, Operation>,
    );

    async fn read_discovery_object(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
    ) -> SingleVersionedValue;

    async fn read_discovery_object_maybe(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
    ) -> Option<SingleVersionedValue>;

    async fn read_discovery_objects(
        &self,
        table_id: DbTableId,
        object_ids: HashSet<ObjectId>,
    ) -> HashMap<ObjectId, SingleVersionedValue>;

    async fn read_discovery_object_history(
        &self,
        table_id: DbTableId,
        object_ids: ObjectId,
        range: TimeRange,
    ) -> Vec<SingleVersionedValue>;

    async fn read_discovery_objects_history(
        &self,
        table_id: DbTableId,
        object_ids: HashSet<ObjectId>,
        range: TimeRange,
    ) -> HashMap<ObjectId, Vec<SingleVersionedValue>>;

    async fn read_discovery_object_at(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        timestamp: DateTime<Utc>,
    ) -> Option<SingleVersionedValue>;

    async fn read_discovery_objects_at(
        &self,
        table_id: DbTableId,
        object_ids: HashSet<ObjectId>,
        timestamp: DateTime<Utc>,
    ) -> HashMap<ObjectId, SingleVersionedValue>;

    async fn query_discovery_objects(
        &self,
        table_id: DbTableId,
        filter: Filter,
    ) -> HashMap<ObjectId, SingleVersionedValue>;

    async fn query_discovery_objects_history(
        &self,
        table_id: DbTableId,
        filter: Filter,
        range: TimeRange,
    ) -> HashMap<ObjectId, Vec<SingleVersionedValue>>;

    async fn query_discovery_objects_at(
        &self,
        table_id: DbTableId,
        filter: Filter,
        timestamp: DateTime<Utc>,
    ) -> HashMap<ObjectId, SingleVersionedValue>;

    /* Config object (dual-versioned) manipulation. */

    async fn create_config_object(
        &self,
        table_id: DbTableId,
        value: Value,
        commit: bool,
    ) -> ObjectId;

    async fn create_config_object_with_id(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        value: Value,
        commit: bool,
    );

    async fn create_or_update_config_object(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        value: Value,
        commit: bool,
    );

    async fn update_config_object(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        value: Value,
        commit: bool,
    );

    async fn remove_config_object(&self, table_id: DbTableId, object_id: ObjectId);

    async fn activate_config_object(&self, table_id: DbTableId, object_id: ObjectId);

    async fn read_config_object(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        timeline: Timeline,
    ) -> DualVersionedValue;

    async fn read_config_object_maybe(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        timeline: Timeline,
    ) -> Option<DualVersionedValue>;

    async fn read_config_objects(
        &self,
        table_id: DbTableId,
        object_ids: HashSet<ObjectId>,
        timeline: Timeline,
    ) -> HashMap<ObjectId, DualVersionedValue>;

    async fn read_config_object_history(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        timeline: Timeline,
        range: TimeRange,
    ) -> Vec<DualVersionedValue>;

    async fn read_config_objects_history(
        &self,
        table_id: DbTableId,
        object_ids: HashSet<ObjectId>,
        timeline: Timeline,
        range: TimeRange,
    ) -> HashMap<ObjectId, Vec<DualVersionedValue>>;

    async fn read_config_object_at(
        &self,
        table_id: DbTableId,
        object_id: ObjectId,
        timeline: Timeline,
        timestamp: DateTime<Utc>,
    ) -> Option<DualVersionedValue>;

    async fn read_config_objects_at(
        &self,
        table_id: DbTableId,
        object_ids: HashSet<ObjectId>,
        timeline: Timeline,
        timestamp: DateTime<Utc>,
    ) -> HashMap<ObjectId, DualVersionedValue>;

    async fn query_config_objects(
        &self,
        table_id: DbTableId,
        filter: Filter,
        timeline: Timeline,
    ) -> HashMap<ObjectId, DualVersionedValue>;

    async fn query_config_objects_history(
        &self,
        table_id: DbTableId,
        filter: Filter,
        timeline: Timeline,
        range: TimeRange,
    ) -> HashMap<ObjectId, Vec<DualVersionedValue>>;

    async fn query_config_objects_at(
        &self,
        table_id: DbTableId,
        filter: Filter,
        timeline: Timeline,
        timestamp: DateTime<Utc>,
    ) -> HashMap<ObjectId, DualVersionedValue>;

    /* Configuration and thresholds. */

    // async fn load_package(
    //     &self,
    //     name: PackageName,
    //     version: PackageVersion,
    //     spec: String,
    // ) -> Result<(), Self::Error>;

    // async fn unload_package(
    //     &self,
    //     name: PackageName,
    // ) -> Result<(), Self::Error>;

    // async fn load_ruleset(
    //     &self,
    //     rule_id: RuleId,
    //     value: Vec<Rule>,
    // ) -> Result<(), Self::Error>;

    // async fn load_queryable_items(
    //     &self,
    //     item_type: ItemTypeId,
    //     items: HashMap<String, HashMap<String, Value>>,
    // ) -> Result<(), Self::Error>;
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy, Debug)]
pub struct VerificationId(Uuid);

impl VerificationId {
    // New definition involves randomness; not adding a `Default` instance!
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Display for VerificationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum VerificationMsg {
    Overlap(Box<VersionProblem>),
    Gap(Box<VersionProblem>),
    Progress(u64),
    Error(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VersionProblem {
    pub object_id: ObjectId,
    pub prev_version_id: String,
    pub cur_version_id: String,
    pub prev_to: Option<DateTime<Utc>>,
    pub cur_from: DateTime<Utc>,
}
