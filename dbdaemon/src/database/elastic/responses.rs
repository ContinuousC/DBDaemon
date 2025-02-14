/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::fmt::Display;

use chrono::{DateTime, Utc};
use dbschema::DbTableId;
use dbschema_elastic::ElasticValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::backend::ElasticId;

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub status: u16,
    pub error: ErrorDescription,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorDescription {
    pub r#type: String,
    pub reason: String,
    // pub index: Option<String>,
    // pub index_uuid: String,
    // pub shard: u16,
    // pub root_cause
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterInfoResponse {
    pub name: String,
    pub cluster_name: String,
    pub cluster_uuid: String,
    pub version: ClusterVersion,
    pub tagline: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterVersion {
    #[serde(default)]
    pub distribution: ClusterDistribution,
    pub number: String,
    // pub build_flavor: String,
    pub build_type: String,
    pub build_hash: String,
    pub build_date: DateTime<Utc>,
    pub build_snapshot: bool,
    pub lucene_version: String,
    pub minimum_wire_compatibility_version: String,
    pub minimum_index_compatibility_version: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClusterDistribution {
    #[default]
    Elasticsearch,
    OpenSearch,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexResponse {
    pub acknowledged: bool,
    pub index: DbTableId,
    pub shards_acknowledged: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RefreshResponse {
    #[serde(rename = "_shards")]
    shards: Shards,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DocumentResponse {
    #[serde(rename = "_id")]
    pub id: ElasticId,
    #[serde(rename = "_index")]
    pub index: DbTableId,
    #[serde(rename = "_shards")]
    pub shards: Shards,
    pub result: DocumentResult,
    // _primary_term: 1,
    // _seq_no: 0,
    // "_type": "_doc",
    // "_version": 1,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocumentResult {
    Created,
    Updated,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
    #[serde(rename = "_shards")]
    pub shards: Shards,
    pub hits: Hits,
    pub timed_out: bool,
    pub took: u64,
    pub pit_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateByQueryResponse {
    pub took: u64,
    pub timed_out: bool,
    pub total: u64,
    pub updated: u64,
    pub deleted: u64,
    pub batches: u64,
    pub version_conflicts: u64,
    pub noops: u64,
    pub retries: u64,
    pub throttled_millis: u64,
    pub requests_per_second: u64,
    pub throttled_until_millis: u64,
    //pub failures: Vec<???>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PitResponse {
    // Elasticsearch: id
    // Opensearch: pit_id
    #[serde(alias = "pit_id")]
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Shards {
    pub failed: u32,
    pub skipped: Option<u32>,
    pub successful: u32,
    pub total: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Hits {
    pub hits: Vec<ElasticObject>,
    pub max_score: Option<f32>,
    pub total: Total,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Total {
    pub relation: String,
    pub value: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ElasticObject {
    #[serde(rename = "_id")]
    pub id: ElasticId,
    #[serde(rename = "_version")]
    pub version: u64,
    #[serde(rename = "_index")]
    pub index: String,
    #[serde(rename = "_score")]
    pub score: Option<f32>,
    #[serde(rename = "_source")]
    pub source: ElasticValue,
    pub sort: Option<Value>,
    /* Removed in ES8. */
    //#[serde(rename = "_type")]
    //pub typ: String
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BulkReponse {
    pub errors: bool,
    pub items: Vec<BulkItem>,
    pub took: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BulkItem {
    Index(BulkItemResult),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BulkItemResult {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_index")]
    index: String,
    #[serde(rename = "_primary_term")]
    primary_term: u64,
    #[serde(rename = "_seq_no")]
    seq_no: u64,
    #[serde(rename = "_shards")]
    shards: Shards,
    #[serde(rename = "_version")]
    version: u64,
    result: DocumentResult,
    status: u64,
}

impl BulkItem {
    pub fn status(&self) -> u64 {
        match self {
            BulkItem::Index(res) => res.status,
        }
    }
}

impl Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error.reason)
    }
}
