/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ElasticFilter, ElasticMapping};

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct CreateIndex {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aliasses: Option<HashMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mappings: Option<ElasticMapping>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<IndexSettings>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct IndexSettings {
    #[serde(rename = "index.mapping.total_fields.limit")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_fields_limit: Option<u64>,
    #[serde(rename = "index.mapping.nested_fields.limit")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nested_fields_limit: Option<u64>,
}

#[derive(Serialize, Debug)]
pub struct SearchRequest {
    pub query: ElasticFilter,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pit: Option<Pit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search_after: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Pit {
    pub id: String,
    pub keep_alive: String,
}
