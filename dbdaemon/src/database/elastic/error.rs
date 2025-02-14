use std::io;
use std::path::PathBuf;

use thiserror::Error;

use dbschema_elastic::{ConversionError, FilterError, MappingError};

use super::responses::ErrorResponse;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("unable connect to the elastic database: {0}")]
    Initialization(#[from] InitializationError),
    #[error("mapping error: {0}")]
    Mapping(#[from] MappingError),
    #[error("filter error: {0}")]
    Filter(#[from] FilterError),
    #[error("conversion error: {0}")]
    Conversion(#[from] ConversionError),
    #[error("schema error: {0}")]
    Schema(#[from] dbschema::Error),
    #[error("invalid method: {0}")]
    InvalidMethod(http::Method),
    #[error("failed request: {0}")]
    FailedRequest(serde_json::Value),
    #[error("Json (de)serialization error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Request error: {0}")]
    Request(#[from] reqwest::Error),
    #[error("Request error: {0}")]
    RequestMw(#[from] reqwest_middleware::Error),
    #[error("Received an error response from ElasticSearch: {0}")]
    EsError(ErrorResponse),
    #[error("Bulk update completely failed")]
    BulkUpdateComplete,
    #[error("Bulk update partially failed")]
    BulkUpdatePartial,
    #[error("Missing pit_id in query response")]
    MissingPitId,
    #[error("Missing sort field in query response")]
    MissingSortField,
    #[error("Query did not return any hits")]
    ZeroHits,
    #[error("Query returned multiple hits (expected one)")]
    ManyHits,
    #[error("Timeout")]
    Timeout,
    #[error("Not implemented!")]
    Unimplemented,
}

#[derive(Error, Debug)]
pub enum InitializationError {
    #[error("Failed to read config file '{0}': {1}")]
    ReadConfigIo(PathBuf, std::io::Error),
    #[error("Failed to read config file '{0}': {1}")]
    ReadConfigFmt(PathBuf, serde_yaml::Error),
    #[error("Unable to connect to database (username: {0})")]
    InvalidCredentials(String),
    #[error("Invalid elasticsearch url: {0}")]
    InvalidUrl(url::ParseError),
    #[error("Failed to read CA Certificate \"{0}\": {1}")]
    ReadCa(String, io::Error),
    #[error("Failed to read client certificate \"{0}\": {1}")]
    ReadClientCert(String, io::Error),
    #[error("Failed to read client certificate key \"{0}\": {1}")]
    ReadClientKey(String, io::Error),
    #[error("Failed to parse CA Certificate \"{0}\": {1}")]
    ParseCa(String, reqwest::Error),
    #[error("Failed to parse client certificate: {0}")]
    ParseClientCert(reqwest::Error),
    #[error("Failed to build HTTP(S) Client: {0}")]
    BuildClient(reqwest::Error),
}
