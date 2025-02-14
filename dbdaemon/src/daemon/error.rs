/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::path::PathBuf;

use dbdaemon_api::VerificationId;
use thiserror::Error;

use dbschema::{DbTableId, ObjectId, VersioningType};

use crate::database::elastic::{self, ElasticId};

use super::table_state::TableNonOperationalState;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Unable to create a mapping of the schema: {0}")]
    MappingError(#[from] elastic::MappingError),
    #[error("Unable to Serialize/Deserialize the object: {0:?}")]
    JSONError(#[from] serde_json::Error),
    #[error("Encountered a problem converting the object {0:?}")]
    ConversionError(#[from] elastic::ConversionError),
    #[error("Table not found: {0}")]
    TableNotFound(DbTableId),
    #[error("Schema {0} is not available. Make sure it is present in /etc/dbdaemon/schemas and relead the daemon")]
    SchemaNotAvailable(String),
    #[error("Schema {0} is not registerd. Make sure it is registerd with the register_metric_schema-command in the backend")]
    SchemaNotRegisterd(String),
    #[error("Schema {0} is not available. Make sure it is present in /etc/dbdaemon/schemas and relead the daemon")]
    SchemaNotInDatabase(String),
    #[error("Table {0} is {1}")]
    TableNotReady(DbTableId, TableNonOperationalState),
    #[error("{0} not yet implemented")]
    NotYetImplented(String),
    #[error("error in the dbschema: {0:?}")]
    DbSchemaError(#[from] dbschema::Error),
    #[error("encountered a problen with the response of the database: {0}")]
    ResponseError(String),
    #[cfg(feature = "elastic")]
    #[error("elastic error: {0}")]
    Elastic(#[from] elastic::Error),
    #[error(
        "invalid query for {1} index '{0}'; this request is only \
	     available for {2} indices"
    )]
    WrongVersioningType(DbTableId, VersioningType, VersioningType),
    #[error("invalid method '{0} for metric table '{1}'")]
    NoTimeline(&'static str, DbTableId),
    #[error("invalid method '{0}' for  non-dual-versioned table '{1}'")]
    NoDualTimeline(&'static str, DbTableId),
    #[error("invalid method '{0}' for  timestamped] table '{1}'")]
    NotATimestampedTable(&'static str, DbTableId),
    #[error("object id '{1}' already exists in table '{0}'")]
    ObjectIdAlreadyExists(DbTableId, ObjectId),
    #[error("object id '{1}' does not exists in table '{0}'")]
    ObjectDoesNotExist(DbTableId, ObjectId),
    #[error("Failed to create schema directory '{0}': {1}")]
    CreateTableDir(PathBuf, std::io::Error),
    #[error("Failed to read schema directory '{0}': {1}")]
    ReadTableDir(PathBuf, std::io::Error),
    #[error("Failed to read table definition from '{0}': {1}")]
    ReadTable(PathBuf, std::io::Error),
    #[error("Failed to read table state from '{0}': {1}")]
    ReadState(PathBuf, std::io::Error),
    #[error("Invalid table name: {0}")]
    TableName(PathBuf),
    #[error("error in table definition '{0}': {1}")]
    TableFormat(PathBuf, serde_json::Error),
    #[error("failed to decode table state from '{0}': {1}")]
    StateFormat(PathBuf, serde_json::Error),
    #[error("{0}")]
    IOFileErrore(String),
    #[error(" unable to Deserialize config: {0}")]
    YAMLDeserialization(#[from] serde_yaml::Error),
    #[error("unable to convert path to string: {0}")]
    StringConversion(PathBuf),
    #[error("inconsistent data in table {0}, elastic id {1}")]
    InconsistentData(DbTableId, ElasticId),
    #[error("no verification with id {0} is currently in progress")]
    NoSuchVerificationWorker(VerificationId),
}
