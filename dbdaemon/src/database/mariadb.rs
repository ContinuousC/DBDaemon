/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug)]
pub struct Database {}

impl Database {
    pub fn whoami(&self) -> String {
        String::from("MariaDB")
    }

    pub fn new(_config: DatabaseConfig) -> Result<Database, InitializationError> {
        Ok(Database {})
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DatabaseConfig {
    host: Option<String>,
    port: Option<u16>,
    username: Option<String>,
    password: Option<String>,
}

#[derive(Error, Debug)]
pub enum InitializationError {
    #[error("Unable to connect to database (username: {0})")]
    InvalidCredentials(String),
}
