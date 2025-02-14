/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

pub mod backend;

pub mod elastic;
pub mod mariadb;

pub(crate) use backend::Database;
