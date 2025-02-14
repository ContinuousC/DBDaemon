/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::{convert::TryFrom, io::Write};

use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;
use serde_json::{json, Value};

use super::error::{Error, Result};

#[derive(Debug)]
pub enum BulkOp<'a, T = Value> {
    Create {
        index: Option<&'a str>,
        id: &'a str,
        //version_type: VersionType,
        version: u64,
        value: &'a T,
    },
    Index {
        index: Option<&'a str>,
        id: &'a str,
        //version_type: VersionType,
        version: u64,
        value: &'a T,
    },
    Delete {
        index: Option<&'a str>,
        id: &'a str,
        //version_type: VersionType,
        version: u64,
    },
    // Update {
    // 	index: Option<String>,
    // 	id: Option<String>,
    // 	value: Partial<T>
    // },
}

// #[derive(Serialize, Debug)]
// #[serde(rename_all = "snake_case")]
// pub enum VersionType {
//     External,
// }

impl<T: Serialize> TryFrom<BulkOp<'_, T>> for Bytes {
    type Error = Error;
    fn try_from(op: BulkOp<T>) -> Result<Self> {
        let mut r = BytesMut::new().writer();
        op.write(&mut r)?;
        Ok(r.into_inner().freeze())
    }
}

impl<T> BulkOp<'_, T>
where
    T: Serialize,
{
    pub fn write<W: Write>(&self, mut writer: W) -> Result<()> {
        match self {
            BulkOp::Create {
                index,
                id,
                value,
                version,
            } => {
                serde_json::to_writer(
                    &mut writer,
                    &json!({"create": {
                        "_index": index,
                        "_id": id,
                        "version_type": "external",
                        "version": version
                    }}),
                )?;
                writer.write_all(b"\n")?;
                serde_json::to_writer(&mut writer, value)?;
                writer.write_all(b"\n")?;
            }
            BulkOp::Index {
                index,
                id,
                value,
                version,
            } => {
                serde_json::to_writer(
                    &mut writer,
                    &json!({"index": {
                        "_index": index,
                        "_id": id,
                        "version_type": "external",
                        "version": version
                    }}),
                )?;
                writer.write_all(b"\n")?;
                serde_json::to_writer(&mut writer, value)?;
                writer.write_all(b"\n")?;
            }
            BulkOp::Delete { index, id, version } => {
                serde_json::to_writer(
                    &mut writer,
                    &json!({"delete": {
                        "_index": index,
                        "_id": id,
                        "version_type": "external",
                        "version": version
                    }}),
                )?;
                writer.write_all(b"\n")?;
            }
        }
        Ok(())
    }
}
