use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Operation<T = Value> {
    Create(T),
    Update(T),
    CreateOrUpdate(T),
    Remove,
}

impl<T: Serialize> Operation<T> {
    pub fn json_serialized(&self) -> serde_json::Result<Operation<Value>> {
        Ok(match self {
            Operation::Create(v) => Operation::Create(serde_json::to_value(v)?),
            Operation::Update(v) => Operation::Update(serde_json::to_value(v)?),
            Operation::CreateOrUpdate(v) => Operation::CreateOrUpdate(serde_json::to_value(v)?),
            Operation::Remove => Operation::Remove,
        })
    }
}
