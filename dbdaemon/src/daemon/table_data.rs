#[cfg(test)]
use dbschema::VersioningType;

use crate::database::elastic::ElasticId;

use super::{dual_versioned_data::DualVersionedData, single_versioned_data::SingleVersionedData};

#[derive(Debug)]
pub enum TableData {
    Timestamped,
    SingleTimeline(SingleVersionedData),
    DualTimeline(DualVersionedData),
}

#[derive(Clone, Debug)]
pub struct ElasticDoc<T> {
    pub elastic_id: ElasticId,
    pub version: u64,
    pub value: T,
}

impl<T> ElasticDoc<T> {
    pub fn new(value: T) -> Self {
        Self {
            elastic_id: ElasticId::new(),
            version: 0,
            value,
        }
    }

    pub fn update<F>(self, f: F) -> Self
    where
        F: FnOnce(T) -> T,
    {
        Self {
            elastic_id: self.elastic_id,
            version: self.version + 1,
            value: f(self.value),
        }
    }

    pub fn update_new<F, G>(self, prev: F, new: G) -> (Self, Self)
    where
        T: Clone,
        F: FnOnce(T) -> T,
        G: FnOnce(T) -> T,
    {
        let new = Self::new(new(self.value.clone()));
        let prev = self.update(prev);
        (prev, new)
    }
}

impl TableData {
    #[cfg(test)]
    pub fn new(versioning: VersioningType) -> Self {
        match versioning {
            VersioningType::Timestamped => Self::Timestamped,
            VersioningType::SingleTimeline => Self::SingleTimeline(SingleVersionedData::new()),
            VersioningType::DualTimeline => Self::DualTimeline(DualVersionedData::new()),
        }
    }

    pub fn single_versioned(&self) -> Option<&SingleVersionedData> {
        match self {
            Self::SingleTimeline(data) => Some(data),
            _ => None,
        }
    }
    pub fn single_versioned_mut(&mut self) -> Option<&mut SingleVersionedData> {
        match self {
            Self::SingleTimeline(data) => Some(data),
            _ => None,
        }
    }
    pub fn dual_versioned(&self) -> Option<&DualVersionedData> {
        match self {
            Self::DualTimeline(data) => Some(data),
            _ => None,
        }
    }
    pub fn dual_versioned_mut(&mut self) -> Option<&mut DualVersionedData> {
        match self {
            Self::DualTimeline(data) => Some(data),
            _ => None,
        }
    }
}
