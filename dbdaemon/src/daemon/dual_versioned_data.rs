use std::collections::{hash_map::Entry, HashMap};

use dbschema::{DbSchema, DbTableId, DualVersionedValue, Identified, ObjectId, Timeline};
use parking_lot::MappedRwLockWriteGuard;
use serde_json::Value;

use crate::database::{
    elastic::{self, ElasticId},
    Database,
};

use super::{
    data_write::Transaction,
    error::{Error, Result},
    filters::{filter_active_dual, filter_current_dual},
    modify::{modify, modify_res},
    table_data::ElasticDoc,
    table_mapping::TableMapping,
};

#[derive(Debug)]
pub struct DualVersionedData(HashMap<ObjectId, DualVersionedObj>);

#[derive(Debug)]
pub struct DualVersionedTransaction<'a> {
    data: MappedRwLockWriteGuard<'a, DualVersionedData>,
    updates: HashMap<ObjectId, DualVersionedUpdate>,
}

/// Current / active state for dual versioned objects.
#[derive(Debug)]
enum DualVersionedObj<T = DualVersionedDoc> {
    /// Current but not active.
    Created { current: T, committed: bool },
    /// Active but not current.
    Removed { active: T },
    /// Active and current (different objects).
    Updated {
        active: T,
        current: T,
        committed: bool,
    },
    /// Active and current (same object).
    Activated { active: T },
}

#[derive(Debug)]
enum DualVersionedUpdate {
    Insert(Value, bool),
    Remove,
    Activate,
    ActivateInsert(Value, bool),
    ActivateRemove,
    InsertActivate(Value),
    InsertActivateInsert(Value, Value, bool),
    InsertActivateRemove(Value),
    RemoveActivate,
    RemoveActivateInsert(Value, bool),
}

type DualVersionedDoc = ElasticDoc<DualVersionedValue>;

impl DualVersionedData {
    #[cfg(test)]
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub async fn load(
        elastic: &elastic::Database,
        table_id: &DbTableId,
        mapping: &TableMapping,
    ) -> Result<Self> {
        Ok(Self(
            elastic
                .query_objects::<Identified<DualVersionedValue>>(
                    table_id,
                    &mapping.table_schema,
                    &filter_current_dual().or(filter_active_dual()),
                    &mapping.sort_fields,
                    None,
                )
                .await?
                .into_iter()
                .try_fold::<HashMap<ObjectId, DualVersionedObj>, _, Result<_>>(
                    HashMap::new(),
                    |mut map, (elastic_id, version, doc)| {
                        let obj =
                            DualVersionedObj::from_doc(elastic_id.clone(), version, doc.value)
                                .ok_or_else(|| {
                                    Error::InconsistentData(table_id.clone(), elastic_id.clone())
                                })?;
                        match map.entry(doc.object_id) {
                            Entry::Occupied(mut ent) => {
                                ent.get_mut().insert(obj).then_some(()).ok_or_else(|| {
                                    Error::InconsistentData(table_id.clone(), elastic_id.clone())
                                })?;
                            }
                            Entry::Vacant(ent) => {
                                ent.insert(obj);
                            }
                        }
                        Ok(map)
                    },
                )?,
        ))
    }

    pub fn get(&self, object_id: &ObjectId, timeline: Timeline) -> Option<&DualVersionedValue> {
        match timeline {
            Timeline::Current => self.get_current(object_id),
            Timeline::Active => self.get_active(object_id),
        }
    }

    pub fn get_current(&self, object_id: &ObjectId) -> Option<&DualVersionedValue> {
        Some(&self.0.get(object_id)?.get_current()?.value)
    }

    pub fn get_active(&self, object_id: &ObjectId) -> Option<&DualVersionedValue> {
        Some(&self.0.get(object_id)?.get_active()?.value)
    }

    pub fn iter(
        &self,
        timeline: Timeline,
    ) -> impl Iterator<Item = (&ObjectId, &DualVersionedValue)> {
        self.0
            .iter()
            .filter_map(move |(object_id, obj)| Some((object_id, &obj.get(timeline)?.value)))
    }

    // pub fn iter_current(
    //     &self,
    // ) -> impl Iterator<Item = (&ObjectId, &DualVersionedValue)> {
    //     self.0.iter().filter_map(|(object_id, obj)| {
    //         Some((object_id, &obj.get_current()?.value))
    //     })
    // }

    // pub fn iter_active(
    //     &self,
    // ) -> impl Iterator<Item = (&ObjectId, &DualVersionedValue)> {
    //     self.0.iter().filter_map(|(object_id, obj)| {
    //         Some((object_id, &obj.get_active()?.value))
    //     })
    // }
}

impl<T> DualVersionedObj<T> {
    fn created(current: T, committed: bool) -> Self {
        Self::Created { current, committed }
    }

    fn removed(active: T) -> Self {
        Self::Removed { active }
    }

    fn activated(active: T) -> Self {
        Self::Activated { active }
    }

    fn updated(active: T, current: T, committed: bool) -> Self {
        Self::Updated {
            active,
            current,
            committed,
        }
    }

    fn combine(self, other: Self) -> std::result::Result<Self, Self> {
        match (self, other) {
            (Self::Created { current, committed }, Self::Removed { active })
            | (Self::Removed { active }, Self::Created { current, committed }) => {
                Ok(Self::Updated {
                    active,
                    current,
                    committed,
                })
            }
            (this, _) => Err(this),
        }
    }

    fn insert(&mut self, other: Self) -> bool {
        modify_res(self, |this| match this.combine(other) {
            Ok(obj) => (obj, true),
            Err(this) => (this, false),
        })
    }

    fn get(&self, timeline: Timeline) -> Option<&T> {
        match timeline {
            Timeline::Current => self.get_current(),
            Timeline::Active => self.get_active(),
        }
    }

    fn get_current(&self) -> Option<&T> {
        match self {
            DualVersionedObj::Created { current, .. }
            | DualVersionedObj::Updated { current, .. }
            | DualVersionedObj::Activated { active: current } => Some(current),
            DualVersionedObj::Removed { .. } => None,
        }
    }

    // fn get_current_mut(&mut self) -> Option<&mut T> {
    //     match self {
    //         DualVersionedObj::Created { current, .. }
    //         | DualVersionedObj::Updated { current, .. }
    //         | DualVersionedObj::Activated { active: current } => Some(current),
    //         DualVersionedObj::Removed { .. } => None,
    //     }
    // }

    fn get_active(&self) -> Option<&T> {
        match self {
            DualVersionedObj::Created { .. } => None,
            DualVersionedObj::Updated { active, .. }
            | DualVersionedObj::Activated { active }
            | DualVersionedObj::Removed { active } => Some(active),
        }
    }

    // fn get_active_mut(&mut self) -> Option<&mut T> {
    //     match self {
    //         DualVersionedObj::Created { .. } => None,
    //         DualVersionedObj::Updated { active, .. }
    //         | DualVersionedObj::Activated { active }
    //         | DualVersionedObj::Removed { active } => Some(active),
    //     }
    // }
}

impl DualVersionedObj<ElasticDoc<DualVersionedValue>> {
    fn from_doc(elastic_id: ElasticId, version: u64, value: DualVersionedValue) -> Option<Self> {
        let is_current = value.version.current.to.is_none();
        let is_active = value
            .version
            .active
            .as_ref()
            .is_some_and(|active| active.to.is_none());
        let committed = value.version.committed.is_some();
        let doc = ElasticDoc {
            elastic_id,
            version,
            value,
        };
        match (is_current, is_active) {
            (true, true) => Some(Self::Activated { active: doc }),
            (false, true) => Some(Self::Removed { active: doc }),
            (true, false) => Some(Self::Created {
                current: doc,
                committed,
            }),
            (false, false) => None,
        }
    }
}

impl DualVersionedUpdate {
    fn get_current<'a, F>(&'a self, get_current: F) -> Option<&'a Value>
    where
        F: FnOnce() -> Option<&'a Value>,
    {
        match self {
            DualVersionedUpdate::Insert(value, _)
            | DualVersionedUpdate::ActivateInsert(value, _)
            | DualVersionedUpdate::InsertActivate(value)
            | DualVersionedUpdate::InsertActivateInsert(_, value, _)
            | DualVersionedUpdate::RemoveActivateInsert(value, _) => Some(value),
            DualVersionedUpdate::Remove
            | DualVersionedUpdate::ActivateRemove
            | DualVersionedUpdate::InsertActivateRemove(_)
            | DualVersionedUpdate::RemoveActivate => None,
            DualVersionedUpdate::Activate => get_current(),
        }
    }

    fn get_active<'a, F, G>(&'a self, get_current: F, get_active: G) -> Option<&'a Value>
    where
        F: FnOnce() -> Option<&'a Value>,
        G: FnOnce() -> Option<&'a Value>,
    {
        match self {
            DualVersionedUpdate::Insert(_, _) | DualVersionedUpdate::Remove => get_active(),
            DualVersionedUpdate::Activate
            | DualVersionedUpdate::ActivateInsert(_, _)
            | DualVersionedUpdate::ActivateRemove => get_current(),
            DualVersionedUpdate::InsertActivate(value)
            | DualVersionedUpdate::InsertActivateInsert(value, _, _)
            | DualVersionedUpdate::InsertActivateRemove(value) => Some(value),
            DualVersionedUpdate::RemoveActivate
            | DualVersionedUpdate::RemoveActivateInsert(_, _) => None,
        }
    }

    fn insert(&mut self, value: Value, commit: bool) {
        modify(self, |this| match this {
            Self::Insert(_, _) | Self::Remove => Self::Insert(value, commit),
            Self::Activate | Self::ActivateInsert(_, _) | Self::ActivateRemove => {
                Self::ActivateInsert(value, commit)
            }
            Self::InsertActivate(before)
            | Self::InsertActivateInsert(before, _, _)
            | Self::InsertActivateRemove(before) => {
                Self::InsertActivateInsert(before, value, commit)
            }
            Self::RemoveActivate | Self::RemoveActivateInsert(_, _) => {
                Self::RemoveActivateInsert(value, commit)
            }
        });
    }

    fn remove(&mut self) {
        modify(self, |this| match this {
            Self::Insert(_, _) | Self::Remove => Self::Remove,
            Self::Activate | Self::ActivateInsert(_, _) | Self::ActivateRemove => {
                Self::ActivateRemove
            }
            Self::InsertActivate(before)
            | Self::InsertActivateInsert(before, _, _)
            | Self::InsertActivateRemove(before) => Self::InsertActivateRemove(before),
            Self::RemoveActivate | Self::RemoveActivateInsert(_, _) => Self::RemoveActivate,
        });
    }

    fn activate(&mut self) {
        modify(self, |this| match this {
            Self::Activate => Self::Activate,
            Self::InsertActivate(value) => Self::InsertActivate(value),
            Self::RemoveActivate => Self::RemoveActivate,
            Self::Insert(value, _)
            | Self::ActivateInsert(value, _)
            | Self::InsertActivateInsert(_, value, _)
            | Self::RemoveActivateInsert(value, _) => Self::InsertActivate(value),
            Self::Remove | Self::ActivateRemove | Self::InsertActivateRemove(_) => {
                Self::RemoveActivate
            }
        });
    }
}

impl<'a> DualVersionedTransaction<'a> {
    pub fn new(data: MappedRwLockWriteGuard<'a, DualVersionedData>) -> Self {
        Self {
            data,
            updates: HashMap::new(),
        }
    }

    fn get_current(&self, object_id: &ObjectId) -> Option<&Value> {
        let get_current = || Some(&self.data.get_current(object_id)?.value);
        self.updates
            .get(object_id)
            .and_then(|update| update.get_current(get_current))
            .or_else(get_current)
    }

    fn get_active(&self, object_id: &ObjectId) -> Option<&Value> {
        let get_current = || Some(&self.data.get_current(object_id)?.value);
        let get_active = || Some(&self.data.get_active(object_id)?.value);
        self.updates
            .get(object_id)
            .and_then(|update| update.get_active(get_current, get_active))
            .or_else(get_active)
    }

    pub fn create(&mut self, object_id: ObjectId, value: Value, commit: bool) -> bool {
        self.get_current(&object_id).is_some() || {
            self.insert(object_id, value, commit);
            true
        }
    }

    pub fn update(&mut self, object_id: ObjectId, value: Value, commit: bool) -> bool {
        self.get_current(&object_id).is_some() && {
            self.insert(object_id, value, commit);
            true
        }
    }

    pub fn insert(&mut self, object_id: ObjectId, value: Value, commit: bool) {
        match self.updates.entry(object_id) {
            Entry::Occupied(mut ent) => {
                ent.get_mut().insert(value, commit);
            }
            Entry::Vacant(ent) => {
                ent.insert(DualVersionedUpdate::Insert(value, commit));
            }
        }
    }

    pub fn remove(&mut self, object_id: ObjectId) -> bool {
        self.get_current(&object_id).is_some() && {
            match self.updates.entry(object_id) {
                Entry::Occupied(mut ent) => {
                    ent.get_mut().remove();
                }
                Entry::Vacant(ent) => {
                    ent.insert(DualVersionedUpdate::Remove);
                }
            }
            true
        }
    }

    pub fn activate(&mut self, object_id: ObjectId) -> bool {
        (self.get_current(&object_id).is_some() || self.get_active(&object_id).is_some()) && {
            match self.updates.entry(object_id) {
                Entry::Occupied(mut ent) => {
                    ent.get_mut().activate();
                }
                Entry::Vacant(ent) => {
                    ent.insert(DualVersionedUpdate::Activate);
                }
            }
            true
        }
    }
}

impl<'a> Transaction<'a> for DualVersionedTransaction<'a> {
    type Value = DualVersionedValue;
    fn commit(
        mut self,
        now: chrono::DateTime<chrono::Utc>,
        _force_update: bool,
        _value_schema: &DbSchema,
        updates: &mut super::updates::UpdateGuard<'a, Self::Value>,
    ) {
        for (object_id, update) in self.updates {
            let obj = self.data.0.remove(&object_id);
            let obj = match update {
                DualVersionedUpdate::Insert(value, commit) => match obj {
                    Some(DualVersionedObj::Created { current, committed }) => {
                        let new_current = match committed {
                            true => updates.replace(
                                object_id.clone(),
                                current,
                                |v| v.remove(now),
                                |v| v.update(now, value, commit),
                            ),
                            false => updates.update(object_id.clone(), current, |v| {
                                v.update_uncommitted(now, value, commit)
                            }),
                        };
                        Some(DualVersionedObj::created(new_current, commit))
                    }
                    Some(DualVersionedObj::Removed { active }) => {
                        let current = updates.create(
                            object_id.clone(),
                            DualVersionedValue::new(now, value, commit),
                        );
                        Some(DualVersionedObj::updated(active, current, commit))
                    }
                    Some(DualVersionedObj::Activated { active }) => {
                        let (active, current) = updates.split(
                            object_id.clone(),
                            active,
                            |v| v.remove(now),
                            |v| v.update(now, value, commit),
                        );
                        Some(DualVersionedObj::updated(active, current, commit))
                    }
                    Some(DualVersionedObj::Updated {
                        active,
                        current,
                        committed,
                    }) => {
                        let current = match committed {
                            true => updates.replace(
                                object_id.clone(),
                                current,
                                |v| v.remove(now),
                                |v| v.update(now, value, commit),
                            ),
                            false => updates.update(object_id.clone(), current, |v| {
                                v.update_uncommitted(now, value, commit)
                            }),
                        };
                        Some(DualVersionedObj::updated(active, current, commit))
                    }
                    None => {
                        let current = updates.create(
                            object_id.clone(),
                            DualVersionedValue::new(now, value, commit),
                        );
                        Some(DualVersionedObj::created(current, commit))
                    }
                },
                DualVersionedUpdate::Remove => match obj {
                    Some(DualVersionedObj::Created {
                        current,
                        committed: _,
                    }) => {
                        updates.remove(object_id.clone(), current, |v| v.remove(now));
                        None
                    }
                    Some(DualVersionedObj::Updated {
                        active,
                        current,
                        committed: _,
                    }) => {
                        updates.remove(object_id.clone(), current, |v| v.remove(now));
                        Some(DualVersionedObj::removed(active))
                    }
                    Some(DualVersionedObj::Activated { active }) => {
                        let active = updates.update(object_id.clone(), active, |v| v.remove(now));
                        Some(DualVersionedObj::removed(active))
                    }
                    Some(DualVersionedObj::Removed { active: _ }) | None => obj,
                },
                DualVersionedUpdate::Activate => match obj {
                    Some(DualVersionedObj::Created {
                        current,
                        committed: _,
                    }) => {
                        let active =
                            updates.update(object_id.clone(), current, |v| v.activate(now, None));
                        Some(DualVersionedObj::activated(active))
                    }
                    Some(DualVersionedObj::Updated {
                        active,
                        current,
                        committed: _,
                    }) => {
                        let new_active = updates.update(object_id.clone(), current, |v| {
                            v.activate(now, active.value.version.active.as_ref())
                        });
                        updates.remove(object_id.clone(), active, |v| v.activate_remove(now));
                        Some(DualVersionedObj::activated(new_active))
                    }
                    Some(DualVersionedObj::Removed { active }) => {
                        updates.remove(object_id.clone(), active, |v| v.activate_remove(now));
                        None
                    }
                    Some(DualVersionedObj::Activated { active: _ }) | None => obj,
                },
                DualVersionedUpdate::ActivateInsert(value, commit) => match obj {
                    Some(DualVersionedObj::Created {
                        current,
                        committed: _,
                    }) => {
                        let (active, current) = updates.split(
                            object_id.clone(),
                            current,
                            |v| v.activate(now, None).remove(now),
                            |v| v.update(now, value, commit),
                        );
                        Some(DualVersionedObj::updated(active, current, commit))
                    }
                    Some(DualVersionedObj::Updated {
                        active,
                        current,
                        committed: _,
                    }) => {
                        let (new_active, current) = updates.split(
                            object_id.clone(),
                            current,
                            |v| v.activate(now, active.value.version.active.as_ref()),
                            |v| v.update(now, value, commit),
                        );
                        updates.remove(object_id.clone(), active, |v| v.activate_remove(now));
                        Some(DualVersionedObj::updated(new_active, current, commit))
                    }
                    Some(DualVersionedObj::Removed { active }) => {
                        let current = updates.replace(
                            object_id.clone(),
                            active,
                            |v| v.activate_remove(now),
                            |v| v.update(now, value, commit),
                        );
                        Some(DualVersionedObj::created(current, commit))
                    }
                    Some(DualVersionedObj::Activated { active }) => {
                        let current = updates.update(object_id.clone(), active.clone(), |v| {
                            v.update(now, value, commit)
                        });
                        Some(DualVersionedObj::updated(active, current, commit))
                    }
                    None => {
                        let current = updates.create(
                            object_id.clone(),
                            DualVersionedValue::new(now, value, commit),
                        );
                        Some(DualVersionedObj::created(current, commit))
                    }
                },
                DualVersionedUpdate::ActivateRemove => match obj {
                    Some(DualVersionedObj::Created {
                        current,
                        committed: _,
                    }) => {
                        let active = updates.update(object_id.clone(), current, |v| {
                            v.activate(now, None).remove(now)
                        });
                        Some(DualVersionedObj::removed(active))
                    }
                    Some(DualVersionedObj::Updated {
                        active,
                        current,
                        committed: _,
                    }) => {
                        let new_active = updates.update(object_id.clone(), current, |v| {
                            v.activate(now, active.value.version.active.as_ref())
                                .remove(now)
                        });
                        updates.remove(object_id.clone(), active, |v| v.activate_remove(now));
                        Some(DualVersionedObj::removed(new_active))
                    }
                    Some(DualVersionedObj::Removed { active }) => {
                        updates.remove(object_id.clone(), active, |v| v.activate_remove(now));
                        None
                    }
                    Some(DualVersionedObj::Activated { active: _ }) | None => obj,
                },
                DualVersionedUpdate::InsertActivate(value) => match obj {
                    Some(DualVersionedObj::Created { current, committed }) => {
                        let active = match committed {
                            false => updates.update(object_id.clone(), current, |v| {
                                v.update_uncommitted(now, value, true).activate(now, None)
                            }),
                            true => updates.replace(
                                object_id.clone(),
                                current,
                                |v| v.remove(now),
                                |v| v.update(now, value, true).activate(now, None),
                            ),
                        };
                        Some(DualVersionedObj::activated(active))
                    }
                    Some(DualVersionedObj::Updated {
                        active,
                        current,
                        committed,
                    }) => {
                        let prev_active = active.value.version.active.as_ref();
                        let new_active = match committed {
                            false => updates.update(object_id.clone(), current, |v| {
                                v.update_uncommitted(now, value, true)
                                    .activate(now, prev_active)
                            }),
                            true => updates.replace(
                                object_id.clone(),
                                current,
                                |v| v.remove(now),
                                |v| v.update(now, value, true).activate(now, prev_active),
                            ),
                        };
                        updates.remove(object_id.clone(), active, |v| v.activate_remove(now));
                        Some(DualVersionedObj::activated(new_active))
                    }
                    Some(DualVersionedObj::Removed { active })
                    | Some(DualVersionedObj::Activated { active }) => {
                        let prev_active = active.value.version.active.clone();
                        let active = updates.replace(
                            object_id.clone(),
                            active,
                            |v| v.activate_remove(now),
                            |v| {
                                v.update(now, value, true)
                                    .activate(now, prev_active.as_ref())
                            },
                        );
                        Some(DualVersionedObj::activated(active))
                    }
                    None => {
                        let active = updates.create(
                            object_id.clone(),
                            DualVersionedValue::new(now, value, true).activate(now, None),
                        );
                        Some(DualVersionedObj::activated(active))
                    }
                },
                DualVersionedUpdate::InsertActivateInsert(before, after, commit) => match obj {
                    Some(DualVersionedObj::Created { current, committed }) => {
                        let (active, current) = match committed {
                            false => updates.split(
                                object_id.clone(),
                                current,
                                |v| {
                                    v.update_uncommitted(now, before, true)
                                        .activate(now, None)
                                        .remove(now)
                                },
                                |v| v.update(now, after, commit),
                            ),
                            true => {
                                updates
                                    .remove(object_id.clone(), current.clone(), |v| v.remove(now));
                                let active = updates.create(
                                    object_id.clone(),
                                    current
                                        .value
                                        .clone()
                                        .update(now, before, true)
                                        .activate(now, None)
                                        .remove(now),
                                );
                                let current = updates.create(
                                    object_id.clone(),
                                    current.value.update(now, after, commit),
                                );
                                (active, current)
                            }
                        };
                        Some(DualVersionedObj::updated(active, current, commit))
                    }
                    Some(DualVersionedObj::Updated {
                        active,
                        current,
                        committed,
                    }) => {
                        let prev_active = active.value.version.active.clone();
                        updates.remove(object_id.clone(), active, |v| v.activate_remove(now));
                        let (active, current) = match committed {
                            false => updates.split(
                                object_id.clone(),
                                current,
                                |v| {
                                    v.update_uncommitted(now, before, true)
                                        .activate(now, prev_active.as_ref())
                                        .remove(now)
                                },
                                |v| v.update(now, after, commit),
                            ),
                            true => {
                                updates
                                    .remove(object_id.clone(), current.clone(), |v| v.remove(now));
                                let active = updates.create(
                                    object_id.clone(),
                                    current
                                        .value
                                        .clone()
                                        .update(now, before, true)
                                        .activate(now, None)
                                        .remove(now),
                                );
                                let current = updates.create(
                                    object_id.clone(),
                                    current.value.update(now, after, commit),
                                );
                                (active, current)
                            }
                        };
                        Some(DualVersionedObj::updated(active, current, commit))
                    }
                    Some(DualVersionedObj::Removed { active }) => {
                        updates.remove(object_id.clone(), active, |v| v.activate_remove(now));
                        let active = updates.create(
                            object_id.clone(),
                            DualVersionedValue::new(now, before, true)
                                .activate(now, None)
                                .remove(now),
                        );
                        let current = updates.create(
                            object_id.clone(),
                            DualVersionedValue::new(now, after, commit),
                        );
                        Some(DualVersionedObj::updated(active, current, commit))
                    }
                    Some(DualVersionedObj::Activated { active }) => {
                        let prev_active = active.value.version.active.as_ref();
                        updates.remove(object_id.clone(), active.clone(), |v| {
                            v.activate_remove(now)
                        });
                        let active = updates.create(
                            object_id.clone(),
                            active
                                .value
                                .clone()
                                .update(now, before, true)
                                .activate(now, prev_active)
                                .remove(now),
                        );
                        let current = updates.create(
                            object_id.clone(),
                            active.value.clone().update(now, after, commit),
                        );
                        Some(DualVersionedObj::updated(active, current, commit))
                    }
                    None => {
                        let active = updates.create(
                            object_id.clone(),
                            DualVersionedValue::new(now, before, true)
                                .activate(now, None)
                                .remove(now),
                        );
                        let current = updates.create(
                            object_id.clone(),
                            DualVersionedValue::new(now, after, commit),
                        );
                        Some(DualVersionedObj::updated(active, current, commit))
                    }
                },
                DualVersionedUpdate::InsertActivateRemove(value) => match obj {
                    Some(DualVersionedObj::Created { current, committed }) => {
                        let active = match committed {
                            false => updates.update(object_id.clone(), current, |v| {
                                v.update_uncommitted(now, value, true)
                                    .activate(now, None)
                                    .remove(now)
                            }),
                            true => {
                                updates
                                    .remove(object_id.clone(), current.clone(), |v| v.remove(now));
                                updates.create(
                                    object_id.clone(),
                                    current
                                        .value
                                        .update(now, value, true)
                                        .activate(now, None)
                                        .remove(now),
                                )
                            }
                        };
                        Some(DualVersionedObj::removed(active))
                    }
                    Some(DualVersionedObj::Updated {
                        active,
                        current,
                        committed,
                    }) => {
                        let prev_active = active.value.version.active.clone();
                        updates.remove(object_id.clone(), active, |v| v.activate_remove(now));
                        let active = match committed {
                            false => updates.update(object_id.clone(), current, |v| {
                                v.update_uncommitted(now, value, true)
                                    .activate(now, prev_active.as_ref())
                                    .remove(now)
                            }),
                            true => {
                                updates
                                    .remove(object_id.clone(), current.clone(), |v| v.remove(now));
                                updates.create(
                                    object_id.clone(),
                                    current
                                        .value
                                        .update(now, value, true)
                                        .activate(now, prev_active.as_ref())
                                        .remove(now),
                                )
                            }
                        };
                        Some(DualVersionedObj::removed(active))
                    }
                    Some(DualVersionedObj::Removed { active }) => {
                        let prev_active = active.value.version.active.clone();
                        updates.remove(object_id.clone(), active, |v| v.activate_remove(now));
                        let active = updates.create(
                            object_id.clone(),
                            DualVersionedValue::new(now, value, true)
                                .activate(now, prev_active.as_ref())
                                .remove(now),
                        );
                        Some(DualVersionedObj::removed(active))
                    }
                    Some(DualVersionedObj::Activated { active }) => {
                        let prev_active = active.value.version.active.clone();
                        updates.remove(object_id.clone(), active.clone(), |v| {
                            v.remove(now).activate_remove(now)
                        });
                        let active = updates.create(
                            object_id.clone(),
                            active
                                .value
                                .update(now, value, true)
                                .activate(now, prev_active.as_ref())
                                .remove(now),
                        );
                        Some(DualVersionedObj::removed(active))
                    }
                    None => {
                        let active = updates.create(
                            object_id.clone(),
                            DualVersionedValue::new(now, value, true)
                                .activate(now, None)
                                .remove(now),
                        );
                        Some(DualVersionedObj::removed(active))
                    }
                },
                DualVersionedUpdate::RemoveActivate => match obj {
                    Some(DualVersionedObj::Created {
                        current,
                        committed: _,
                    }) => {
                        updates.remove(object_id.clone(), current, |v| v.remove(now));
                        None
                    }
                    Some(DualVersionedObj::Updated {
                        active,
                        current,
                        committed: _,
                    }) => {
                        updates.remove(object_id.clone(), active, |v| v.activate_remove(now));
                        updates.remove(object_id.clone(), current, |v| v.remove(now));
                        None
                    }
                    Some(DualVersionedObj::Removed { active }) => {
                        updates.remove(object_id.clone(), active, |v| v.activate_remove(now));
                        None
                    }
                    Some(DualVersionedObj::Activated { active }) => {
                        updates.remove(object_id.clone(), active, |v| {
                            v.remove(now).activate_remove(now)
                        });
                        None
                    }
                    None => None,
                },
                DualVersionedUpdate::RemoveActivateInsert(value, commit) => match obj {
                    Some(DualVersionedObj::Created {
                        current,
                        committed: _,
                    }) => {
                        updates.remove(object_id.clone(), current, |v| v.remove(now));
                        let current = updates.create(
                            object_id.clone(),
                            DualVersionedValue::new(now, value, commit),
                        );
                        Some(DualVersionedObj::created(current, commit))
                    }
                    Some(DualVersionedObj::Updated {
                        active,
                        current,
                        committed: _,
                    }) => {
                        let prev_active = active.value.version.active.as_ref();
                        updates.remove(object_id.clone(), current, |v| {
                            v.activate(now, prev_active)
                                .remove(now)
                                .activate_remove(now)
                        });
                        updates.remove(object_id.clone(), active, |v| v.activate_remove(now));
                        let current = updates.create(
                            object_id.clone(),
                            DualVersionedValue::new(now, value, commit),
                        );
                        Some(DualVersionedObj::created(current, commit))
                    }
                    Some(DualVersionedObj::Removed { active }) => {
                        updates.remove(object_id.clone(), active, |v| v.activate_remove(now));
                        let current = updates.create(
                            object_id.clone(),
                            DualVersionedValue::new(now, value, commit),
                        );
                        Some(DualVersionedObj::created(current, commit))
                    }
                    Some(DualVersionedObj::Activated { active }) => {
                        updates.remove(object_id.clone(), active, |v| {
                            v.remove(now).activate_remove(now).remove(now)
                        });
                        let current = updates.create(
                            object_id.clone(),
                            DualVersionedValue::new(now, value, commit),
                        );
                        Some(DualVersionedObj::created(current, commit))
                    }
                    None => {
                        let current = updates.create(
                            object_id.clone(),
                            DualVersionedValue::new(now, value, commit),
                        );
                        Some(DualVersionedObj::created(current, commit))
                    }
                },
            };
            if let Some(obj) = obj {
                self.data.0.insert(object_id, obj);
            }
        }
    }
}

#[cfg(test)]
mod test {

    use chrono::Utc;
    use dbschema::{DbTableId, DualVersioned, HasSchema, HasTableDef, Identified, ObjectId};
    use serde_json::json;

    use crate::daemon::{state::State, table_state::TableOperationalState};

    #[tokio::test]
    async fn transaction_insert() {
        type Document = Identified<DualVersioned<Object>>;
        #[derive(HasSchema, Debug)]
        #[allow(unused)]
        struct Object {
            field: String,
        }

        let state = State::new();
        let table_id = DbTableId::new("test-table");

        {
            let (_schemas, mut table) = state
                .write_table(
                    &table_id,
                    "test",
                    crate::daemon::table_state::TableNonOperationalState::Registering,
                    true,
                )
                .await
                .unwrap();
            table.or_insert_with(|| TableOperationalState::new(Document::table_def()));
        }

        let now = Utc::now();
        let table = state.read_table(&table_id, "test").await.unwrap();
        let object_id = ObjectId::new();
        let updates = {
            let mut data = table.write_data_dual_versioned(now).unwrap();
            data.insert(object_id.clone(), json!({"field": "test"}), true);
            data.commit()
        };

        let _updates = updates.extract().into_values().collect::<Vec<_>>();
        //updates.sort();
        // let _expected = vec![(
        //     0,
        //     Identified {
        //         object_id,
        //         value: DualVersionedValue {
        //             version: DualVersionInfo {
        //                 current: Anchor {
        //                     created: now,
        //                     from: now,
        //                     to: None,
        //                 },
        //                 committed: Some(now),
        //                 active: None,
        //             },
        //             value: json!({"field": "test"}),
        //         },
        //     },
        // )];

        //assert_eq!(updates, expected);
    }
}
