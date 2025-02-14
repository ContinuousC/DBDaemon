/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::ops::Deref;

use parking_lot::MappedRwLockReadGuard;

use super::table_read::TableReadGuard;

pub struct DataReadGuard<'a, T> {
    _state: &'a TableReadGuard<'a>,
    data: MappedRwLockReadGuard<'a, T>,
}

impl<'a, T> DataReadGuard<'a, T> {
    pub fn new(state: &'a TableReadGuard<'a>, data: MappedRwLockReadGuard<'a, T>) -> Self {
        Self {
            _state: state,
            data,
        }
    }
}

impl<T> Deref for DataReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
