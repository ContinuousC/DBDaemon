use dbschema::DbTableId;

use super::{state::State, table_state::TableOperationalState};

pub struct TableWriteGuard<'a> {
    tables: &'a State,
    table_id: &'a DbTableId,
    _method: &'static str,
    state: Option<TableOperationalState>,
}

impl<'a> TableWriteGuard<'a> {
    pub fn new(
        tables: &'a State,
        table_id: &'a DbTableId,
        method: &'static str,
        state: Option<TableOperationalState>,
    ) -> Self {
        Self {
            tables,
            table_id,
            _method: method,
            state,
        }
    }

    pub fn as_mut(&mut self) -> Option<&mut TableOperationalState> {
        self.state.as_mut()
    }

    pub fn or_insert_with<F>(&mut self, f: F) -> &mut TableOperationalState
    where
        F: FnOnce() -> TableOperationalState,
    {
        match &mut self.state {
            Some(state) => state,
            state @ None => {
                state.replace(f());
                state.as_mut().unwrap()
            }
        }
    }

    pub fn remove(&mut self) -> Option<TableOperationalState> {
        self.state.take()
    }
}

impl Drop for TableWriteGuard<'_> {
    fn drop(&mut self) {
        match self.state.take() {
            Some(state) => self.tables.insert(self.table_id.clone(), state),
            None => self.tables.remove(self.table_id),
        }
    }
}
