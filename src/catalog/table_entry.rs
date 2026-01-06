use crate::catalog::table::Table;
use std::sync::Arc;

pub(crate) struct TableEntry {
    table: Arc<Table>,
}

impl TableEntry {
    pub(crate) fn new(table: Table) -> TableEntry {
        Self {
            table: Arc::new(table),
        }
    }

    pub fn table(&self) -> Arc<Table> {
        self.table.clone()
    }
}
