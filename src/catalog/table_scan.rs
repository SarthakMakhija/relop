use crate::storage::row::Row;
use crate::storage::table_store::{TableStore, TableStoreIterator};
use std::sync::Arc;

/// A handle to a table scan operation that owns the `TableStore`.
///
/// This struct holds the `Arc<TableStore>` to ensure the data is kept alive
/// during the scan, but it does not eagerly collect rows or hold an iterator itself.
/// The iterator is created on demand via the `.iter()` method, which yields a
/// `TableIterator` bound to the lifetime of `TableScan` (and thus the `Arc`).
pub struct TableScan {
    store: Arc<TableStore>,
}

impl TableScan {
    pub(crate) fn new(store: Arc<TableStore>) -> Self {
        Self { store }
    }

    /// Returns an iterator over the rows in the table.
    ///
    /// The returned `TableIterator` borrows from this `TableScan` to ensure validity.
    pub fn iter(&self) -> TableIterator<'_> {
        TableIterator {
            iter: self.store.iter(),
        }
    }
}

/// Iterator that scans all rows in a table.
///
/// This iterator borrows from `TableScan` (via the `TableStore` reference)
/// and thus cannot outlive the `TableScan`.
pub struct TableIterator<'a> {
    iter: TableStoreIterator<'a>,
}

impl Iterator for TableIterator<'_> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}
