use crate::storage::row::Row;
use crate::storage::table_store::{TableStore, TableStoreIterator};
use std::sync::Arc;

/// A handle to a table scan operation that owns the `TableStore`.
///
/// This struct holds the `Arc<TableStore>` to ensure the data is kept alive
/// during the scan, but it does not eagerly collect rows or hold an iterator itself.
/// The iterator is created on demand via the `.iter()` method, which yields a
/// `TableIterator` bound to the lifetime of `TableScan` (and thus the `Arc`).
pub(crate) struct TableScan {
    store: Arc<TableStore>,
}

impl TableScan {
    /// Creates a new instance of TableScan.
    pub(crate) fn new(store: Arc<TableStore>) -> Self {
        Self { store }
    }

    /// Returns an iterator over the rows in the table.
    ///
    /// The returned `TableIterator` borrows from this `TableScan` to ensure validity.
    pub(crate) fn iter(&self) -> TableIterator<'_> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::row::Row;
    use crate::types::column_value::ColumnValue;

    #[test]
    fn scan_table() {
        let store = Arc::new(TableStore::new());
        store.insert(Row::filled(vec![ColumnValue::Int(1)]));
        store.insert(Row::filled(vec![ColumnValue::Int(2)]));

        let table_scan = TableScan::new(store);
        let mut iterator = table_scan.iter();

        let row1 = iterator.next().unwrap();
        assert_eq!(ColumnValue::Int(1), row1.column_values()[0]);

        let row2 = iterator.next().unwrap();
        assert_eq!(ColumnValue::Int(2), row2.column_values()[0]);

        assert!(iterator.next().is_none());
    }

    #[test]
    fn scan_empty_table() {
        let store = Arc::new(TableStore::new());
        let table_scan = TableScan::new(store);

        let mut iterator = table_scan.iter();
        assert!(iterator.next().is_none());
    }
}
