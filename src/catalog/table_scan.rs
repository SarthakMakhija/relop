use crate::storage::row::Row;
use crate::storage::row_filter::{NoFilter, RowFilter};
use crate::storage::table_store::{TableStore, TableStoreIterator};
use std::sync::Arc;

/// A handle to a table scan operation that owns the `TableStore`.
///
/// This struct holds the `Arc<TableStore>` to ensure the data is kept alive
/// during the scan, but it does not eagerly collect rows or hold an iterator itself.
/// The iterator is created on demand via the `.iter()` method, which yields a
/// `TableIterator` bound to the lifetime of `TableScan` (and thus the `Arc`).
pub(crate) struct TableScan<F: RowFilter = NoFilter> {
    store: Arc<TableStore>,
    filter: Arc<F>,
}

impl TableScan<NoFilter> {
    /// Creates a new instance of TableScan with no filter.
    pub(crate) fn new(store: Arc<TableStore>) -> Self {
        Self {
            store,
            filter: Arc::new(NoFilter),
        }
    }
}

impl<F: RowFilter> TableScan<F> {
    /// Creates a new instance of TableScan with a specific filter.
    pub(crate) fn with_filter(store: Arc<TableStore>, filter: F) -> Self {
        Self {
            store,
            filter: Arc::new(filter),
        }
    }

    /// Returns an iterator over the rows in the table.
    ///
    /// The returned `TableIterator` borrows from this `TableScan` to ensure validity.
    pub(crate) fn iter(&self) -> TableIterator<'_, F> {
        TableIterator {
            iter: self.store.iter(),
            filter: self.filter.clone(),
        }
    }
}

/// Iterator that scans rows in a table.
///
/// This iterator borrows from `TableScan` (via the `TableStore` reference)
/// and thus cannot outlive the `TableScan`.
pub(crate) struct TableIterator<'a, F: RowFilter = NoFilter> {
    iter: TableStoreIterator<'a>,
    filter: Arc<F>,
}

impl<F: RowFilter> Iterator for TableIterator<'_, F> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.by_ref().find(|row| self.filter.matches(row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row;

    #[test]
    fn scan_table() {
        let store = Arc::new(TableStore::new());
        store.insert(row![1]);
        store.insert(row![2]);

        let table_scan = TableScan::new(store);
        let mut iterator = table_scan.iter();

        let row1 = iterator.next().unwrap();
        assert_eq!(row![1], row1);

        let row2 = iterator.next().unwrap();
        assert_eq!(row![2], row2);

        assert!(iterator.next().is_none());
    }

    #[test]
    fn scan_empty_table() {
        let store = Arc::new(TableStore::new());
        let table_scan = TableScan::new(store);

        let mut iterator = table_scan.iter();
        assert!(iterator.next().is_none());
    }

    #[test]
    fn scan_table_with_filter() {
        let store = Arc::new(TableStore::new());
        store.insert(row![10]);
        store.insert(row![20]);
        store.insert(row![30]);

        struct Over25Filter;
        impl RowFilter for Over25Filter {
            fn matches(&self, row: &Row) -> bool {
                row.column_value_at(0).unwrap().int_value().unwrap() > 25
            }
        }

        let table_scan = TableScan::with_filter(store, Over25Filter);
        let mut iterator = table_scan.iter();

        let row = iterator.next().unwrap();
        assert_eq!(row![30], row);

        assert!(iterator.next().is_none());
    }
}
