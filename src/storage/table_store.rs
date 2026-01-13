use crate::storage::row::Row;
use crossbeam_skiplist::map::Iter;
use crossbeam_skiplist::SkipMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Unique identifier for a row in a table.
pub type RowId = u64;

/// Manages the in-memory storage of rows for a table.
///
/// `TableStore` implementation is based on `SkipMap` for concurrent access and uses
/// `AtomicU64` for generating unique row IDs.
pub(crate) struct TableStore {
    entries: SkipMap<RowId, Row>,
    current_row_id: AtomicU64,
}

/// Iterator over the rows in a `TableStore`.
pub(crate) struct TableStoreIterator<'a> {
    inner: Iter<'a, RowId, Row>,
}

impl Iterator for TableStoreIterator<'_> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| entry.value().clone())
    }
}

impl TableStore {
    /// Creates a new, empty `TableStore`.
    ///
    /// The row IDs start at 1.
    pub(crate) fn new() -> TableStore {
        Self {
            entries: SkipMap::new(),
            current_row_id: AtomicU64::new(1),
        }
    }

    /// Inserts multiple rows into the store.
    ///
    /// Returns a vector of `RowId`s corresponding to the inserted rows.
    pub(crate) fn insert_all(&self, rows: Vec<Row>) -> Vec<RowId> {
        let mut row_ids = Vec::with_capacity(rows.len());
        for row in rows {
            row_ids.push(self.insert(row));
        }
        row_ids
    }

    /// Inserts a single row into the store.
    ///
    /// Returns the assigned `RowId`.
    pub(crate) fn insert(&self, row: Row) -> RowId {
        let row_id = self.current_row_id.fetch_add(1, Ordering::AcqRel);
        self.entries.insert(row_id, row);
        row_id
    }

    /// Retrieves a row by its `RowId`.
    ///
    /// Returns `Some(Row)` if the row exists, `None` otherwise.
    pub(crate) fn get(&self, row_id: RowId) -> Option<Row> {
        self.entries.get(&row_id).map(|entry| entry.value().clone())
    }

    /// Returns an iterator over all rows in the table.
    pub(crate) fn iter(&self) -> TableStoreIterator<'_> {
        TableStoreIterator {
            inner: self.entries.iter(),
        }
    }
}

#[cfg(test)]
impl TableStore {
    fn scan(&self) -> Vec<Row> {
        self.entries
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::column_value::ColumnValue;

    #[test]
    fn insert_row_and_get_row_id() {
        let store = TableStore::new();
        let row_id = store.insert(Row::filled(vec![
            ColumnValue::Int(10),
            ColumnValue::Text("relop".to_string()),
        ]));

        assert_eq!(1, row_id);
    }

    #[test]
    fn insert_row_and_scan() {
        let store = TableStore::new();
        store.insert(Row::filled(vec![
            ColumnValue::Int(10),
            ColumnValue::Text("relop".to_string()),
        ]));

        let rows: Vec<Row> = store.scan();
        assert_eq!(1, rows.len());

        let inserted_row = rows.first().unwrap();
        let expected_row = Row::filled(vec![
            ColumnValue::Int(10),
            ColumnValue::Text("relop".to_string()),
        ]);

        assert_eq!(&expected_row, inserted_row);
    }

    #[test]
    fn insert_rows() {
        let store = TableStore::new();
        let row_ids = store.insert_all(vec![
            Row::filled(vec![
                ColumnValue::Int(10),
                ColumnValue::Text("relop".to_string()),
            ]),
            Row::filled(vec![
                ColumnValue::Int(20),
                ColumnValue::Text("query".to_string()),
            ]),
        ]);

        assert_eq!(2, row_ids.len());
        assert_eq!(&1, row_ids.first().unwrap());
        assert_eq!(&2, row_ids.last().unwrap());
    }

    #[test]
    fn insert_rows_and_scan() {
        let store = TableStore::new();
        store.insert_all(vec![
            Row::filled(vec![
                ColumnValue::Int(10),
                ColumnValue::Text("relop".to_string()),
            ]),
            Row::filled(vec![
                ColumnValue::Int(20),
                ColumnValue::Text("query".to_string()),
            ]),
        ]);

        let rows = store.scan();
        assert_eq!(2, rows.len());

        assert!(rows.contains(&Row::filled(vec![
            ColumnValue::Int(10),
            ColumnValue::Text("relop".to_string())
        ])));
        assert!(rows.contains(&Row::filled(vec![
            ColumnValue::Int(20),
            ColumnValue::Text("query".to_string())
        ])));
    }

    #[test]
    fn insert_row_and_get_by_row_id() {
        let store = TableStore::new();
        let row_id = store.insert(Row::filled(vec![
            ColumnValue::Int(10),
            ColumnValue::Text("relop".to_string()),
        ]));

        let row = store.get(row_id).unwrap();
        let expected_row = Row::filled(vec![
            ColumnValue::Int(10),
            ColumnValue::Text("relop".to_string()),
        ]);

        assert_eq!(expected_row, row);
    }

    #[test]
    fn insert_row_and_attempt_to_get_by_non_existent_row_id() {
        let store = TableStore::new();
        store.insert(Row::filled(vec![
            ColumnValue::Int(10),
            ColumnValue::Text("relop".to_string()),
        ]));

        let entry = store.get(1000);
        assert!(entry.is_none());
    }

    #[test]
    fn iterate_over_all_rows() {
        let store = TableStore::new();
        store.insert(Row::filled(vec![ColumnValue::Int(10)]));
        store.insert(Row::filled(vec![ColumnValue::Int(20)]));

        let mut iterator = store.iter();

        assert_eq!(
            Row::filled(vec![ColumnValue::Int(10)]),
            iterator.next().unwrap()
        );
        assert_eq!(
            Row::filled(vec![ColumnValue::Int(20)]),
            iterator.next().unwrap()
        );
        assert!(iterator.next().is_none());
    }

    #[test]
    fn attempt_to_iterate_over_all_rows_with_empty_table_store() {
        let store = TableStore::new();
        let mut iterator = store.iter();

        assert!(iterator.next().is_none());
    }
}
