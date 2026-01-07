use crate::storage::row::Row;
use crossbeam_skiplist::SkipMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::AcqRel;

pub type RowId = u64;
pub(crate) struct TableStore {
    entries: SkipMap<RowId, Row>,
    current_row_id: AtomicU64,
}

impl TableStore {
    pub(crate) fn new() -> TableStore {
        Self {
            entries: SkipMap::new(),
            current_row_id: AtomicU64::new(1),
        }
    }

    pub(crate) fn insert_all(&self, rows: Vec<Row>) {
        for row in rows {
            self.insert(row);
        }
    }

    pub(crate) fn insert(&self, row: Row) -> RowId {
        let row_id = self.current_row_id.fetch_add(1, AcqRel);
        self.entries.insert(row_id, row);
        row_id
    }

    pub(crate) fn get(&self, row_id: RowId) -> Option<Row> {
        self.entries.get(&row_id).map(|entry| entry.value().clone())
    }

    pub(crate) fn entries(&self) -> &SkipMap<RowId, Row> {
        &self.entries
    }
}

#[cfg(test)]
impl TableStore {
    fn scan(&self) -> Vec<Row> {
        self.entries.iter().map(|entry| entry.value().clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::row::ColumnValue;

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
}
