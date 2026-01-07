use crate::catalog::table::Table;
use crate::storage::row::Row;
use crate::storage::table_store::{RowId, TableStore};
use crossbeam_skiplist::map::Entry;
use std::sync::Arc;

pub(crate) struct TableEntry {
    table: Table,
    store: TableStore,
}

impl TableEntry {
    pub(crate) fn new(table: Table) -> Arc<TableEntry> {
        Arc::new(Self {
            table,
            store: TableStore::new(),
        })
    }

    pub(crate) fn insert(&self, row: Row) -> RowId {
        self.store.insert(row)
    }

    pub(crate) fn insert_all(&self, rows: Vec<Row>) {
        self.store.insert_all(rows)
    }

    pub(crate) fn get(&self, row_id: RowId) -> Option<Entry<'_, RowId, Row>> {
        self.store.get(row_id)
    }

    pub(crate) fn table_name(&self) -> &str {
        self.table.name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::column::ColumnType;
    use crate::schema::Schema;
    use crate::storage::row::ColumnValue;

    #[test]
    fn insert_row() {
        let table_entry = TableEntry::new(Table::new(
            "employees".to_string(),
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        ));
        table_entry.insert(Row::filled(vec![ColumnValue::Int(100)]));

        let entries = table_entry.store.scan().collect::<Vec<_>>();
        let rows = entries
            .iter()
            .map(|entry| entry.value())
            .collect::<Vec<_>>();

        assert_eq!(1, rows.len());
        assert_eq!(100, rows[0].column_values()[0].int_value().unwrap());
    }

    #[test]
    fn insert_rows() {
        let table_entry = TableEntry::new(Table::new(
            "employees".to_string(),
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        ));
        table_entry.insert_all(vec![
            Row::filled(vec![
                ColumnValue::Int(10),
                ColumnValue::Text("relop".to_string()),
            ]),
            Row::filled(vec![
                ColumnValue::Int(20),
                ColumnValue::Text("query".to_string()),
            ]),
        ]);

        let entries = table_entry.store.scan().collect::<Vec<_>>();
        let rows = entries
            .iter()
            .map(|entry| entry.value())
            .collect::<Vec<_>>();

        assert_eq!(2, rows.len());

        assert!(rows.contains(&&Row::filled(vec![
            ColumnValue::Int(10),
            ColumnValue::Text("relop".to_string())
        ])));
        assert!(rows.contains(&&Row::filled(vec![
            ColumnValue::Int(20),
            ColumnValue::Text("query".to_string())
        ])));
    }

    #[test]
    fn insert_row_and_get_by_row_id() {
        let table_entry = TableEntry::new(Table::new(
            "employees".to_string(),
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        ));
        let row_id = table_entry.insert(Row::filled(vec![ColumnValue::Int(100)]));

        let entry = table_entry.get(row_id).unwrap();
        let row = entry.value();
        assert_eq!(100, row.column_values()[0].int_value().unwrap());
    }

    #[test]
    fn insert_row_and_attempt_to_get_by_non_existent_row_id() {
        let table_entry = TableEntry::new(Table::new(
            "employees".to_string(),
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        ));
        table_entry.insert(Row::filled(vec![ColumnValue::Int(100)]));

        let entry = table_entry.get(1000);
        assert!(entry.is_none());
    }
}
