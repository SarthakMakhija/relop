use crate::catalog::table::Table;
use crate::catalog::table_scan::TableScan;
use crate::storage::primary_key_index::PrimaryKeyIndex;
use crate::storage::row::Row;
use crate::storage::table_store::{RowId, TableStore};
use std::sync::Arc;

pub(crate) struct TableEntry {
    table: Table,
    store: Arc<TableStore>,
    primary_key_index: Option<PrimaryKeyIndex>,
}

impl TableEntry {
    pub(crate) fn new(table: Table) -> Arc<TableEntry> {
        let primary_key_index = Self::maybe_primary_key_index(&table);
        Arc::new(Self {
            table,
            store: Arc::new(TableStore::new()),
            primary_key_index,
        })
    }

    pub(crate) fn insert(&self, row: Row) -> RowId {
        self.store.insert(row)
    }

    pub(crate) fn insert_all(&self, rows: Vec<Row>) -> Vec<RowId> {
        self.store.insert_all(rows)
    }

    pub(crate) fn get(&self, row_id: RowId) -> Option<Row> {
        self.store.get(row_id)
    }

    pub(crate) fn scan(&self) -> TableScan {
        TableScan::new(self.store.clone())
    }

    pub(crate) fn table_name(&self) -> &str {
        self.table.name()
    }
    
    pub(crate) fn has_primary_key_index(&self) -> bool {
        self.primary_key_index.is_some()
    }

    fn maybe_primary_key_index(table: &Table) -> Option<PrimaryKeyIndex> {
        if table.has_primary_key() {
            return Some(PrimaryKeyIndex::new());
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::column::ColumnType;
    use crate::schema::primary_key::PrimaryKey;
    use crate::schema::Schema;
    use crate::storage::row::ColumnValue;

    #[test]
    fn insert_row() {
        let table_entry = TableEntry::new(Table::new(
            "employees".to_string(),
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        ));
        table_entry.insert(Row::filled(vec![ColumnValue::Int(100)]));

        let rows = table_entry.scan().iter().collect::<Vec<_>>();

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

        let rows = table_entry.scan().iter().collect::<Vec<_>>();
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
        let table_entry = TableEntry::new(Table::new(
            "employees".to_string(),
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        ));
        let row_id = table_entry.insert(Row::filled(vec![ColumnValue::Int(100)]));

        let row = table_entry.get(row_id).unwrap();
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

    #[test]
    fn should_create_primary_key_index() {
        let table_entry = TableEntry::new(Table::new(
            "employees".to_string(),
            Schema::new()
                .add_column("id", ColumnType::Int)
                .unwrap()
                .add_primary_key(PrimaryKey::single("id"))
                .unwrap(),
        ));
        assert!(table_entry.has_primary_key_index());
    }

    #[test]
    fn should_not_create_primary_key_index() {
        let table_entry = TableEntry::new(Table::new(
            "employees".to_string(),
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        ));
        assert!(!table_entry.has_primary_key_index());
    }
}
