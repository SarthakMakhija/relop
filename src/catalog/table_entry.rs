use crate::catalog::error::CatalogError;
use crate::catalog::table::Table;
use crate::catalog::table_scan::TableScan;
use crate::storage::primary_key_column_values::PrimaryKeyColumnValues;
use crate::storage::primary_key_index::PrimaryKeyIndex;
use crate::storage::row::Row;
use crate::storage::table_store::{RowId, TableStore};
use std::sync::{Arc, Mutex};

pub(crate) struct TableEntry {
    table: Table,
    store: Arc<TableStore>,
    primary_key_index: Option<PrimaryKeyIndex>,
    insert_lock: Mutex<()>,
}

impl TableEntry {
    pub(crate) fn new(table: Table) -> Arc<TableEntry> {
        let primary_key_index = Self::maybe_primary_key_index(&table);
        Arc::new(Self {
            table,
            store: Arc::new(TableStore::new()),
            primary_key_index,
            insert_lock: Mutex::new(()),
        })
    }

    pub(crate) fn insert(&self, row: Row) -> Result<RowId, CatalogError> {
        let _guard = self.insert_lock.lock().unwrap();

        if let Some(primary_key_index) = &self.primary_key_index {
            let schema = self.table.schema();
            //SAFETY: primary_key_index can only be created if the Table has a primary key.
            //If table has a primary key, we can safely unwrap() primary_key() from schema.
            let primary_key = schema.primary_key().as_ref().unwrap();
            let primary_key_column_values = PrimaryKeyColumnValues::new(&row, primary_key, schema);

            if primary_key_index.contains(&primary_key_column_values) {
                return Err(CatalogError::DuplicatePrimaryKey);
            }
            let row_id = self.store.insert(row);
            primary_key_index.insert(primary_key_column_values, row_id);

            Ok(row_id)
        } else {
            Ok(self.store.insert(row))
        }
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
    
    pub(crate) fn primary_key_index(&self) -> Option<&PrimaryKeyIndex> {
        self.primary_key_index.as_ref()
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
        table_entry
            .insert(Row::filled(vec![ColumnValue::Int(100)]))
            .unwrap();

        let rows = table_entry.scan().iter().collect::<Vec<_>>();

        assert_eq!(1, rows.len());
        assert_eq!(100, rows[0].column_values()[0].int_value().unwrap());
    }

    #[test]
    fn insert_row_with_primary_key() {
        let table_entry = TableEntry::new(Table::new(
            "employees".to_string(),
            Schema::new()
                .add_column("id", ColumnType::Int)
                .unwrap()
                .add_primary_key(PrimaryKey::single("id"))
                .unwrap(),
        ));
        table_entry
            .insert(Row::filled(vec![ColumnValue::Int(100)]))
            .unwrap();

        let rows = table_entry.scan().iter().collect::<Vec<_>>();
        assert_eq!(1, rows.len());
        assert_eq!(100, rows[0].column_values()[0].int_value().unwrap());

        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_primary_key(PrimaryKey::single("id"))
            .unwrap();

        let row = Row::filled(vec![ColumnValue::Int(100)]);
        let primary_key = PrimaryKey::single("id");
        let primary_key_column_values = PrimaryKeyColumnValues::new(&row, &primary_key, &schema);

        let primary_key_index = table_entry.primary_key_index.as_ref().unwrap();
        let row_id = primary_key_index.get(&primary_key_column_values);

        assert!(row_id.is_some());
    }

    #[test]
    fn attempt_to_insert_row_with_duplicate_primary_key() {
        let table_entry = TableEntry::new(Table::new(
            "employees".to_string(),
            Schema::new()
                .add_column("id", ColumnType::Int)
                .unwrap()
                .add_primary_key(PrimaryKey::single("id"))
                .unwrap(),
        ));
        table_entry
            .insert(Row::filled(vec![ColumnValue::Int(100)]))
            .unwrap();

        let result = table_entry
            .insert(Row::filled(vec![ColumnValue::Int(100)]));

        assert!(matches!(result, Err(CatalogError::DuplicatePrimaryKey)));
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
        let row_id = table_entry
            .insert(Row::filled(vec![ColumnValue::Int(100)]))
            .unwrap();

        let row = table_entry.get(row_id).unwrap();
        assert_eq!(100, row.column_values()[0].int_value().unwrap());
    }

    #[test]
    fn insert_row_and_attempt_to_get_by_non_existent_row_id() {
        let table_entry = TableEntry::new(Table::new(
            "employees".to_string(),
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        ));
        table_entry
            .insert(Row::filled(vec![ColumnValue::Int(100)]))
            .unwrap();

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
