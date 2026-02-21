use crate::catalog::error::InsertError;
use crate::catalog::table::Table;
use crate::catalog::table_scan::TableScan;
use crate::storage::batch::Batch;
use crate::storage::row::Row;
use crate::storage::table_store::{RowId, TableStore};
use std::sync::{Arc, Mutex};

/// It holds a reference to the `Table` definition and the underlying `TableStore` for data storage.
///
/// `TableEntry` is responsible for managing concurrent access to the table data, ensuring
/// thread safety during insertions.
pub(crate) struct TableEntry {
    table: Arc<Table>,
    store: Arc<TableStore>,
    insert_lock: Mutex<()>,
}

impl TableEntry {
    /// Creates a new `TableEntry` for the given `Table`.
    ///
    /// This initializes the `TableStore` and, if the table has a primary key,
    /// the `PrimaryKeyIndex`.
    pub(crate) fn new(table: Table) -> Arc<TableEntry> {
        Arc::new(Self {
            table: Arc::new(table),
            store: Arc::new(TableStore::new()),
            insert_lock: Mutex::new(()),
        })
    }

    /// Inserts a single row into the table.
    pub(crate) fn insert(&self, row: Row) -> Result<RowId, InsertError> {
        let _guard = self.insert_lock.lock().unwrap();
        Ok(self.store.insert(row))
    }

    /// Inserts a batch of rows into the table.
    pub(crate) fn insert_all(&self, batch: Batch) -> Result<Vec<RowId>, InsertError> {
        let _guard = self.insert_lock.lock().unwrap();
        Ok(self.store.insert_all(batch.into_rows()))
    }

    /// Retrieves a row by its `RowId`.
    ///
    /// Returns `Some(Row)` if the row exists, or `None` otherwise.
    pub(crate) fn get(&self, row_id: RowId) -> Option<Row> {
        self.store.get(row_id)
    }

    /// Creates a `TableScan` which can be used to iterate over the rows in the table.
    pub(crate) fn scan(&self) -> TableScan {
        TableScan::new(self.store.clone())
    }

    /// Returns a reference to the `Table` definition.
    pub(crate) fn table_ref(&self) -> &Table {
        &self.table
    }

    /// Returns a specific `Arc` reference to the `Table` definition.
    pub(crate) fn table(&self) -> Arc<Table> {
        self.table.clone()
    }
}

#[cfg(test)]
impl TableEntry {
    /// Returns the name of the table.
    pub(crate) fn table_name(&self) -> &str {
        self.table.name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row;
    use crate::rows;
    use crate::schema;
    use crate::types::column_type::ColumnType;

    #[test]
    fn insert_row() {
        let table_entry = TableEntry::new(Table::new(
            "employees",
            schema!["id" => ColumnType::Int].unwrap(),
        ));
        table_entry.insert(row![100]).unwrap();

        let rows = table_entry.scan().iter().collect::<Vec<_>>();

        assert_eq!(1, rows.len());
        assert_eq!(100, rows[0].column_values()[0].int_value().unwrap());
    }

    #[test]
    fn insert_rows() {
        let table_entry = TableEntry::new(Table::new(
            "employees",
            schema!["id" => ColumnType::Int].unwrap(),
        ));
        let batch = Batch::new(rows![[10], [20]]);
        table_entry.insert_all(batch).unwrap();

        let rows = table_entry.scan().iter().collect::<Vec<_>>();
        assert_eq!(2, rows.len());

        assert!(rows.contains(&row![10]));
        assert!(rows.contains(&row![20]));
    }

    #[test]
    fn insert_row_and_get_by_row_id() {
        let table_entry = TableEntry::new(Table::new(
            "employees",
            schema!["id" => ColumnType::Int].unwrap(),
        ));
        let row_id = table_entry.insert(row![100]).unwrap();

        let row = table_entry.get(row_id).unwrap();
        assert_eq!(100, row.column_values()[0].int_value().unwrap());
    }

    #[test]
    fn insert_row_and_attempt_to_get_by_non_existent_row_id() {
        let table_entry = TableEntry::new(Table::new(
            "employees",
            schema!["id" => ColumnType::Int].unwrap(),
        ));
        table_entry.insert(row![100]).unwrap();

        let entry = table_entry.get(1000);
        assert!(entry.is_none());
    }
}
