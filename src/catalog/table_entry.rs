use crate::catalog::error::InsertError;
use crate::catalog::table::Table;
use crate::catalog::table_descriptor::TableDescriptor;
use crate::catalog::table_scan::TableScan;
use crate::storage::batch::Batch;
use crate::storage::primary_key_column_values::PrimaryKeyColumnValues;
use crate::storage::primary_key_index::PrimaryKeyIndex;
use crate::storage::row::Row;
use crate::storage::table_store::{RowId, TableStore};
use std::sync::{Arc, Mutex};

/// `TableEntry` represents a table in the catalog throughout its lifetime.
/// It holds a reference to the `Table` definition, the underlying `TableStore` for data storage,
/// and an optional `PrimaryKeyIndex` for enforcing primary key constraints and fast lookups.
///
/// `TableEntry` is responsible for managing concurrent access to the table data, ensuring
/// thread safety during insertions.
pub(crate) struct TableEntry {
    table: Arc<Table>,
    store: Arc<TableStore>,
    primary_key_index: Option<PrimaryKeyIndex>,
    insert_lock: Mutex<()>,
}

impl TableEntry {
    /// Creates a new `TableEntry` for the given `Table`.
    ///
    /// This initializes the `TableStore` and, if the table has a primary key,
    /// the `PrimaryKeyIndex`.
    pub(crate) fn new(table: Table) -> Arc<TableEntry> {
        let primary_key_index = Self::maybe_primary_key_index(&table);
        Arc::new(Self {
            table: Arc::new(table),
            store: Arc::new(TableStore::new()),
            primary_key_index,
            insert_lock: Mutex::new(()),
        })
    }

    /// Inserts a single row into the table.
    ///
    /// If the table has a primary key, this method checks for duplicate primary keys
    /// before insertion. If a duplicate is found, `InsertError::DuplicatePrimaryKey` is returned.
    ///
    /// This method is thread-safe and uses an internal lock to ensure atomicity of the
    /// check-and-insert operation when a primary key index is present.
    pub(crate) fn insert(&self, row: Row) -> Result<RowId, InsertError> {
        let _guard = self.insert_lock.lock().unwrap();

        if let Some(primary_key_index) = &self.primary_key_index {
            let schema = self.table.schema_ref();
            //SAFETY: primary_key_index can only be created if the Table has a primary key.
            //If table has a primary key, we can safely unwrap() primary_key() from schema.
            let primary_key = schema.primary_key().unwrap();
            let primary_key_column_values = PrimaryKeyColumnValues::new(&row, primary_key, schema);

            if primary_key_index.contains(&primary_key_column_values) {
                return Err(InsertError::DuplicatePrimaryKey);
            }
            let row_id = self.store.insert(row);
            primary_key_index.insert(primary_key_column_values, row_id);

            Ok(row_id)
        } else {
            Ok(self.store.insert(row))
        }
    }

    /// Inserts a batch of rows into the table.
    ///
    /// Similar to `insert`, this method enforces primary key constraints if applicable.
    /// It ensures that neither the new batch nor the existing data contains duplicate primary keys.
    ///
    /// This operation is atomic with respect to the primary key check and insertion.
    pub(crate) fn insert_all(&self, batch: Batch) -> Result<Vec<RowId>, InsertError> {
        let _guard = self.insert_lock.lock().unwrap();

        if let Some(primary_key_index) = &self.primary_key_index {
            let schema = self.table.schema_ref();
            let all_primary_key_column_values = batch
                .unique_primary_key_values(schema)
                .map_err(|_| InsertError::DuplicatePrimaryKey)?;

            primary_key_index.ensure_no_duplicates(&all_primary_key_column_values)?;

            let row_ids = batch
                .into_rows()
                .into_iter()
                .map(|row| self.store.insert(row))
                .collect::<Vec<RowId>>();

            all_primary_key_column_values
                .into_iter()
                .zip(row_ids.iter().copied())
                .for_each(|(primary_key_column_values, row_id)| {
                    primary_key_index.insert(primary_key_column_values, row_id);
                });

            Ok(row_ids)
        } else {
            Ok(self.store.insert_all(batch.into_rows()))
        }
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

    /// Returns a `TableDescriptor` for the table, which contains metadata about the table.
    pub(crate) fn table_descriptor(&self) -> TableDescriptor {
        TableDescriptor::new(self.table.clone())
    }

    /// Returns a reference to the `Table` definition.
    pub(crate) fn table_ref(&self) -> &Table {
        &self.table
    }

    /// Returns a specific `Arc` reference to the `Table` definition.
    pub(crate) fn table(&self) -> Arc<Table> {
        self.table.clone()
    }

    fn maybe_primary_key_index(table: &Table) -> Option<PrimaryKeyIndex> {
        if table.has_primary_key() {
            return Some(PrimaryKeyIndex::new());
        }
        None
    }
}

#[cfg(test)]
impl TableEntry {
    /// Returns the name of the table.
    pub(crate) fn table_name(&self) -> &str {
        self.table.name()
    }

    /// Checks if the table has an associated primary key index.
    pub(crate) fn has_primary_key_index(&self) -> bool {
        self.primary_key_index.is_some()
    }

    /// Returns a reference to the `PrimaryKeyIndex`, if one exists.
    pub(crate) fn primary_key_index(&self) -> Option<&PrimaryKeyIndex> {
        self.primary_key_index.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row;
    use crate::rows;
    use crate::schema;
    use crate::schema::primary_key::PrimaryKey;
    use crate::test_utils::create_schema_with_primary_key;
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
    fn insert_row_with_primary_key() {
        let table_entry = TableEntry::new(Table::new(
            "employees",
            create_schema_with_primary_key(&[("id", ColumnType::Int)], "id"),
        ));
        table_entry.insert(row![100]).unwrap();

        let rows = table_entry.scan().iter().collect::<Vec<_>>();
        assert_eq!(1, rows.len());
        assert_eq!(100, rows[0].column_values()[0].int_value().unwrap());

        let schema = create_schema_with_primary_key(&[("id", ColumnType::Int)], "id");

        let row = row![100];
        let primary_key = PrimaryKey::single("id");
        let primary_key_column_values = PrimaryKeyColumnValues::new(&row, &primary_key, &schema);

        let primary_key_index = table_entry.primary_key_index.as_ref().unwrap();
        let row_id = primary_key_index.get(&primary_key_column_values);

        assert!(row_id.is_some());
    }

    #[test]
    fn attempt_to_insert_row_with_duplicate_primary_key() {
        let table_entry = TableEntry::new(Table::new(
            "employees",
            create_schema_with_primary_key(&[("id", ColumnType::Int)], "id"),
        ));
        table_entry.insert(row![100]).unwrap();

        let result = table_entry.insert(row![100]);
        assert!(matches!(result, Err(InsertError::DuplicatePrimaryKey)));
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
    fn insert_rows_with_duplicate_primary_key_values() {
        let table_entry = TableEntry::new(Table::new(
            "employees",
            create_schema_with_primary_key(&[("id", ColumnType::Int)], "id"),
        ));
        let batch = Batch::new(rows![[10], [10]]);
        let result = table_entry.insert_all(batch);
        assert!(matches!(result, Err(InsertError::DuplicatePrimaryKey)));
    }

    #[test]
    fn insert_rows_with_duplicate_primary_key_values_in_index() {
        let table_entry = TableEntry::new(Table::new(
            "employees",
            create_schema_with_primary_key(&[("id", ColumnType::Int)], "id"),
        ));
        let batch = Batch::new(rows![[10], [20]]);
        let result = table_entry.insert_all(batch);
        assert!(result.is_ok());

        let batch = Batch::new(rows![[10]]);
        let result = table_entry.insert_all(batch);
        assert!(matches!(result, Err(InsertError::DuplicatePrimaryKey)));
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

    #[test]
    fn should_create_primary_key_index() {
        let table_entry = TableEntry::new(Table::new(
            "employees",
            create_schema_with_primary_key(&[("id", ColumnType::Int)], "id"),
        ));
        assert!(table_entry.has_primary_key_index());
    }

    #[test]
    fn should_not_create_primary_key_index() {
        let table_entry = TableEntry::new(Table::new(
            "employees",
            schema!["id" => ColumnType::Int].unwrap(),
        ));
        assert!(!table_entry.has_primary_key_index());
    }
}
