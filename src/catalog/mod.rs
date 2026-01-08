use crate::catalog::error::CatalogError;
use crate::catalog::table::Table;
use crate::catalog::table_entry::TableEntry;
use crate::catalog::table_scan::TableScan;
use crate::schema::Schema;
use crate::storage::row::Row;
use crate::storage::table_store::RowId;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

mod error;
pub(crate) mod table;
pub(crate) mod table_entry;
mod table_scan;

struct Catalog {
    tables: RwLock<HashMap<String, Arc<TableEntry>>>,
}

impl Catalog {
    pub(crate) fn new() -> Catalog {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn create_table(&self, name: &str, schema: Schema) -> Result<(), CatalogError> {
        let mut tables = self.tables.write().unwrap();

        if tables.contains_key(name) {
            return Err(CatalogError::TableAlreadyExists(name.to_string()));
        }

        let table = Table::new(name.to_string(), schema);
        tables.insert(name.to_string(), TableEntry::new(table));

        Ok(())
    }

    pub(crate) fn insert_into(&self, table_name: &str, row: Row) -> Result<RowId, CatalogError> {
        let table_entry = self.table_entry_or_error(table_name)?;
        table_entry.insert(row)
    }

    pub(crate) fn insert_all_into(
        &self,
        table_name: &str,
        rows: Vec<Row>,
    ) -> Result<Vec<RowId>, CatalogError> {
        let table_entry = self.table_entry_or_error(table_name)?;
        Ok(table_entry.insert_all(rows))
    }

    pub(crate) fn get(&self, table_name: &str, row_id: RowId) -> Result<Option<Row>, CatalogError> {
        let table_entry = self.table_entry_or_error(table_name)?;
        Ok(table_entry.get(row_id))
    }

    pub(crate) fn scan(&self, table_name: &str) -> Result<TableScan, CatalogError> {
        let table_entry = self.table_entry_or_error(table_name)?;
        Ok(table_entry.scan())
    }

    fn table_entry_or_error(&self, table_name: &str) -> Result<Arc<TableEntry>, CatalogError> {
        let table_entry = self
            .table_entry(table_name)
            .ok_or_else(|| CatalogError::TableDoesNotExist(table_name.to_string()))?;

        Ok(table_entry)
    }

    fn table_entry(&self, name: &str) -> Option<Arc<TableEntry>> {
        let guard = self.tables.read().unwrap();
        guard.get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::column::ColumnType;
    use crate::schema::primary_key::PrimaryKey;
    use crate::values::column_value::ColumnValue;

    #[test]
    fn create_table() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );

        assert!(result.is_ok());
    }

    #[test]
    fn create_table_without_a_primary_key_index() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );

        assert!(result.is_ok());

        let table_entry = catalog.table_entry("employees").unwrap();
        assert!(!table_entry.has_primary_key_index());
    }

    #[test]
    fn create_table_with_a_primary_key_index() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new()
                .add_column("id", ColumnType::Int)
                .unwrap()
                .add_primary_key(PrimaryKey::single("id"))
                .unwrap(),
        );

        assert!(result.is_ok());

        let table_entry = catalog.table_entry("employees").unwrap();
        assert!(table_entry.has_primary_key_index());
    }

    #[test]
    fn create_table_and_get_table_by_name() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );

        assert!(result.is_ok());

        let table_entry = catalog.table_entry("employees").unwrap();
        assert_eq!("employees", table_entry.table_name());
    }

    #[test]
    fn get_table_by_non_existing_name() {
        let catalog = Catalog::new();

        let table_entry = catalog.table_entry("employees");
        assert!(table_entry.is_none());
    }

    #[test]
    fn attempt_to_create_an_already_created_table() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());

        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(matches!(
            result,
            Err(CatalogError::TableAlreadyExists(ref table_name)) if table_name == "employees"));
    }

    #[test]
    fn insert_into_table() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());

        let row_id = catalog
            .insert_into(
                "employees",
                Row::filled(vec![
                    ColumnValue::Int(1),
                    ColumnValue::Text("relop".to_string()),
                ]),
            )
            .unwrap();

        let row = catalog.get("employees", row_id).unwrap().unwrap();
        let expected_row = Row::filled(vec![
            ColumnValue::Int(1),
            ColumnValue::Text("relop".to_string()),
        ]);

        assert_eq!(expected_row, row);
    }

    #[test]
    fn attempt_to_insert_into_non_existent_table() {
        let catalog = Catalog::new();

        let result = catalog.insert_into(
            "employees",
            Row::filled(vec![
                ColumnValue::Int(1),
                ColumnValue::Text("relop".to_string()),
            ]),
        );

        assert!(
            matches!(result, Err(CatalogError::TableDoesNotExist(ref table_name)) if table_name == "employees")
        );
    }

    #[test]
    fn insert_all_into_table() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());

        let row_ids = catalog
            .insert_all_into(
                "employees",
                vec![
                    Row::filled(vec![
                        ColumnValue::Int(1),
                        ColumnValue::Text("relop".to_string()),
                    ]),
                    Row::filled(vec![
                        ColumnValue::Int(2),
                        ColumnValue::Text("operator".to_string()),
                    ]),
                ],
            )
            .unwrap();

        assert_eq!(2, row_ids.len());

        let row = catalog
            .get("employees", *row_ids.first().unwrap())
            .unwrap()
            .unwrap();
        let expected_row = Row::filled(vec![
            ColumnValue::Int(1),
            ColumnValue::Text("relop".to_string()),
        ]);
        assert_eq!(expected_row, row);

        let row = catalog
            .get("employees", *row_ids.last().unwrap())
            .unwrap()
            .unwrap();

        let expected_row = Row::filled(vec![
            ColumnValue::Int(2),
            ColumnValue::Text("operator".to_string()),
        ]);
        assert_eq!(expected_row, row);
    }

    #[test]
    fn attempt_to_insert_all_into_non_existent_table() {
        let catalog = Catalog::new();
        let result = catalog.insert_all_into(
            "employees",
            vec![
                Row::filled(vec![
                    ColumnValue::Int(1),
                    ColumnValue::Text("relop".to_string()),
                ]),
                Row::filled(vec![
                    ColumnValue::Int(2),
                    ColumnValue::Text("operator".to_string()),
                ]),
            ],
        );

        assert!(matches!(
            result,
            Err(CatalogError::TableDoesNotExist(ref table_name)) if table_name == "employees"
        ))
    }

    #[test]
    fn get_by_row_id_from_table() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());

        let row_id = catalog
            .insert_into(
                "employees",
                Row::filled(vec![
                    ColumnValue::Int(1),
                    ColumnValue::Text("relop".to_string()),
                ]),
            )
            .unwrap();

        let row = catalog.get("employees", row_id).unwrap().unwrap();
        let expected_row = Row::filled(vec![
            ColumnValue::Int(1),
            ColumnValue::Text("relop".to_string()),
        ]);

        assert_eq!(expected_row, row);
    }

    #[test]
    fn attempt_to_get_by_row_id_from_non_existent_table() {
        let catalog = Catalog::new();

        let result = catalog.get("employees", 1);
        assert!(
            matches!(result, Err(CatalogError::TableDoesNotExist(ref table_name)) if table_name == "employees")
        );
    }

    #[test]
    fn insert_into_table_and_scan() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        assert!(result.is_ok());

        catalog
            .insert_into(
                "employees",
                Row::filled(vec![
                    ColumnValue::Int(1),
                    ColumnValue::Text("relop".to_string()),
                ]),
            )
            .unwrap();

        let rows = catalog
            .scan("employees")
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(1, rows.len());

        let expected_row = Row::filled(vec![
            ColumnValue::Int(1),
            ColumnValue::Text("relop".to_string()),
        ]);

        assert_eq!(expected_row, rows[0]);
    }

    #[test]
    fn attempt_to_scan_a_non_existent_table() {
        let catalog = Catalog::new();
        let result = catalog.scan("employees");

        assert!(
            matches!(result, Err(CatalogError::TableDoesNotExist(ref table_name)) if table_name == "employees")
        );
    }
}

#[cfg(test)]
mod table_insert_and_index_tests {
    use crate::catalog::Catalog;
    use crate::schema::column::ColumnType;
    use crate::schema::primary_key::PrimaryKey;
    use crate::schema::Schema;
    use crate::storage::primary_key_column_values::PrimaryKeyColumnValues;
    use crate::storage::row::Row;
    use crate::values::column_value::ColumnValue;

    #[test]
    fn insert_into_table_with_primary_key() {
        let catalog = Catalog::new();
        let result = catalog.create_table(
            "employees",
            Schema::new()
                .add_column("id", ColumnType::Int)
                .unwrap()
                .add_primary_key(PrimaryKey::single("id"))
                .unwrap(),
        );
        assert!(result.is_ok());

        catalog
            .insert_into(
                "employees",
                Row::filled(vec![
                    ColumnValue::Int(1),
                    ColumnValue::Text("relop".to_string()),
                ]),
            )
            .unwrap();

        let row = Row::filled(vec![
            ColumnValue::Int(1),
            ColumnValue::Text("relop".to_string()),
        ]);
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_primary_key(PrimaryKey::single("id"))
            .unwrap();

        let primary_key = PrimaryKey::single("id");
        let primary_key_column_values = PrimaryKeyColumnValues::new(&row, &primary_key, &schema);

        let table_entry = catalog.table_entry("employees").unwrap();
        let primary_key_index = table_entry.primary_key_index().unwrap();
        let row_id = primary_key_index.get(&primary_key_column_values);

        assert!(row_id.is_some());
    }
}
