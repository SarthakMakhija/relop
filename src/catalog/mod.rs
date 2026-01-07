use crate::catalog::error::CatalogError;
use crate::catalog::table::Table;
use crate::catalog::table_entry::TableEntry;
use crate::schema::Schema;
use crate::storage::row::Row;
use crate::storage::table_store::RowId;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

mod error;
pub(crate) mod table;
pub(crate) mod table_entry;

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
        let table_entry = self.table_entry(table_name);
        if let Some(table_entry) = table_entry {
            return Ok(table_entry.insert(row));
        }
        Err(CatalogError::TableDoesNotExist(table_name.to_string()))
    }

    pub(crate) fn get(&self, table_name: &str, row_id: RowId) -> Result<Option<Row>, CatalogError> {
        let table_entry = self.table_entry(table_name);
        if let Some(table_entry) = table_entry {
            return Ok(table_entry.get(row_id));
        }
        Err(CatalogError::TableDoesNotExist(table_name.to_string()))
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
    use crate::storage::row::ColumnValue;

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
}
