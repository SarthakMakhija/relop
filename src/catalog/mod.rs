use crate::catalog::error::CatalogError;
use crate::catalog::table::Table;
use crate::catalog::table_entry::TableEntry;
use crate::schema::Schema;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

mod error;
pub(crate) mod table;
pub(crate) mod table_entry;

struct Catalog {
    tables: RwLock<HashMap<String, TableEntry>>,
}

impl Catalog {
    pub(crate) fn new() -> Catalog {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn create_table(&self, name: &str, schema: Schema) -> Result<(), CatalogError> {
        self.ensure_table_is_not_already_created(name)?;

        let table = Table::new(name.to_string(), schema);
        self.tables
            .write()
            .unwrap()
            .insert(name.to_string(), TableEntry::new(table));

        Ok(())
    }

    fn get_table(&self, name: &str) -> Option<Arc<Table>> {
        let guard = self.tables.read().unwrap();
        guard.get(name).map(|entry| entry.table())
    }

    fn ensure_table_is_not_already_created(&self, name: &str) -> Result<(), CatalogError> {
        if self.tables.read().unwrap().contains_key(name) {
            return Err(CatalogError::TableAlreadyExists(name.to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::column::ColumnType;

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

        let table_ref = catalog.get_table("employees").unwrap();
        let table = table_ref.as_ref();

        assert_eq!("employees", table.name());
    }

    #[test]
    fn get_table_by_non_existing_name() {
        let catalog = Catalog::new();

        let table_ref = catalog.get_table("employees");
        assert!(table_ref.is_none());
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
}
