use crate::catalog::table::Table;
use std::sync::Arc;

pub(crate) struct TableDescriptor {
    table: Arc<Table>,
}

impl TableDescriptor {
    pub(crate) fn new(table: Arc<Table>) -> TableDescriptor {
        Self { table }
    }

    pub(crate) fn table_name(&self) -> &str {
        self.table.name()
    }

    pub(crate) fn column_names(&self) -> Vec<&str> {
        self.table.schema().column_names()
    }

    pub(crate) fn primary_key_column_names(&self) -> Option<&[String]> {
        self.table.schema().primary_key_column_names()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::primary_key::PrimaryKey;
    use crate::schema::Schema;
    use crate::types::column_type::ColumnType;

    #[test]
    fn table_name() {
        let table = Table::new(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        let table_descriptor = TableDescriptor::new(Arc::new(table));

        assert_eq!("employees", table_descriptor.table_name());
    }

    #[test]
    fn column_names() {
        let table = Table::new(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );
        let table_descriptor = TableDescriptor::new(Arc::new(table));

        assert_eq!(vec!["id"], table_descriptor.column_names());
    }

    #[test]
    fn primary_key_column_names() {
        let table = Table::new(
            "employees",
            Schema::new()
                .add_column("id", ColumnType::Int)
                .unwrap()
                .add_primary_key(PrimaryKey::single("id"))
                .unwrap(),
        );
        let table_descriptor = TableDescriptor::new(Arc::new(table));

        assert_eq!(
            vec!["id"],
            table_descriptor.primary_key_column_names().unwrap()
        );
    }
}
