pub mod primary_key;
pub mod error;
mod column;

use crate::schema::column::Column;
use crate::schema::column::ColumnType;
use crate::schema::primary_key::PrimaryKey;
use crate::schema::error::SchemaError;

pub struct Schema {
    columns: Vec<Column>,
    primary_key: Option<PrimaryKey>,
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

impl Schema {
    pub fn new() -> Self {
        Self { columns: Vec::new(), primary_key: None }
    }

    pub fn add_column(mut self, name: &str, column_type: ColumnType) -> Result<Self, SchemaError> {
        self.ensure_column_not_defined(name)?;

        self.columns.push(Column::new(name, column_type));
        Ok(self)
    }

    pub fn add_primary_key(mut self, primary_key: PrimaryKey) -> Result<Self, SchemaError> {
        self.ensure_primary_key_not_defined()?;
        self.ensure_primary_key_columns_exist(&primary_key)?;

        self.primary_key = Some(primary_key);
        Ok(self)
    }

    pub fn total_columns(&self) -> usize {
        self.columns.len()
    }

    fn ensure_column_not_defined(&self, name: &str) -> Result<(), SchemaError> {
        if self.has_column(name) {
            return Err(SchemaError::DuplicateColumnName(name.to_string()));
        }
        Ok(())
    }

    fn ensure_primary_key_not_defined(&self) -> Result<(), SchemaError> {
        if self.primary_key.is_some() {
            return Err(SchemaError::PrimaryKeyAlreadyDefined);
        }
        Ok(())
    }

    fn ensure_primary_key_columns_exist(&self, primary_key: &PrimaryKey) -> Result<(), SchemaError> {
        for primary_key_column_name in primary_key.column_names() {
            if !self.has_column(primary_key_column_name) {
                return Err(SchemaError::PrimaryKeyColumnNotFound(
                    primary_key_column_name.to_string(),
                ));
            }
        }
        Ok(())
    }

    fn has_column(&self, column_name: &str) -> bool {
        self.columns.iter().any(|column| column.matches_name(column_name))
    }
}

#[cfg(test)]
impl Schema {
    fn get_column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_column_to_schema() {
        let mut schema = Schema::new();
        schema = schema.add_column("id", ColumnType::Int).unwrap();

        assert_eq!(1, schema.total_columns());
    }

    #[test]
    fn get_column_from_schema() {
        let mut schema = Schema::new();
        schema = schema.add_column("id", ColumnType::Int).unwrap();

        let column = schema.get_column(0).unwrap();

        assert_eq!("id", column.name());
        assert_eq!(ColumnType::Int, *column.column_type());
    }

    #[test]
    fn add_column_with_the_same_name_to_schema() {
        let schema = Schema::new();
        let result = schema
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("id", ColumnType::String);

        assert!(matches!(
            result,
            Err(SchemaError::DuplicateColumnName(ref column_name)) if column_name == "id"
        ));
    }

    #[test]
    fn attempt_to_get_at_an_index_beyond_the_number_of_columns() {
        let schema = Schema::new();
        let column = schema.get_column(1);

        assert!(column.is_none());
    }

    #[test]
    fn add_primary_key_to_schema() {
        let mut schema = Schema::new();
        schema = schema.add_column("id", ColumnType::Int).unwrap();

        schema = schema.add_primary_key(PrimaryKey::new("id")).unwrap();

        assert!(schema.primary_key.is_some());
    }

    #[test]
    fn attempt_to_add_primary_key_to_schema_which_already_has_a_primary_key() {
        let mut schema = Schema::new();
        schema = schema.add_column("id", ColumnType::Int).unwrap();
        schema = schema.add_primary_key(PrimaryKey::new("id")).unwrap();

        let result = schema.add_primary_key(PrimaryKey::new("id"));
        assert!(
            matches!(
                result,
                Err(SchemaError::PrimaryKeyAlreadyDefined)
            )
        )
    }

    #[test]
    fn attempt_to_add_primary_key_to_schema_with_a_column_that_does_not_exist() {
        let schema = Schema::new();
        let result = schema.add_primary_key(PrimaryKey::new("id"));

        assert!(matches!(
            result,
            Err(SchemaError::PrimaryKeyColumnNotFound(ref column_name)) if column_name == "id"
        ));
    }
}