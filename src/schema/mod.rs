pub mod column;
pub mod error;
pub mod primary_key;

use crate::schema::column::Column;
use crate::schema::error::SchemaError;
use crate::schema::primary_key::PrimaryKey;
use crate::types::column_type::ColumnType;
use crate::types::column_value::ColumnValue;

/// Represents the schema of a table, defining its columns and optional primary key.
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
    /// Creates a new, empty `Schema`.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::schema::Schema;
    ///
    /// let schema = Schema::new();
    /// ```
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            primary_key: None,
        }
    }

    /// Adds a column to the schema.
    ///
    /// Returns an error if a column with the same name already exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::schema::Schema;
    /// use relop::types::column_type::ColumnType;
    ///
    /// let schema = Schema::new()
    ///     .add_column("id", ColumnType::Int).unwrap()
    ///     .add_column("name", ColumnType::Text).unwrap();
    /// ```
    pub fn add_column(mut self, name: &str, column_type: ColumnType) -> Result<Self, SchemaError> {
        self.ensure_column_not_already_defined(name)?;

        self.columns.push(Column::new(name, column_type));
        Ok(self)
    }

    /// Adds a primary key to the schema.
    ///
    /// Returns an error if a primary key is already defined, or if the primary key columns do not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::schema::Schema;
    /// use relop::schema::primary_key::PrimaryKey;
    /// use relop::types::column_type::ColumnType;
    ///
    /// let schema = Schema::new()
    ///     .add_column("id", ColumnType::Int).unwrap()
    ///     .add_primary_key(PrimaryKey::single("id")).unwrap();
    /// ```
    pub fn add_primary_key(mut self, primary_key: PrimaryKey) -> Result<Self, SchemaError> {
        self.ensure_primary_key_not_already_defined()?;
        self.ensure_primary_key_columns_exist(&primary_key)?;

        self.primary_key = Some(primary_key);
        Ok(self)
    }

    /// Returns the position (index) of the column with the given name.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::schema::Schema;
    /// use relop::types::column_type::ColumnType;
    ///
    /// let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();
    /// assert_eq!(schema.column_position("id"), Some(0));
    /// ```
    pub fn column_position(&self, column_name: &str) -> Option<usize> {
        self.columns
            .iter()
            .enumerate()
            .find_map(|(position, column)| {
                if column.matches_name(column_name) {
                    return Some(position);
                }
                None
            })
    }

    /// Returns the number of columns in the schema.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::schema::Schema;
    /// use relop::types::column_type::ColumnType;
    ///
    /// let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();
    /// assert_eq!(schema.column_count(), 1);
    /// ```
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Checks if the schema has a primary key defined.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::schema::Schema;
    /// use relop::schema::primary_key::PrimaryKey;
    /// use relop::types::column_type::ColumnType;
    ///
    /// let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();
    /// assert!(!schema.has_primary_key());
    ///
    /// let schema = schema.add_primary_key(PrimaryKey::single("id")).unwrap();
    /// assert!(schema.has_primary_key());
    /// ```
    pub fn has_primary_key(&self) -> bool {
        self.primary_key.is_some()
    }

    /// Returns a reference to the primary key, if one is defined.
    pub(crate) fn primary_key(&self) -> Option<&PrimaryKey> {
        self.primary_key.as_ref()
    }

    /// Checks if the provided values are compatible with the schema's column types.
    ///
    /// Returns `Ok(())` if the values match the column count and types, otherwise returns a `SchemaError`.
    pub(crate) fn check_type_compatability(
        &self,
        values: &[ColumnValue],
    ) -> Result<(), SchemaError> {
        if values.len() != self.column_count() {
            return Err(SchemaError::ColumnCountMismatch {
                expected: self.columns.len(),
                actual: values.len(),
            });
        }

        for (index, column) in self.columns.iter().enumerate() {
            let value = &values[index];
            if !column.column_type().accepts(value) {
                return Err(SchemaError::ColumnTypeMismatch {
                    column: column.name().to_string(),
                    expected: column.column_type().clone(),
                    actual: value.column_type(),
                });
            }
        }
        Ok(())
    }

    fn ensure_column_not_already_defined(&self, name: &str) -> Result<(), SchemaError> {
        if self.has_column(name) {
            return Err(SchemaError::DuplicateColumnName(name.to_string()));
        }
        Ok(())
    }

    fn ensure_primary_key_not_already_defined(&self) -> Result<(), SchemaError> {
        if self.primary_key.is_some() {
            return Err(SchemaError::PrimaryKeyAlreadyDefined);
        }
        Ok(())
    }

    fn ensure_primary_key_columns_exist(
        &self,
        primary_key: &PrimaryKey,
    ) -> Result<(), SchemaError> {
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
        self.columns
            .iter()
            .any(|column| column.matches_name(column_name))
    }
}

#[cfg(test)]
impl Schema {
    fn get_column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    pub(crate) fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|column| column.name()).collect()
    }

    pub(crate) fn primary_key_column_names(&self) -> Option<&[String]> {
        if self.has_primary_key() {
            return Some(self.primary_key.as_ref().unwrap().column_names());
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_column_to_schema() {
        let mut schema = Schema::new();
        schema = schema.add_column("id", ColumnType::Int).unwrap();

        assert_eq!(1, schema.column_count());
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
            .add_column("id", ColumnType::Text);

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

        schema = schema.add_primary_key(PrimaryKey::single("id")).unwrap();
        assert!(schema.primary_key.is_some());
    }

    #[test]
    fn has_primary_key() {
        let mut schema = Schema::new();
        schema = schema.add_column("id", ColumnType::Int).unwrap();

        schema = schema.add_primary_key(PrimaryKey::single("id")).unwrap();
        assert!(schema.has_primary_key());
    }

    #[test]
    fn does_not_have_primary_key() {
        let mut schema = Schema::new();
        schema = schema.add_column("id", ColumnType::Int).unwrap();

        assert!(!schema.has_primary_key());
    }

    #[test]
    fn attempt_to_add_primary_key_to_schema_which_already_has_a_primary_key() {
        let mut schema = Schema::new();
        schema = schema.add_column("id", ColumnType::Int).unwrap();
        schema = schema.add_primary_key(PrimaryKey::single("id")).unwrap();

        let result = schema.add_primary_key(PrimaryKey::single("id"));
        assert!(matches!(result, Err(SchemaError::PrimaryKeyAlreadyDefined)))
    }

    #[test]
    fn attempt_to_add_primary_key_to_schema_with_a_column_that_does_not_exist_in_schema() {
        let schema = Schema::new();
        let result = schema.add_primary_key(PrimaryKey::single("id"));

        assert!(matches!(
            result,
            Err(SchemaError::PrimaryKeyColumnNotFound(ref column_name)) if column_name == "id"
        ));
    }

    #[test]
    fn column_position() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("name", ColumnType::Text)
            .unwrap();

        let position = schema.column_position("name").unwrap();
        assert_eq!(1, position)
    }

    #[test]
    fn attempt_to_get_column_position_of_a_column_that_does_not_exist_in_schema() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("name", ColumnType::Text)
            .unwrap();

        let position = schema.column_position("age");
        assert!(position.is_none());
    }

    #[test]
    fn column_count_mismatch() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("grade", ColumnType::Int)
            .unwrap();

        let result = schema.check_type_compatability(&[ColumnValue::text("relop")]);

        assert!(matches! (
            result,
            Err(SchemaError::ColumnCountMismatch{expected, actual}) if expected == 2 && actual == 1));
    }

    #[test]
    fn column_type_mismatch() {
        let mut schema = Schema::new();
        schema = schema.add_column("id", ColumnType::Int).unwrap();

        let result = schema.check_type_compatability(&[ColumnValue::text("relop")]);

        assert!(matches! (
            result,
            Err(SchemaError::ColumnTypeMismatch{column, expected, actual})
                if column == "id" && expected == ColumnType::Int && actual == ColumnType::Text));
    }

    #[test]
    fn type_compatible() {
        let mut schema = Schema::new();
        schema = schema.add_column("id", ColumnType::Int).unwrap();

        let result = schema.check_type_compatability(&[ColumnValue::int(100)]);
        assert!(result.is_ok());
    }

    #[test]
    fn column_names() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("name", ColumnType::Text)
            .unwrap();

        assert_eq!(vec!["id", "name"], schema.column_names());
    }

    #[test]
    fn primary_key_column_names() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_primary_key(PrimaryKey::single("id"))
            .unwrap();

        assert_eq!(vec!["id"], schema.primary_key_column_names().unwrap());
    }

    #[test]
    fn primary_key_column_names_given_no_primary_key() {
        let mut schema = Schema::new();
        schema = schema.add_column("id", ColumnType::Int).unwrap();

        assert!(schema.primary_key_column_names().is_none());
    }
}
