pub mod column;
pub mod error;

use crate::schema::column::Column;
use crate::schema::error::SchemaError;
use crate::types::column_type::ColumnType;
use crate::types::column_value::ColumnValue;

/// Represents the schema of a table, defining its columns.
pub struct Schema {
    columns: Vec<Column>,
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

    /// Returns the position (index) of the column with the given name.
    ///
    /// This method supports:
    /// - **Exact Match**: Matching a fully qualified name (e.g., "employees.id").
    /// - **Suffix Match**: Matching an unqualified name (e.g., "id") against a qualified column name.
    /// - **Case Insensitivity**: Matching is performed ignoring ASCII case.
    ///
    /// # Returns
    /// - `Ok(Some(index))`: If a single matching column is found.
    /// - `Ok(None)`: If no match is found, but the name is either unqualified or uses a valid prefix.
    /// - `Err(SchemaError::AmbiguousColumnName)`: If an unqualified name matches multiple columns.
    /// - `Err(SchemaError::TableAliasNotFound)`: If a qualified name uses a prefix that does not exist in the schema.
    ///
    /// # Examples
    ///
    /// ```
    /// use relop::schema::Schema;
    /// use relop::types::column_type::ColumnType;
    ///
    /// let schema = Schema::new().add_column("employees.id", ColumnType::Int).unwrap();
    ///
    /// // Exact match
    /// assert_eq!(schema.column_position("employees.id").unwrap(), Some(0));
    ///
    /// // Suffix match
    /// assert_eq!(schema.column_position("id").unwrap(), Some(0));
    ///
    /// // Case insensitivity
    /// assert_eq!(schema.column_position("ID").unwrap(), Some(0));
    /// ```
    pub fn column_position(&self, column_name: &str) -> Result<Option<usize>, SchemaError> {
        let matches: Vec<usize> = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.is_match(column_name))
            .map(|(position, _)| position)
            .collect();

        match matches.len() {
            1 => Ok(Some(matches[0])),
            n if n > 1 => Err(SchemaError::AmbiguousColumnName(column_name.to_string())),
            _ => self.validate_prefix(column_name),
        }
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

    /// Returns a reference to the primary key, if one is defined.
    /// Returns a slice of all columns defined in the schema.
    pub(crate) fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Merges this schema with another schema by combining their columns.
    /// Prefixes column names if the respective table prefix is provided.
    pub(crate) fn merge_with_prefixes(
        &self,
        left_prefix: Option<&str>,
        other: &Schema,
        right_prefix: Option<&str>,
    ) -> Self {
        let mut merged_columns = Vec::with_capacity(self.column_count() + other.column_count());

        Self::merge_column_name_with_prefix(left_prefix, &self.columns, &mut merged_columns);
        Self::merge_column_name_with_prefix(right_prefix, &other.columns, &mut merged_columns);

        Self {
            columns: merged_columns,
        }
    }

    /// Creates a new `Schema` with a prefix added to all column names.
    pub(crate) fn with_prefix(&self, prefix: &str) -> Self {
        let mut columns = Vec::with_capacity(self.columns.len());
        Self::merge_column_name_with_prefix(Some(prefix), &self.columns, &mut columns);
        Self { columns }
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

    fn has_column(&self, column_name: &str) -> bool {
        self.columns
            .iter()
            .any(|column| column.matches_name(column_name))
    }

    fn validate_prefix(&self, column_name: &str) -> Result<Option<usize>, SchemaError> {
        if let Some(dot_index) = column_name.rfind('.') {
            let prefix = &column_name[..dot_index];
            if !self.has_any_column_with_prefix(prefix) {
                return Err(SchemaError::TableAliasNotFound(prefix.to_string()));
            }
        }
        Ok(None)
    }

    fn has_any_column_with_prefix(&self, prefix: &str) -> bool {
        self.columns.iter().any(|column| column.has_prefix(prefix))
    }

    fn merge_column_name_with_prefix(
        prefix: Option<&str>,
        source: &Vec<Column>,
        columns: &mut Vec<Column>,
    ) {
        for column in source {
            let name = match prefix {
                Some(prefix) => format!("{}.{}", prefix, column.name()),
                None => column.name().to_string(),
            };
            columns.push(Column::new(name, column.column_type().clone()));
        }
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
    fn column_position() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("name", ColumnType::Text)
            .unwrap();

        let position = schema.column_position("name").unwrap().unwrap();
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

        let position = schema.column_position("age").unwrap();
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
    fn columns_from_schema() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("name", ColumnType::Text)
            .unwrap();

        let columns = schema.columns();
        assert_eq!(2, columns.len());
        assert_eq!("id", columns[0].name());
        assert_eq!("name", columns[1].name());
    }

    #[test]
    fn merge_schemas_with_prefixes() {
        let mut left_schema = Schema::new();
        left_schema = left_schema
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("name", ColumnType::Text)
            .unwrap();

        let mut right_schema = Schema::new();
        right_schema = right_schema
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("name", ColumnType::Text)
            .unwrap();

        let merged_schema =
            left_schema.merge_with_prefixes(Some("employees"), &right_schema, Some("departments"));

        assert_eq!(4, merged_schema.column_count());

        let columns = merged_schema.columns();
        assert_eq!("employees.id", columns[0].name());
        assert_eq!("employees.name", columns[1].name());
        assert_eq!("departments.id", columns[2].name());
        assert_eq!("departments.name", columns[3].name());
    }

    #[test]
    fn merge_schemas_without_one_prefix() {
        let mut left_schema = Schema::new();
        left_schema = left_schema
            .add_column("employees.id", ColumnType::Int)
            .unwrap()
            .add_column("employees.name", ColumnType::Text)
            .unwrap();

        let mut right_schema = Schema::new();
        right_schema = right_schema.add_column("id", ColumnType::Int).unwrap();

        let merged_schema =
            left_schema.merge_with_prefixes(None, &right_schema, Some("departments"));

        assert_eq!(3, merged_schema.column_count());

        let columns = merged_schema.columns();
        assert_eq!("employees.id", columns[0].name());
        assert_eq!("employees.name", columns[1].name());
        assert_eq!("departments.id", columns[2].name());
    }

    #[test]
    fn column_position_with_qualified_name() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("employees.id", ColumnType::Int)
            .unwrap()
            .add_column("departments.id", ColumnType::Int)
            .unwrap();

        assert_eq!(schema.column_position("employees.id").unwrap(), Some(0));
        assert_eq!(schema.column_position("departments.id").unwrap(), Some(1));
    }

    #[test]
    fn column_position_with_unqualified_lookup_against_qualified_schema() {
        let mut schema = Schema::new();
        schema = schema.add_column("employees.id", ColumnType::Int).unwrap();

        assert_eq!(schema.column_position("id").unwrap(), Some(0));
    }

    #[test]
    fn column_position_with_unqualified_lookup_against_multiple_qualified_schemas_fails() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("employees.id", ColumnType::Int)
            .unwrap()
            .add_column("departments.id", ColumnType::Int)
            .unwrap();

        let result = schema.column_position("id");
        assert!(matches!(
            result,
            Err(SchemaError::AmbiguousColumnName(ref column_name)) if column_name == "id"
        ));
    }

    #[test]
    fn column_position_with_ambiguous_name() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("employees.id", ColumnType::Int)
            .unwrap()
            .add_column("departments.id", ColumnType::Int)
            .unwrap();

        let result = schema.column_position("id");
        assert!(matches!(
            result,
            Err(SchemaError::AmbiguousColumnName(ref column_name)) if column_name == "id"
        ));
    }

    #[test]
    fn column_position_with_invalid_prefix_returns_table_alias_not_found() {
        let mut schema = Schema::new();
        schema = schema.add_column("employees.id", ColumnType::Int).unwrap();

        let result = schema.column_position("dep.id");
        assert!(matches!(
            result,
            Err(SchemaError::TableAliasNotFound(ref prefix)) if prefix == "dep"
        ));
    }

    #[test]
    fn column_position_with_valid_prefix_but_missing_column_returns_none() {
        let mut schema = Schema::new();
        schema = schema.add_column("employees.id", ColumnType::Int).unwrap();

        let result = schema.column_position("employees.age").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn schema_with_prefix() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("name", ColumnType::Text)
            .unwrap();

        let prefixed_schema = schema.with_prefix("e");

        assert_eq!(2, prefixed_schema.column_count());
        let columns = prefixed_schema.columns();
        assert_eq!("e.id", columns[0].name());
        assert_eq!("e.name", columns[1].name());
    }
}
