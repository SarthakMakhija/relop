use crate::schema::primary_key::PrimaryKey;
use crate::schema::Schema;
use crate::types::column_type::ColumnType;

/// Creates a new `Schema` with the given columns.
///
/// # Arguments
///
/// * `columns` - A slice of tuples, where each tuple contains the column name and type.
///
/// # Examples
///
/// ```
/// use relop::types::column_type::ColumnType;
/// use relop::test_utils::create_schema;
///
/// let schema = create_schema(&[("id", ColumnType::Int), ("name", ColumnType::Text)]);
/// assert_eq!(schema.column_count(), 2);
/// ```
pub fn create_schema(columns: &[(&str, ColumnType)]) -> Schema {
    let mut schema = Schema::new();
    for (name, col_type) in columns {
        schema = schema.add_column(name, col_type.clone()).unwrap();
    }
    schema
}

pub fn create_schema_with_primary_key(columns: &[(&str, ColumnType)], primary_key: &str) -> Schema {
    let mut schema = Schema::new();
    for (name, col_type) in columns {
        schema = schema.add_column(name, col_type.clone()).unwrap();
    }
    schema
        .add_primary_key(PrimaryKey::single(primary_key))
        .unwrap()
}
