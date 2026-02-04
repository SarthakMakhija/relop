use crate::schema::Schema;
use std::sync::Arc;

/// Represents a table in the database catalog.
///
/// Holds the table's name and schema.
pub struct Table {
    name: String,
    schema: Arc<Schema>,
}

impl Table {
    /// Creates a new `Table` with the given name and schema.
    pub fn new<N: Into<String>>(name: N, schema: Schema) -> Table {
        Self {
            name: name.into(),
            schema: Arc::new(schema),
        }
    }

    /// Returns the table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Checks if the table has a primary key.
    pub(crate) fn has_primary_key(&self) -> bool {
        self.schema.has_primary_key()
    }

    /// Returns the table schema reference.
    pub(crate) fn schema_ref(&self) -> &Schema {
        &self.schema
    }
}

#[cfg(test)]
impl Table {
    /// Returns the column names.
    pub(crate) fn column_names(&self) -> Vec<&str> {
        self.schema.column_names()
    }

    /// Returns the primary key column names.
    pub(crate) fn primary_key_column_names(&self) -> Option<&[String]> {
        self.schema.primary_key_column_names()
    }
}
