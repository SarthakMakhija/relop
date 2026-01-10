use crate::schema::Schema;

/// Represents a table in the database catalog.
///
/// Holds the table's name and schema.
pub struct Table {
    name: String,
    schema: Schema,
}

impl Table {
    /// Creates a new `Table` with the given name and schema.
    pub fn new<N: Into<String>>(name: N, schema: Schema) -> Table {
        Self {
            name: name.into(),
            schema,
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

    /// Returns the table schema.
    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }
}
