use crate::schema::Schema;

pub(crate) struct Table {
    name: String,
    schema: Schema,
}

impl Table {
    pub fn new<N: Into<String>>(name: N, schema: Schema) -> Table {
        Self {
            name: name.into(),
            schema,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn has_primary_key(&self) -> bool {
        self.schema.has_primary_key()
    }

    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }
}
