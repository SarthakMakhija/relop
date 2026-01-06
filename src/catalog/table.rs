use crate::schema::Schema;

pub(crate) struct Table {
    name: String,
    schema: Schema,
}

impl Table {
    pub fn new(name: String, schema: Schema) -> Table {
        Self { name, schema }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
