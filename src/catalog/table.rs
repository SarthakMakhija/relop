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
    
    pub(crate) fn has_primary_key(&self) -> bool {
        self.schema.has_primary_key()
    }
}
