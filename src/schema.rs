pub struct Schema {
    columns: Vec<Column>,
    primary_key: Option<PrimaryKey>,
}

pub(crate) struct Column {
    name: String,
    column_type: ColumnType,
}

#[derive(Debug, PartialEq)]
pub enum ColumnType {
    Int,
    Boolean,
}

pub struct PrimaryKey {
    column_names: Vec<String>,
}

impl PrimaryKey {
    pub fn new(column_name: &str) -> Self {
        Self::composite(vec![column_name])
    }
    
    pub fn composite(column_names: Vec<&str>) -> Self {
        Self {
            column_names: column_names.iter().map(|name| name.to_string()).collect(),
        }
    }

    fn column_names(&self) -> &[String] {
        &self.column_names
    }
}

impl Schema {
    pub fn new() -> Self {
        Self { columns: Vec::new(), primary_key: None }
    }

    pub fn add_column(mut self, name: &str, column_type: ColumnType) -> Self {
        self.columns.push(Column { name: name.to_string(), column_type });
        self
    }

    pub fn add_primary_key(mut self, primary_key: PrimaryKey) -> Self {
        primary_key.column_names().iter().for_each(|primary_key_column_name| {
            if !self.contains_column(primary_key_column_name) {
                panic!("Primary key column not found in schema");
            }
        });

        self.primary_key = Some(primary_key);
        self
    }

    pub fn total_columns(&self) -> usize {
        self.columns.len()
    }

    fn contains_column(&self, column_name: &str) -> bool {
        self.columns.iter().any(|column| column.name == column_name)
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
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
        schema = schema.add_column("id", ColumnType::Int);
        
        assert_eq!(1, schema.total_columns());
    }

    #[test]
    fn get_column_from_schema() {
        let mut schema = Schema::new();
        schema = schema.add_column("id", ColumnType::Int);
        
        let column = schema.get_column(0).unwrap();

        assert_eq!("id", column.name);
        assert_eq!(column.column_type, ColumnType::Int);
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
        schema = schema.add_column("id", ColumnType::Int);

        schema = schema.add_primary_key(PrimaryKey::new("id"));
        
        assert!(schema.primary_key.is_some());
    }

    #[test]
    #[should_panic]
    fn attempt_to_add_primary_key_to_schema_with_a_column_that_does_not_exist() {
        let schema = Schema::new();
        schema.add_primary_key(PrimaryKey::new("id"));
    }
}