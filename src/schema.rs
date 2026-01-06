pub struct Schema {
    columns: Vec<Column>,
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

impl Schema {
    pub fn new() -> Self {
        Self { columns: Vec::new() }
    }

    pub fn add_column(mut self, name: &str, column_type: ColumnType) -> Self {
        self.columns.push(Column { name: name.to_string(), column_type });
        self
    }

    pub fn total_columns(&self) -> usize {
        self.columns.len()
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
}