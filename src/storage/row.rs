pub struct Row {
    values: Vec<ColumnValue>
}

#[derive(Debug, PartialEq)]
pub enum ColumnValue {
    Int(i64),
    Text(String),
}

impl Row {
    pub fn empty() -> Row {
        Self {values: Vec::new()}
    }

    pub fn filled(values: Vec<ColumnValue>) -> Row {
        Self {values }
    }

    pub fn add(mut self, value: ColumnValue) -> Self {
        self.values.push(value);
        self
    }

    fn column_value_count(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
impl Row {
    fn column_value_at(&self, index: usize) -> Option<&ColumnValue> {
        self.values.get(index)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::row::{ColumnValue, Row};

    #[test]
    fn create_a_row_with_a_single_column_value() {
        let row = Row::empty().add(ColumnValue::Text("relop".to_string()));

        assert_eq!(1, row.column_value_count());
        assert_eq!(&ColumnValue::Text("relop".to_string()), row.column_value_at(0).unwrap());
    }

    #[test]
    fn create_a_row_with_two_column_values() {
        let row = Row::empty().add(ColumnValue::Text("relop".to_string())).add(ColumnValue::Int(100));

        assert_eq!(2, row.column_value_count());
        assert_eq!(&ColumnValue::Text("relop".to_string()), row.column_value_at(0).unwrap());
        assert_eq!(&ColumnValue::Int(100), row.column_value_at(1).unwrap());
    }

    #[test]
    fn create_a_filled_row_with_two_column_values() {
        let row = Row::filled(vec![ColumnValue::Text("relop".to_string()), ColumnValue::Int(200)]);

        assert_eq!(2, row.column_value_count());
        assert_eq!(&ColumnValue::Text("relop".to_string()), row.column_value_at(0).unwrap());
        assert_eq!(&ColumnValue::Int(200), row.column_value_at(1).unwrap());
    }
}