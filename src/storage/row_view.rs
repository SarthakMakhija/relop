use crate::catalog::table::Table;
use crate::storage::row::Row;
use crate::types::column_value::ColumnValue;

pub struct RowView<'a> {
    row: Row,
    table: &'a Table,
}

impl<'a> RowView<'a> {
    pub fn new(row: Row, table: &'a Table) -> Self {
        Self { row, table }
    }

    pub fn column(&self, column_name: &str) -> Option<&ColumnValue> {
        let column_position = self.table.schema().column_position(column_name)?;
        self.row.column_value_at(column_position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Schema;
    use crate::types::column_type::ColumnType;

    #[test]
    fn column() {
        let table = Table::new(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );

        let row = Row::filled(vec![ColumnValue::Int(200)]);

        let view = RowView::new(row, &table);
        assert_eq!(&ColumnValue::Int(200), view.column("id").unwrap());
    }

    #[test]
    fn attempt_to_get_non_existing_column() {
        let table = Table::new(
            "employees",
            Schema::new().add_column("id", ColumnType::Int).unwrap(),
        );

        let row = Row::filled(vec![ColumnValue::Int(200)]);

        let view = RowView::new(row, &table);
        assert!(view.column("name").is_none());
    }
}
