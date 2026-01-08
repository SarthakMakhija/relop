use crate::schema::primary_key::PrimaryKey;
use crate::schema::Schema;
use crate::storage::row::{ColumnValue, Row};

#[derive(Hash, Eq, PartialEq)]
pub(crate) struct PrimaryKeyColumnValues {
    values: Vec<ColumnValue>,
}

impl PrimaryKeyColumnValues {
    pub(crate) fn new(row: &Row, primary_key: &PrimaryKey, schema: &Schema) -> PrimaryKeyColumnValues {
        let column_values = primary_key.column_names().iter().map(|column_name| {
            //SAFETY: PrimaryKey validates that the column names are present in Schema.
            //So, unwrap is safe.
            let position = schema.column_position(column_name).unwrap();

            //SAFETY: During row insertion, the system checks that the order of column values in Row
            //matches the order defined in Schema.
            //Another check is made to ensure that the column values in Row have the same datatype
            //corresponding to the columns defined in Schema.
            //So, unwrap is safe.
            row.column_value_at(position).unwrap().clone()
        }).collect::<Vec<ColumnValue>>();

        Self {
            values: column_values
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::column::ColumnType;
    use crate::schema::primary_key::PrimaryKey;
    use crate::schema::Schema;
    use crate::storage::primary_key_column_values::PrimaryKeyColumnValues;
    use crate::storage::row::{ColumnValue, Row};

    #[test]
    fn create_primary_key_column_values() {
        let mut schema = Schema::new();
        schema = schema.add_column("first_name", ColumnType::Text).unwrap().add_column("id", ColumnType::Int).unwrap();

        let row = Row::filled(vec![ColumnValue::Text("relop".to_string()), ColumnValue::Int(200)]);
        let primary_key = PrimaryKey::composite(vec!["first_name", "id"]).unwrap();

        let primary_key_column_values = PrimaryKeyColumnValues::new(
            &row,
            &primary_key,
            &schema,
        );
        assert_eq!(2, primary_key_column_values.values.len());

        let first_name_value = primary_key_column_values.values.first().unwrap().text_value().unwrap();
        assert_eq!("relop", first_name_value);

        let id_value = primary_key_column_values.values.last().unwrap().int_value().unwrap();
        assert_eq!(200, id_value);
    }
}