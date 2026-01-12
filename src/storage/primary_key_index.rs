use crate::catalog::error::InsertError;
use crate::storage::primary_key_column_values::PrimaryKeyColumnValues;
use crate::storage::table_store::RowId;
use std::collections::HashMap;
use std::sync::RwLock;

/// An in-memory index for enforcing primary key uniqueness.
pub(crate) struct PrimaryKeyIndex {
    index: RwLock<HashMap<PrimaryKeyColumnValues, RowId>>,
}

impl PrimaryKeyIndex {
    /// Creates a new, empty `PrimaryKeyIndex`.
    pub(crate) fn new() -> PrimaryKeyIndex {
        Self {
            index: RwLock::new(HashMap::new()),
        }
    }

    /// Inserts a primary key value and its associated row ID into the index.
    ///
    /// # Panics
    ///
    /// Panics if the primary key already exists. Uniqueness should be verified before calling this method.
    pub(crate) fn insert(&self, key: PrimaryKeyColumnValues, row_id: RowId) {
        let mut index = self.index.write().unwrap();
        let old = index.insert(key, row_id);

        debug_assert!(
            old.is_none(),
            "PrimaryKeyIndex invariant violated: duplicate key inserted"
        );
    }

    /// Checks if a primary key value exists in the index.
    pub(crate) fn contains(&self, key: &PrimaryKeyColumnValues) -> bool {
        let index = self.index.read().unwrap();
        index.contains_key(key)
    }

    /// Retrieves the `RowId` associated with a primary key value.
    #[allow(dead_code)]
    pub(crate) fn get(&self, key: &PrimaryKeyColumnValues) -> Option<RowId> {
        let index = self.index.read().unwrap();
        index.get(key).cloned()
    }

    /// Checks that none of the provided primary keys already exist in the index.
    ///
    /// # Arguments
    ///
    /// * `keys` - A slice of primary key values to check.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If no duplicates are found.
    /// * `Err(InsertError::DuplicatePrimaryKey)` - If any of the keys already exist.
    pub(crate) fn ensure_no_duplicates(
        &self,
        keys: &[PrimaryKeyColumnValues],
    ) -> Result<(), InsertError> {
        for key in keys {
            if self.contains(key) {
                return Err(InsertError::DuplicatePrimaryKey);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::primary_key::PrimaryKey;
    use crate::schema::Schema;
    use crate::storage::row::Row;
    use crate::types::column_type::ColumnType;
    use crate::types::column_value::ColumnValue;

    #[test]
    fn insert_a_single_primary_key_column_value_in_index() {
        let mut schema = Schema::new();
        schema = schema.add_column("first_name", ColumnType::Text).unwrap();

        let row = Row::filled(vec![ColumnValue::Text("relop".to_string())]);
        let primary_key = PrimaryKey::single("first_name");

        let primary_key_column_values = PrimaryKeyColumnValues::new(&row, &primary_key, &schema);

        let row_id = 100;

        let index = PrimaryKeyIndex::new();
        index.insert(primary_key_column_values, row_id);

        let primary_key_column_values = PrimaryKeyColumnValues::new(&row, &primary_key, &schema);
        assert!(index.contains(&primary_key_column_values));
    }

    #[test]
    #[should_panic]
    fn attempt_to_add_duplicate_primary_key() {
        let mut schema = Schema::new();
        schema = schema.add_column("first_name", ColumnType::Text).unwrap();

        let row = Row::filled(vec![ColumnValue::Text("relop".to_string())]);
        let primary_key = PrimaryKey::single("first_name");

        let primary_key_column_values = PrimaryKeyColumnValues::new(&row, &primary_key, &schema);

        let row_id = 100;

        let index = PrimaryKeyIndex::new();
        index.insert(primary_key_column_values, row_id);

        index.insert(
            PrimaryKeyColumnValues::new(&row, &primary_key, &schema),
            row_id,
        );
    }

    #[test]
    fn insert_a_composite_primary_key_column_value_in_index() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("first_name", ColumnType::Text)
            .unwrap()
            .add_column("id", ColumnType::Int)
            .unwrap();

        let row = Row::filled(vec![
            ColumnValue::Text("relop".to_string()),
            ColumnValue::Int(200),
        ]);
        let primary_key = PrimaryKey::composite(vec!["first_name", "id"]).unwrap();

        let primary_key_column_values = PrimaryKeyColumnValues::new(&row, &primary_key, &schema);

        let row_id = 100;

        let index = PrimaryKeyIndex::new();
        index.insert(primary_key_column_values, row_id);

        let primary_key_column_values = PrimaryKeyColumnValues::new(&row, &primary_key, &schema);
        assert!(index.contains(&primary_key_column_values));
    }

    #[test]
    fn get_row_id_from_index() {
        let mut schema = Schema::new();
        schema = schema.add_column("first_name", ColumnType::Text).unwrap();

        let row = Row::filled(vec![ColumnValue::Text("relop".to_string())]);
        let primary_key = PrimaryKey::single("first_name");

        let primary_key_column_values = PrimaryKeyColumnValues::new(&row, &primary_key, &schema);

        let row_id = 100;

        let index = PrimaryKeyIndex::new();
        index.insert(primary_key_column_values, row_id);

        let primary_key_column_values = PrimaryKeyColumnValues::new(&row, &primary_key, &schema);
        assert_eq!(row_id, index.get(&primary_key_column_values).unwrap());
    }

    #[test]
    fn attempt_to_get_non_existing_index_key() {
        let mut schema = Schema::new();
        schema = schema.add_column("first_name", ColumnType::Text).unwrap();

        let row = Row::filled(vec![ColumnValue::Text("relop".to_string())]);
        let primary_key = PrimaryKey::single("first_name");

        let index = PrimaryKeyIndex::new();

        let primary_key_column_values = PrimaryKeyColumnValues::new(&row, &primary_key, &schema);
        assert!(index.get(&primary_key_column_values).is_none());
    }

    #[test]
    fn should_not_contain_index_key() {
        let mut schema = Schema::new();
        schema = schema.add_column("first_name", ColumnType::Text).unwrap();

        let row = Row::filled(vec![ColumnValue::Text("relop".to_string())]);
        let primary_key = PrimaryKey::single("first_name");

        let index = PrimaryKeyIndex::new();

        let primary_key_column_values = PrimaryKeyColumnValues::new(&row, &primary_key, &schema);
        assert!(!index.contains(&primary_key_column_values));
    }

    #[test]
    fn duplicate_primary_key_value() {
        let mut schema = Schema::new();
        schema = schema.add_column("first_name", ColumnType::Text).unwrap();

        let row = Row::filled(vec![ColumnValue::Text("relop".to_string())]);
        let primary_key = PrimaryKey::single("first_name");
        let primary_key_column_values = PrimaryKeyColumnValues::new(&row, &primary_key, &schema);

        let index = PrimaryKeyIndex::new();
        index.insert(primary_key_column_values, 100);

        let result =
            index.ensure_no_duplicates(&[PrimaryKeyColumnValues::new(&row, &primary_key, &schema)]);
        assert!(matches!(result, Err(InsertError::DuplicatePrimaryKey)));
    }

    #[test]
    fn no_duplicate_primary_key_value() {
        let mut schema = Schema::new();
        schema = schema.add_column("first_name", ColumnType::Text).unwrap();

        let row = Row::filled(vec![ColumnValue::Text("relop".to_string())]);
        let primary_key = PrimaryKey::single("first_name");
        let index = PrimaryKeyIndex::new();

        let result =
            index.ensure_no_duplicates(&[PrimaryKeyColumnValues::new(&row, &primary_key, &schema)]);
        assert!(result.is_ok());
    }
}
