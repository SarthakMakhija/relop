use crate::schema::error::SchemaError;

pub struct PrimaryKey {
    column_names: Vec<String>,
}

impl PrimaryKey {
    pub fn new(column_name: &str) -> Self {
        PrimaryKey {
            column_names: vec![column_name.to_string()],
        }
    }

    pub fn composite(column_names: Vec<&str>) -> Result<Self, SchemaError> {
        let mut seen = std::collections::HashSet::new();
        let mut names = Vec::new();

        for name in column_names {
            if !seen.insert(name) {
                return Err(SchemaError::DuplicatePrimaryKeyColumnName(name.to_string()));
            }
            names.push(name.to_string());
        }

        Ok(Self { column_names: names })
    }


    pub(crate) fn column_names(&self) -> &[String] {
        &self.column_names
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_single_column_primary_key() {
        let primary_key = PrimaryKey::new("id");

        assert_eq!(1, primary_key.column_names.len());
    }

    #[test]
    fn create_composite_primary_key() {
        let primary_key = PrimaryKey::composite(vec!["id", "first_name"]).unwrap();

        assert_eq!(2, primary_key.column_names.len());
    }

    #[test]
    fn attempt_to_create_composite_primary_key_with_duplicate_column_names() {
        let result = PrimaryKey::composite(vec!["id", "first_name", "id"]);
        assert!(matches!(
            result,
            Err(SchemaError::DuplicatePrimaryKeyColumnName(column_name)) if column_name == "id"
        ));
    }

    #[test]
    fn get_primary_key_column_names() {
        let primary_key = PrimaryKey::composite(vec!["id", "first_name"]).unwrap();

        assert_eq!(primary_key.column_names(), vec!["id", "first_name"]);
    }
}