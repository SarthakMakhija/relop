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
        let primary_key = PrimaryKey::composite(vec!["id", "first_name"]);

        assert_eq!(2, primary_key.column_names.len());
    }

    #[test]
    fn get_primary_key_column_names() {
        let primary_key = PrimaryKey::composite(vec!["id", "first_name"]);

        assert_eq!(primary_key.column_names(), vec!["id", "first_name"]);
    }
}