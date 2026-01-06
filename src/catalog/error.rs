#[derive(Debug, PartialEq)]
pub enum CatalogError {
    TableAlreadyExists(String),
}