#[derive(Debug, PartialEq)]
pub enum CatalogError {
    TableAlreadyExists(String),
    TableDoesNotExist(String),
    DuplicatePrimaryKey,
}