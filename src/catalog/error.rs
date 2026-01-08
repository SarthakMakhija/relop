use crate::schema::error::SchemaError;

#[derive(Debug, PartialEq)]
pub enum CatalogError {
    TableAlreadyExists(String),
    TableDoesNotExist(String),

}
#[derive(Debug, PartialEq)]
pub enum InsertError {
    Catalog(CatalogError),
    Schema(SchemaError),
    DuplicatePrimaryKey,
}
