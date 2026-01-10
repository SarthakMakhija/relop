use crate::catalog::error::CatalogError;

#[derive(Debug)]
pub enum ExecutionError {
    Catalog(CatalogError),
}
