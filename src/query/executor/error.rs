use crate::catalog::error::CatalogError;

#[derive(Debug)]
pub(crate) enum ExecutionError {
    Catalog(CatalogError),
}
