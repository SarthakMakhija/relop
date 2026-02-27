use crate::storage::row::Row;

/// A trait for filtering rows in the storage layer.
///
/// `RowFilter` allows the storage layer to execute filtering logic without
/// depending on the query layer's `Predicate` implementation.
pub(crate) trait RowFilter: Send + Sync {
    /// Returns `true` if the row satisfies the filter, `false` otherwise.
    fn matches(&self, row: &Row) -> bool;
}
