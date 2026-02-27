use crate::query::executor::error::ExecutionError;
use crate::schema::Schema;
use crate::storage::row_view::RowView;

/// Represents the result of a query, providing access to the rows and column values.
///
/// `ResultSet` wraps a `TableIterator` and the associated `Table` metadata, allowing
/// iteration over rows and safe retrieval of column values by name.
/// Represents the result of a query, providing access to the rows and column values.
///
/// `ResultSet` acts as a factory for iterators. It owns the underlying data source (like `TableScan`),
/// enabling multiple iterations or consistent views.
///
/// # Design Decisions
///
/// `ResultSet` is designed as a factory for iterators rather than an iterator itself.
///
/// This separation decouples the ownership of the query result data from the specific state of iteration.
///
/// Consequently, this design:
/// - **Avoids Self-Referential Structs**: It prevents issues where a struct would need to hold both the data owner (`TableScan`) and the iterator that borrows from it.
/// - **Enables Thread Safety**: `ResultSet` remains immutable and can be safely shared across threads.
/// - **Allows Multiple Passes**: Consumers can create multiple independent iterators over the same result set.
pub trait ResultSet {
    // Return a boxed iterator that yields Result<RowView, ...>
    // The iterator is bound by the lifetime of &self
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowViewResult> + '_>, ExecutionError>;

    fn schema(&self) -> &Schema;
}

/// Represents the result for an individual RowView.
pub type RowViewResult<'a> = Result<RowView<'a>, ExecutionError>;
