/// Represents errors that can occur during batch operations.
#[derive(Debug, PartialEq)]
pub enum BatchError {
    /// Indicates that a duplicate primary key was encountered in the batch.
    DuplicatePrimaryKey,
}
