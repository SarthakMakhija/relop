/// Represents a sort key in an `ORDER BY` clause.
///
/// It specifies which column to sort by and the direction of the sort.
pub(crate) struct OrderingKey {
    /// The name of the column to sort by.
    pub(crate) column: String,
    /// The direction of the sort (e.g., Ascending, Descending).
    pub(crate) direction: OrderingDirection,
}

/// Defines the direction of a sort order.
pub(crate) enum OrderingDirection {
    /// Ascending order (lowest to highest).
    Ascending,
    /// Descending order (highest to lowest).
    Descending,
}
