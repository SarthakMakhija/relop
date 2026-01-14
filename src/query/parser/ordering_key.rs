/// Represents a sort key in an `ORDER BY` clause.
///
/// It specifies which column to sort by and the direction of the sort.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct OrderingKey {
    /// The name of the column to sort by.
    pub(crate) column: String,
    /// The direction of the sort (e.g., Ascending, Descending).
    pub(crate) direction: OrderingDirection,
}

/// Defines the direction of a sort order.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum OrderingDirection {
    /// Ascending order (lowest to highest).
    Ascending,
    /// Descending order (highest to lowest).
    Descending,
}

impl OrderingKey {
    /// Creates an `OrderingKey` for the specified column in ascending order.
    pub(crate) fn ascending_by<C: Into<String>>(column_name: C) -> Self {
        OrderingKey {
            column: column_name.into(),
            direction: OrderingDirection::Ascending,
        }
    }

    /// Creates an `OrderingKey` for the specified column in descending order.
    pub(crate) fn descending_by<C: Into<String>>(column_name: C) -> Self {
        OrderingKey {
            column: column_name.into(),
            direction: OrderingDirection::Descending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascending_by() {
        let key = OrderingKey::ascending_by("id");
        assert_eq!(key.column, "id");
        assert_eq!(key.direction, OrderingDirection::Ascending);
    }

    #[test]
    fn descending_by() {
        let key = OrderingKey::descending_by("rank");
        assert_eq!(key.column, "rank");
        assert_eq!(key.direction, OrderingDirection::Descending);
    }
}
