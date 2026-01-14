use crate::query::parser::ordering_key::OrderingKey;
use crate::query::parser::projection::Projection;

/// `Ast` represents the Abstract Syntax Tree for SQL statements.
pub(crate) enum Ast {
    /// Represents a `SHOW TABLES` statement.
    ShowTables,
    /// Represents a `DESCRIBE TABLE` statement.
    DescribeTable {
        /// The name of the table to describe.
        table_name: String,
    },
    /// Represents a `SELECT` statement.
    Select {
        /// The name of the table to select from.
        table_name: String,
        /// The projection (columns or all) to select.
        projection: Projection,
        /// The ORDER BY clause, defining the columns and directions used to order rows.
        order_by: Option<Vec<OrderingKey>>,
        /// The LIMIT (max records) to return.
        limit: Option<usize>,
    },
}
