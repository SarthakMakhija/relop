pub(crate) mod predicate;

use crate::query::parser::ast::{Ast, WhereClause};
use crate::query::parser::ordering_key::OrderingKey;
use crate::query::parser::projection::Projection;
use crate::query::plan::predicate::Predicate;

/// `LogicalPlan` represents the logical steps required to execute a query.
#[derive(Eq, PartialEq)]
pub(crate) enum LogicalPlan {
    /// Plan to show table names.
    ShowTables,
    /// Plan to describe a table's schema.
    DescribeTable {
        /// Name of the table.
        table_name: String,
    },
    /// Plan to scan a table.
    Scan {
        /// Name of the table.
        table_name: String,
    },
    /// Plan to project specific columns from a base plan.
    Projection {
        /// The source plan.
        base_plan: Box<LogicalPlan>,
        /// The columns to project.
        columns: Vec<String>,
    },
    Filter {
        /// The source plan.
        base_plan: Box<LogicalPlan>,
        //// The filter predicate.
        predicate: Predicate,
    },
    /// Plan to limit results from a base plan.
    Limit {
        /// The source plan.
        base_plan: Box<LogicalPlan>,
        /// The limit value.
        count: usize,
    },
    /// Plan to order the results.
    Sort {
        /// The source plan.
        base_plan: Box<LogicalPlan>,
        /// The ordering keys.
        ordering_keys: Vec<OrderingKey>,
    },
}

impl LogicalPlan {
    /// Wraps the `LogicalPlan` in a `Box`.
    pub(crate) fn boxed(self) -> Box<LogicalPlan> {
        Box::new(self)
    }
}

/// `LogicalPlanner` converts an Abstract Syntax Tree (AST) into a `LogicalPlan`.
pub(crate) struct LogicalPlanner;

impl LogicalPlanner {
    /// Converts a given `Ast` into a `LogicalPlan`.
    /// The plan hierarchy is:
    /// Scan → Filter → Projection → Sort → Limit
    pub(crate) fn plan(ast: Ast) -> LogicalPlan {
        match ast {
            Ast::ShowTables => LogicalPlan::ShowTables,
            Ast::DescribeTable { table_name } => LogicalPlan::DescribeTable { table_name },
            Ast::Select {
                table_name,
                projection,
                where_clause,
                limit,
                order_by,
            } => {
                let base_plan = LogicalPlan::Scan { table_name };
                let base_plan = Self::plan_for_filter(where_clause, base_plan);
                let base_plan = Self::plan_for_projection(projection, base_plan);
                let base_plan = Self::plan_for_sort(order_by, base_plan);
                Self::plan_for_limit(limit, base_plan)
            }
        }
    }

    fn plan_for_projection(projection: Projection, base_plan: LogicalPlan) -> LogicalPlan {
        match projection {
            Projection::All => base_plan,
            Projection::Columns(columns) => LogicalPlan::Projection {
                base_plan: base_plan.boxed(),
                columns,
            },
        }
    }

    fn plan_for_filter(where_clause: Option<WhereClause>, base_plan: LogicalPlan) -> LogicalPlan {
        if let Some(clause) = where_clause {
            return LogicalPlan::Filter {
                base_plan: base_plan.boxed(),
                predicate: Predicate::from(clause),
            };
        }
        base_plan
    }

    fn plan_for_sort(order_by: Option<Vec<OrderingKey>>, base_plan: LogicalPlan) -> LogicalPlan {
        if let Some(keys) = order_by {
            return LogicalPlan::Sort {
                base_plan: base_plan.boxed(),
                ordering_keys: keys,
            };
        }
        base_plan
    }

    fn plan_for_limit(limit: Option<usize>, base_plan: LogicalPlan) -> LogicalPlan {
        if let Some(value) = limit {
            return LogicalPlan::Limit {
                base_plan: base_plan.boxed(),
                count: value,
            };
        }
        base_plan
    }
}

#[cfg(test)]
impl LogicalPlan {
    /// Creates a plan to show tables.
    pub(crate) fn show_tables() -> Self {
        LogicalPlan::ShowTables
    }

    /// Creates a plan to describe a table.
    pub(crate) fn describe_table<T: Into<String>>(table_name: T) -> Self {
        LogicalPlan::DescribeTable {
            table_name: table_name.into(),
        }
    }

    /// Creates a plan to scan a table.
    pub(crate) fn scan<T: Into<String>>(table_name: T) -> Self {
        LogicalPlan::Scan {
            table_name: table_name.into(),
        }
    }

    /// Creates a plan to project columns.
    pub(crate) fn project<T: Into<String>>(self, columns: Vec<T>) -> Self {
        LogicalPlan::Projection {
            base_plan: self.boxed(),
            columns: columns.into_iter().map(|column| column.into()).collect(),
        }
    }

    /// Creates a plan to limit results.
    pub(crate) fn limit(self, count: usize) -> Self {
        LogicalPlan::Limit {
            base_plan: self.boxed(),
            count,
        }
    }

    /// Creates a plan to order results.
    pub(crate) fn order_by(self, ordering_keys: Vec<OrderingKey>) -> Self {
        LogicalPlan::Sort {
            base_plan: self.boxed(),
            ordering_keys,
        }
    }

    /// Creates a plan to filter results.
    pub(crate) fn filter(self, predicate: Predicate) -> Self {
        LogicalPlan::Filter {
            base_plan: self.boxed(),
            predicate,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{Literal, Operator};
    use crate::query::parser::projection::Projection;
    use crate::query::plan::predicate::LogicalOperator;
    use crate::{asc, desc};

    #[test]
    fn logical_plan_for_show_tables() {
        let logical_plan = LogicalPlanner::plan(Ast::ShowTables);
        assert!(matches!(logical_plan, LogicalPlan::ShowTables));
    }

    #[test]
    fn logical_plan_for_describe_table() {
        let logical_plan = LogicalPlanner::plan(Ast::DescribeTable {
            table_name: "employees".to_string(),
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::DescribeTable { table_name } if table_name == "employees"
        ));
    }

    #[test]
    fn logical_plan_for_select_all() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::All,
            where_clause: None,
            order_by: None,
            limit: None,
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Scan { table_name } if table_name == "employees"
        ));
    }

    #[test]
    fn logical_plan_for_select_with_projection() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::Columns(vec![String::from("id")]),
            where_clause: None,
            order_by: None,
            limit: None,
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Projection {base_plan: _, columns } if columns.iter().eq(&["id"])
        ));
    }

    #[test]
    fn logical_plan_for_select_with_projection_validating_the_base_plan() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::Columns(vec![String::from("id")]),
            where_clause: None,
            order_by: None,
            limit: None,
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Projection {base_plan, columns: _ }
                if matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name } if table_name == "employees")
        ));
    }

    #[test]
    fn logical_plan_for_select_with_where_clause() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::All,
            where_clause: Some(WhereClause::Comparison {
                column_name: "age".to_string(),
                operator: Operator::Greater,
                literal: Literal::Int(30),
            }),
            order_by: None,
            limit: None,
        });

        assert!(matches!(
            logical_plan,
            LogicalPlan::Filter { base_plan, predicate }
                if predicate == Predicate::comparison("age", LogicalOperator::Greater, Literal::Int(30))
                    && matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name } if table_name == "employees")
        ));
    }

    #[test]
    fn logical_plan_for_select_with_projection_and_where_clause() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::Columns(vec![String::from("id")]),
            where_clause: Some(WhereClause::Comparison {
                column_name: "age".to_string(),
                operator: Operator::Greater,
                literal: Literal::Int(30),
            }),
            order_by: None,
            limit: None,
        });

        assert!(matches!(
            logical_plan,
            LogicalPlan::Projection {base_plan, columns} if columns == vec!["id"]
                && matches!(
                base_plan.as_ref(),
                LogicalPlan::Filter { base_plan, predicate }
                if *predicate == Predicate::comparison("age", LogicalOperator::Greater, Literal::Int(30))
                    && matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name } if table_name == "employees")
            )
        ));
    }

    #[test]
    fn logical_plan_for_select_with_order_by_ascending() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::All,
            where_clause: None,
            order_by: Some(vec![asc!("id")]),
            limit: None,
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Sort {base_plan, ordering_keys }
                if ordering_keys == vec![asc!("id")] &&
                    matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name } if table_name == "employees") ));
    }

    #[test]
    fn logical_plan_for_select_with_order_by_descending() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::All,
            where_clause: None,
            order_by: Some(vec![desc!("id")]),
            limit: None,
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Sort {base_plan, ordering_keys }
                if ordering_keys == vec![desc!("id")] &&
                    matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name } if table_name == "employees") ));
    }

    #[test]
    fn logical_plan_for_select_with_order_by_multiple_columns() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::All,
            where_clause: None,
            order_by: Some(vec![asc!("id"), desc!("name")]),
            limit: None,
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Sort {base_plan, ordering_keys }
                if ordering_keys == vec![asc!("id"), desc!("name")] &&
                    matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name } if table_name == "employees") ));
    }

    #[test]
    fn logical_plan_for_select_all_with_limit_base_plan() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::All,
            where_clause: None,
            order_by: None,
            limit: Some(10),
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Limit { base_plan, count: _ } if matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name } if table_name == "employees")
        ));
    }

    #[test]
    fn logical_plan_for_select_all_with_limit_count() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::All,
            where_clause: None,
            order_by: None,
            limit: Some(10),
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Limit { base_plan: _, count } if count == 10
        ));
    }

    #[test]
    fn logical_plan_for_select_with_projection_and_limit() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::Columns(vec![String::from("id")]),
            where_clause: None,
            order_by: None,
            limit: Some(10),
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Limit { base_plan: _, count } if count == 10
        ));
    }

    #[test]
    fn logical_plan_for_select_with_projection_and_limit_validating_the_columns() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::Columns(vec![String::from("id")]),
            where_clause: None,
            order_by: None,
            limit: Some(10),
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Limit {base_plan, count: _ }
                if matches!(base_plan.as_ref(), LogicalPlan::Projection { base_plan: _, columns }
                if columns.iter().eq(&[String::from("id")]) )
        ));
    }

    #[test]
    fn logical_plan_for_select_with_projection_and_limit_validating_the_base_plan() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::Columns(vec![String::from("id")]),
            where_clause: None,
            order_by: None,
            limit: Some(10),
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Limit {base_plan, count: _ }
                if matches!(base_plan.as_ref(), LogicalPlan::Projection { base_plan, columns: _ }
                    if matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name } if table_name == "employees") )
        ));
    }

    #[test]
    fn logical_plan_for_select_with_order_by_and_limit() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::All,
            where_clause: None,
            order_by: Some(vec![asc!("id"), desc!("name")]),
            limit: Some(10),
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Limit {base_plan, count}
                if count == 10 && matches!(base_plan.as_ref(), LogicalPlan::Sort { base_plan, ordering_keys }
                    if *ordering_keys == vec![asc!("id"), desc!("name")] &&
                        matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name } if table_name == "employees")
            )
        ));
    }
}
