pub(crate) mod error;
pub(crate) mod predicate;

use crate::query::parser::ast::{Ast, WhereClause};
use crate::query::parser::ordering_key::OrderingKey;
use crate::query::parser::projection::Projection;
use crate::query::plan::error::PlanningError;
use crate::query::plan::predicate::Predicate;

/// `LogicalPlan` represents the logical steps required to execute a query.
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
        /// The name of the table to scan.
        table_name: String,
        /// The optional alias for the table.
        alias: Option<String>,
    },
    /// Plan to perform a join between two tables.
    Join {
        /// The left-hand plan.
        left: Box<LogicalPlan>,
        /// The right-hand plan.
        right: Box<LogicalPlan>,
        /// The optional ON condition over joined rows.
        on: Option<Predicate>,
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
    pub(crate) fn plan(ast: Ast) -> Result<LogicalPlan, PlanningError> {
        match ast {
            Ast::ShowTables => Ok(LogicalPlan::ShowTables),
            Ast::DescribeTable { table_name } => Ok(LogicalPlan::DescribeTable { table_name }),
            Ast::Select {
                source,
                projection,
                where_clause,
                limit,
                order_by,
            } => {
                let base_plan = Self::plan_for_source(source)?;
                let base_plan = Self::plan_for_filter(where_clause, base_plan)?;
                let base_plan = Self::plan_for_projection(projection, base_plan);
                let base_plan = Self::plan_for_sort(order_by, base_plan);
                Ok(Self::plan_for_limit(limit, base_plan))
            }
        }
    }

    fn plan_for_source(
        source: crate::query::parser::ast::TableSource,
    ) -> Result<LogicalPlan, PlanningError> {
        match source {
            crate::query::parser::ast::TableSource::Table { name, alias } => {
                Ok(LogicalPlan::Scan {
                    table_name: name,
                    alias,
                })
            }
            crate::query::parser::ast::TableSource::Join { left, right, on } => {
                let left_plan = Self::plan_for_source(*left)?;
                let right_plan = Self::plan_for_source(*right)?;

                let on_predicate = match on {
                    Some(expression) => Some(Predicate::try_from(expression)?),
                    None => None,
                };

                Ok(LogicalPlan::Join {
                    left: left_plan.boxed(),
                    right: right_plan.boxed(),
                    on: on_predicate,
                })
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

    fn plan_for_filter(
        where_clause: Option<WhereClause>,
        base_plan: LogicalPlan,
    ) -> Result<LogicalPlan, PlanningError> {
        if let Some(clause) = where_clause {
            return Ok(LogicalPlan::Filter {
                base_plan: base_plan.boxed(),
                predicate: Predicate::try_from(clause)?,
            });
        }
        Ok(base_plan)
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
            alias: None,
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
    use crate::query::parser::ast::{BinaryOperator, Literal};
    use crate::query::parser::projection::Projection;
    use crate::query::plan::predicate::LogicalOperator;
    use crate::{asc, desc};

    #[test]
    fn logical_plan_for_show_tables() {
        let logical_plan = LogicalPlanner::plan(Ast::ShowTables).unwrap();
        assert!(matches!(logical_plan, LogicalPlan::ShowTables));
    }

    #[test]
    fn logical_plan_for_describe_table() {
        let logical_plan = LogicalPlanner::plan(Ast::DescribeTable {
            table_name: "employees".to_string(),
        })
        .unwrap();
        assert!(matches!(
            logical_plan,
            LogicalPlan::DescribeTable { table_name } if table_name == "employees"
        ));
    }

    #[test]
    fn logical_plan_for_select_all() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::table("employees"),
            projection: Projection::All,
            where_clause: None,
            order_by: None,
            limit: None,
        })
        .unwrap();
        assert!(matches!(
            logical_plan,
            LogicalPlan::Scan { table_name, .. } if table_name == "employees"
        ));
    }

    #[test]
    fn logical_plan_for_select_with_projection() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::table("employees"),
            projection: Projection::Columns(vec!["id".to_string()]),
            where_clause: None,
            order_by: None,
            limit: None,
        })
        .unwrap();
        assert!(matches!(
            logical_plan,
            LogicalPlan::Projection {base_plan: _, columns } if columns.iter().eq(&["id"])
        ));
    }

    #[test]
    fn logical_plan_for_select_with_projection_validating_the_base_plan() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::table("employees"),
            projection: Projection::Columns(vec!["id".to_string()]),
            where_clause: None,
            order_by: None,
            limit: None,
        })
        .unwrap();
        assert!(matches!(
            logical_plan,
            LogicalPlan::Projection {base_plan, columns: _ }
                if matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name, .. } if table_name == "employees")
        ));
    }

    #[test]
    fn logical_plan_for_select_with_where_clause() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::table("employees"),
            projection: Projection::All,
            where_clause: Some(WhereClause::comparison(
                Literal::ColumnReference("age".to_string()),
                BinaryOperator::Greater,
                Literal::Int(30),
            )),
            order_by: None,
            limit: None,
        })
        .unwrap();

        assert!(matches!(
            logical_plan,
            LogicalPlan::Filter { base_plan, predicate }
                if matches!(&predicate, Predicate::Single(predicate::LogicalClause::Comparison { ref lhs, ref operator, ref rhs })
                    if matches!(lhs, Literal::ColumnReference(ref name) if name == "age") && *operator == LogicalOperator::Greater && *rhs == Literal::Int(30))
                        && matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name, .. } if table_name == "employees")
        ));
    }

    #[test]
    fn logical_plan_for_select_with_projection_and_where_clause() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::table("employees"),
            projection: Projection::Columns(vec![String::from("id")]),
            where_clause: Some(WhereClause::comparison(
                Literal::ColumnReference("age".to_string()),
                BinaryOperator::Greater,
                Literal::Int(30),
            )),
            order_by: None,
            limit: None,
        })
        .unwrap();

        assert!(matches!(
            logical_plan,
            LogicalPlan::Projection {base_plan, columns} if columns == vec!["id"]
                && matches!(
                base_plan.as_ref(),
                LogicalPlan::Filter { base_plan, predicate }
                if matches!(predicate, Predicate::Single(predicate::LogicalClause::Comparison { ref lhs, ref operator, ref rhs })
                    if matches!(lhs, Literal::ColumnReference(ref name) if name == "age") && *operator == LogicalOperator::Greater && *rhs == Literal::Int(30))
                        && matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name, .. } if table_name == "employees")
            )
        ));
    }

    #[test]
    fn logical_plan_for_select_with_order_by_ascending() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::table("employees"),
            projection: Projection::All,
            where_clause: None,
            order_by: Some(vec![asc!("id")]),
            limit: None,
        })
        .unwrap();
        assert!(matches!(
            logical_plan,
            LogicalPlan::Sort {base_plan, ordering_keys }
                if ordering_keys == vec![asc!("id")] &&
                    matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name, .. } if table_name == "employees") ));
    }

    #[test]
    fn logical_plan_for_select_with_order_by_descending() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::table("employees"),
            projection: Projection::All,
            where_clause: None,
            order_by: Some(vec![desc!("id")]),
            limit: None,
        })
        .unwrap();
        assert!(matches!(
            logical_plan,
            LogicalPlan::Sort {base_plan, ordering_keys }
                if ordering_keys == vec![desc!("id")] &&
                    matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name, .. } if table_name == "employees") ));
    }

    #[test]
    fn logical_plan_for_select_with_order_by_multiple_columns() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::table("employees"),
            projection: Projection::All,
            where_clause: None,
            order_by: Some(vec![asc!("id"), desc!("name")]),
            limit: None,
        })
        .unwrap();
        assert!(matches!(
            logical_plan,
            LogicalPlan::Sort {base_plan, ordering_keys }
                if ordering_keys == vec![asc!("id"), desc!("name")] &&
                    matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name, .. } if table_name == "employees") ));
    }

    #[test]
    fn logical_plan_for_select_all_with_limit_base_plan() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::table("employees"),
            projection: Projection::All,
            where_clause: None,
            order_by: None,
            limit: Some(10),
        })
        .unwrap();
        assert!(matches!(
            logical_plan,
            LogicalPlan::Limit { base_plan, count: _ } if matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name, .. } if table_name == "employees")
        ));
    }

    #[test]
    fn logical_plan_for_select_all_with_limit_count() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::table("employees"),
            projection: Projection::All,
            where_clause: None,
            order_by: None,
            limit: Some(10),
        })
        .unwrap();
        assert!(matches!(
            logical_plan,
            LogicalPlan::Limit { base_plan: _, count } if count == 10
        ));
    }

    #[test]
    fn logical_plan_for_select_with_projection_and_limit() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::table("employees"),
            projection: Projection::Columns(vec![String::from("id")]),
            where_clause: None,
            order_by: None,
            limit: Some(10),
        })
        .unwrap();
        assert!(matches!(
            logical_plan,
            LogicalPlan::Limit { base_plan: _, count } if count == 10
        ));
    }

    #[test]
    fn logical_plan_for_select_with_projection_and_limit_validating_the_columns() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::table("employees"),
            projection: Projection::Columns(vec![String::from("id")]),
            where_clause: None,
            order_by: None,
            limit: Some(10),
        })
        .unwrap();
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
            source: crate::query::parser::ast::TableSource::table("employees"),
            projection: Projection::Columns(vec![String::from("id")]),
            where_clause: None,
            order_by: None,
            limit: Some(10),
        })
        .unwrap();
        assert!(matches!(
            logical_plan,
            LogicalPlan::Limit {base_plan, count: _ }
                if matches!(base_plan.as_ref(), LogicalPlan::Projection { base_plan, columns: _ }
                    if matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name, .. } if table_name == "employees") )
        ));
    }

    #[test]
    fn logical_plan_for_select_with_order_by_and_limit() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::table("employees"),
            projection: Projection::All,
            where_clause: None,
            order_by: Some(vec![asc!("id"), desc!("name")]),
            limit: Some(10),
        })
        .unwrap();
        assert!(matches!(
            logical_plan,
            LogicalPlan::Limit {base_plan, count}
                if count == 10 && matches!(base_plan.as_ref(), LogicalPlan::Sort { base_plan, ordering_keys }
                    if *ordering_keys == vec![asc!("id"), desc!("name")] &&
                        matches!(base_plan.as_ref(), LogicalPlan::Scan { table_name, .. } if table_name == "employees")
            )
        ));
    }

    #[test]
    fn logical_plan_for_select_with_join() {
        use crate::query::parser::ast::Clause;

        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::Join {
                left: Box::new(crate::query::parser::ast::TableSource::table("employees")),
                right: Box::new(crate::query::parser::ast::TableSource::table("departments")),
                on: Some(crate::query::parser::ast::Expression::Single(
                    Clause::Comparison {
                        lhs: Literal::ColumnReference("employee_id".to_string()),
                        operator: BinaryOperator::Eq,
                        rhs: Literal::ColumnReference("department_id".to_string()),
                    },
                )),
            },
            projection: Projection::All,
            where_clause: None,
            order_by: None,
            limit: None,
        })
        .unwrap();

        assert!(matches!(
            logical_plan,
            LogicalPlan::Join { left, right, on }
            if matches!(left.as_ref(), LogicalPlan::Scan { table_name, .. } if table_name == "employees")
            && matches!(right.as_ref(), LogicalPlan::Scan { table_name, .. } if table_name == "departments")
            && matches!(
                on.as_ref().unwrap(),
                Predicate::Single(predicate::LogicalClause::Comparison { lhs, operator, rhs })
                if matches!(lhs, Literal::ColumnReference(column_name) if column_name == "employee_id")
                && *operator == LogicalOperator::Eq
                && matches!(rhs, Literal::ColumnReference(column_name) if column_name == "department_id")
            )
        ));
    }

    #[test]
    fn logical_plan_for_select_with_multiple_joins() {
        use crate::query::parser::ast::Clause;

        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::Join {
                left: Box::new(crate::query::parser::ast::TableSource::Join {
                    left: Box::new(crate::query::parser::ast::TableSource::table("employees")),
                    right: Box::new(crate::query::parser::ast::TableSource::table("departments")),
                    on: Some(crate::query::parser::ast::Expression::Single(
                        Clause::Comparison {
                            lhs: Literal::ColumnReference("employee_id".to_string()),
                            operator: BinaryOperator::Eq,
                            rhs: Literal::ColumnReference("department_id".to_string()),
                        },
                    )),
                }),
                right: Box::new(crate::query::parser::ast::TableSource::table("roles")),
                on: Some(crate::query::parser::ast::Expression::Single(
                    Clause::Comparison {
                        lhs: Literal::ColumnReference("role_id".to_string()),
                        operator: BinaryOperator::Eq,
                        rhs: Literal::ColumnReference("id".to_string()),
                    },
                )),
            },
            projection: Projection::All,
            where_clause: None,
            order_by: None,
            limit: None,
        })
        .unwrap();

        assert!(matches!(
            logical_plan,
            LogicalPlan::Join {
                left: left_outer,
                right: right_outer,
                on: on_outer
            }
            if matches!(
                left_outer.as_ref(),
                LogicalPlan::Join {
                    left: left_inner,
                    right: right_inner,
                    on: on_inner
                }
                if matches!(left_inner.as_ref(), LogicalPlan::Scan { table_name, .. } if table_name == "employees")
                && matches!(right_inner.as_ref(), LogicalPlan::Scan { table_name, .. } if table_name == "departments")
                && matches!(
                    on_inner.as_ref().unwrap(),
                    Predicate::Single(predicate::LogicalClause::Comparison { lhs, operator, rhs })
                    if matches!(lhs, Literal::ColumnReference(column_name) if column_name == "employee_id")
                    && *operator == LogicalOperator::Eq
                    && matches!(rhs, Literal::ColumnReference(column_name) if column_name == "department_id")
                )
            )
            && matches!(right_outer.as_ref(), LogicalPlan::Scan { table_name, .. } if table_name == "roles")
            && matches!(
                on_outer.as_ref().unwrap(),
                Predicate::Single(predicate::LogicalClause::Comparison { lhs, operator, .. })
                if matches!(lhs, Literal::ColumnReference(column_name) if column_name == "role_id")
                && *operator == LogicalOperator::Eq
            )
        ));
    }

    #[test]
    fn logical_plan_for_select_with_alias() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::table_with_alias("employees", "e"),
            projection: Projection::All,
            where_clause: None,
            order_by: None,
            limit: None,
        })
        .unwrap();
        assert!(matches!(
            logical_plan,
            LogicalPlan::Scan { table_name, alias } if table_name == "employees" && alias.as_deref() == Some("e")
        ));
    }

    #[test]
    fn logical_plan_for_select_with_join_and_aliases() {
        use crate::query::parser::ast::Clause;

        let logical_plan = LogicalPlanner::plan(Ast::Select {
            source: crate::query::parser::ast::TableSource::Join {
                left: Box::new(crate::query::parser::ast::TableSource::table_with_alias(
                    "employees",
                    "e",
                )),
                right: Box::new(crate::query::parser::ast::TableSource::table_with_alias(
                    "departments",
                    "d",
                )),
                on: Some(crate::query::parser::ast::Expression::Single(
                    Clause::Comparison {
                        lhs: Literal::ColumnReference("e.id".to_string()),
                        operator: BinaryOperator::Eq,
                        rhs: Literal::ColumnReference("d.employee_id".to_string()),
                    },
                )),
            },
            projection: Projection::All,
            where_clause: None,
            order_by: None,
            limit: None,
        })
        .unwrap();

        assert!(matches!(
            logical_plan,
            LogicalPlan::Join { left, right, .. }
            if matches!(left.as_ref(), LogicalPlan::Scan { table_name, alias } if table_name == "employees" && alias.as_deref() == Some("e"))
            && matches!(right.as_ref(), LogicalPlan::Scan { table_name, alias } if table_name == "departments" && alias.as_deref() == Some("d"))
        ));
    }
}
