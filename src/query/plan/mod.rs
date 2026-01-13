use crate::query::parser::ast::Ast;
use crate::query::parser::projection::Projection;

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
    ScanTable {
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
    /// Plan to limit results from a base plan.
    Limit {
        /// The source plan.
        base_plan: Box<LogicalPlan>,
        /// The limit value.
        count: usize,
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
    pub(crate) fn plan(ast: Ast) -> LogicalPlan {
        match ast {
            Ast::ShowTables => LogicalPlan::ShowTables,
            Ast::DescribeTable { table_name } => LogicalPlan::DescribeTable { table_name },
            Ast::Select {
                table_name,
                projection,
                limit,
            } => {
                let base_plan = Self::plan_for_projection(projection, table_name);
                if let Some(value) = limit {
                    return LogicalPlan::Limit {
                        base_plan: base_plan.boxed(),
                        count: value,
                    };
                }
                base_plan
            }
        }
    }

    fn plan_for_projection(projection: Projection, table_name: String) -> LogicalPlan {
        match projection {
            Projection::All => LogicalPlan::ScanTable { table_name },
            Projection::Columns(columns) => LogicalPlan::Projection {
                base_plan: LogicalPlan::ScanTable { table_name }.boxed(),
                columns,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::projection::Projection;

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
            limit: None,
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::ScanTable { table_name } if table_name == "employees"
        ));
    }

    #[test]
    fn logical_plan_for_select_with_projection() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::Columns(vec![String::from("id")]),
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
            limit: None,
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Projection {base_plan, columns: _ } if matches!(base_plan.as_ref(), LogicalPlan::ScanTable { table_name } if table_name == "employees")
        ));
    }

    #[test]
    fn logical_plan_for_select_all_with_limit_base_plan() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::All,
            limit: Some(10),
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Limit { base_plan, count: _ } if matches!(base_plan.as_ref(), LogicalPlan::ScanTable { table_name } if table_name == "employees")
        ));
    }

    #[test]
    fn logical_plan_for_select_all_with_limit_count() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::All,
            limit: Some(10),
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Limit { base_plan: _, count } if count == 10
        ));
    }

    ///////

    #[test]
    fn logical_plan_for_select_with_projection_and_limit() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::Columns(vec![String::from("id")]),
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
            limit: Some(10),
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Limit {base_plan, count: _ }
                if matches!(base_plan.as_ref(), LogicalPlan::Projection { base_plan: _, columns } if columns.iter().eq(&[String::from("id")]) )
        ));
    }

    #[test]
    fn logical_plan_for_select_with_projection_and_limit_validating_the_base_plan() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::Columns(vec![String::from("id")]),
            limit: Some(10),
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Limit {base_plan, count: _ }
                if matches!(base_plan.as_ref(), LogicalPlan::Projection { base_plan, columns: _ }
                    if matches!(base_plan.as_ref(), LogicalPlan::ScanTable { table_name } if table_name == "employees") )
        ));
    }
}
