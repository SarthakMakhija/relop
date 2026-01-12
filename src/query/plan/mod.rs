use crate::query::parser::ast::Ast;
use crate::query::parser::projection::Projection;

#[derive(Eq, PartialEq)]
pub(crate) enum LogicalPlan {
    ShowTables,
    DescribeTable {
        table_name: String,
    },
    ScanTable {
        table_name: String,
    },
    Projection {
        base: Box<LogicalPlan>,
        columns: Vec<String>,
    },
}

pub(crate) struct LogicalPlanner;

impl LogicalPlanner {
    pub(crate) fn plan(ast: Ast) -> LogicalPlan {
        match ast {
            Ast::ShowTables => LogicalPlan::ShowTables,
            Ast::DescribeTable { table_name } => LogicalPlan::DescribeTable { table_name },
            Ast::Select {
                table_name,
                projection,
            } => match projection {
                Projection::All => LogicalPlan::ScanTable { table_name },
                Projection::Columns(columns) => LogicalPlan::Projection {
                    base: Box::new(LogicalPlan::ScanTable { table_name }),
                    columns,
                },
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
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Projection {base: _, columns } if columns.iter().eq(&["id"])
        ));
    }

    #[test]
    fn logical_plan_for_select_with_projection_validating_the_base_plan() {
        let logical_plan = LogicalPlanner::plan(Ast::Select {
            table_name: "employees".to_string(),
            projection: Projection::Columns(vec![String::from("id")]),
        });
        assert!(matches!(
            logical_plan,
            LogicalPlan::Projection {base, columns: _ } if matches!(base.as_ref(), LogicalPlan::ScanTable { table_name } if table_name == "employees")
        ));
    }
}
