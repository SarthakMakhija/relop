use crate::query::optimizer::OptimizerRule;
use crate::query::plan::predicate::Predicate;
use crate::query::plan::LogicalPlan;

/// A rule that pushes `Filter` nodes down into `Scan` nodes.
pub(crate) struct PredicatePushdownRule;

impl OptimizerRule for PredicatePushdownRule {
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        let plan = plan.map_children(|logical_plan| self.optimize(logical_plan));

        match plan {
            LogicalPlan::Filter {
                base_plan,
                predicate,
            } => match *base_plan {
                LogicalPlan::Scan {
                    table_name,
                    alias,
                    filter: existing,
                    schema,
                } => {
                    let combined_filter = match existing {
                        Some(existing_filter) => Predicate::And(vec![existing_filter, predicate]),
                        None => predicate,
                    };
                    LogicalPlan::Scan {
                        table_name,
                        alias,
                        filter: Some(combined_filter),
                        schema,
                    }
                }
                _ => LogicalPlan::Filter {
                    base_plan,
                    predicate,
                },
            },
            _ => plan,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::Literal;
    use crate::query::plan::predicate::{LogicalOperator, Predicate};

    #[test]
    fn push_down_filter_to_scan() {
        let plan = LogicalPlan::scan("employees").filter(Predicate::comparison(
            Literal::ColumnReference("id".to_string()),
            LogicalOperator::Eq,
            Literal::Int(1),
        ));

        let optimizer = PredicatePushdownRule;
        let optimized_plan = optimizer.optimize(plan);

        let expected_plan = LogicalPlan::Scan {
            table_name: "employees".to_string(),
            alias: None,
            filter: Some(Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            )),
            schema: std::sync::Arc::new(crate::schema::Schema::new()),
        };

        assert_eq!(optimized_plan, expected_plan);
    }

    #[test]
    fn push_down_filter_through_projection() {
        let plan = LogicalPlan::scan("employees")
            .filter(Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            ))
            .project(vec!["id"]);

        let optimizer = PredicatePushdownRule;
        let optimized_plan = optimizer.optimize(plan);

        let expected_plan = LogicalPlan::Projection {
            base_plan: Box::new(LogicalPlan::Scan {
                table_name: "employees".to_string(),
                alias: None,
                filter: Some(Predicate::comparison(
                    Literal::ColumnReference("id".to_string()),
                    LogicalOperator::Eq,
                    Literal::Int(1),
                )),
                schema: std::sync::Arc::new(crate::schema::Schema::new()),
            }),
            columns: vec!["id".to_string()],
        };

        assert_eq!(optimized_plan, expected_plan);
    }

    #[test]
    fn push_down_multiple_filters_to_scan() {
        let plan = LogicalPlan::scan("employees")
            .filter(Predicate::comparison(
                Literal::ColumnReference("age".to_string()),
                LogicalOperator::Greater,
                Literal::Int(30),
            ))
            .filter(Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            ));

        let optimizer = PredicatePushdownRule;
        let optimized_plan = optimizer.optimize(plan);

        let expected_plan = LogicalPlan::Scan {
            table_name: "employees".to_string(),
            alias: None,
            filter: Some(Predicate::And(vec![
                Predicate::comparison(
                    Literal::ColumnReference("age".to_string()),
                    LogicalOperator::Greater,
                    Literal::Int(30),
                ),
                Predicate::comparison(
                    Literal::ColumnReference("id".to_string()),
                    LogicalOperator::Eq,
                    Literal::Int(1),
                ),
            ])),
            schema: std::sync::Arc::new(crate::schema::Schema::new()),
        };

        assert_eq!(optimized_plan, expected_plan);
    }
}
