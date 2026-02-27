use crate::query::plan::LogicalPlan;

/// A trait for rules that optimize a `LogicalPlan`.
pub(crate) trait OptimizerRule {
    /// Applies the optimization rule to the given plan.
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan;
}

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
                    filter: _some_filter,
                } => LogicalPlan::Scan {
                    table_name,
                    alias,
                    filter: Some(predicate),
                },
                _ => LogicalPlan::Filter {
                    base_plan,
                    predicate,
                },
            },
            _ => plan,
        }
    }
}

/// The query optimizer that applies a set of rules to a `LogicalPlan`.
pub(crate) struct Optimizer {
    rules: Vec<Box<dyn OptimizerRule>>,
}

impl Optimizer {
    /// Creates a new `Optimizer` with the default set of rules.
    pub(crate) fn new() -> Self {
        Self {
            rules: vec![Box::new(PredicatePushdownRule)],
        }
    }

    /// Optimized the given plan by applying all rules.
    pub(crate) fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        self.rules.iter().fold(plan, |acc, rule| rule.optimize(acc))
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

        let optimizer = Optimizer::new();
        let optimized_plan = optimizer.optimize(plan);

        let expected_plan = LogicalPlan::Scan {
            table_name: "employees".to_string(),
            alias: None,
            filter: Some(Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            )),
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

        let optimizer = Optimizer::new();
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
            }),
            columns: vec!["id".to_string()],
        };

        assert_eq!(optimized_plan, expected_plan);
    }
}
