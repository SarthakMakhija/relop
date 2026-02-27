pub(crate) mod predicate_pushdown;

use crate::query::optimizer::predicate_pushdown::PredicatePushdownRule;
use crate::query::plan::LogicalPlan;

/// A trait for rules that optimize a `LogicalPlan`.
pub(crate) trait OptimizerRule {
    /// Applies the optimization rule to the given plan.
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan;
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