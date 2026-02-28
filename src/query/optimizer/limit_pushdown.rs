use crate::query::optimizer::OptimizerRule;
use crate::query::plan::LogicalPlan;

/// An optimizer rule that pushes a `Limit` operation down into a `Sort` operation
/// if the `Limit` immediately encloses the `Sort`. This allows the execution engine
/// to perform an efficiently bounded Top-K sort instead of a full sort.
pub(crate) struct LimitPushdownRule;

impl OptimizerRule for LimitPushdownRule {
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        let plan = plan.map_children(|child| self.optimize(child));

        match plan {
            LogicalPlan::Limit { count, base_plan } => {
                let optimized_base_plan = self.optimize(*base_plan);
                if let LogicalPlan::Sort {
                    base_plan: sort_base,
                    ordering_keys,
                    limit: _,
                } = optimized_base_plan
                {
                    // Merge Limit into Sort
                    LogicalPlan::Sort {
                        base_plan: sort_base,
                        ordering_keys,
                        limit: Some(count),
                    }
                } else {
                    // Not a Sort node, just rebuild the Limit
                    LogicalPlan::Limit {
                        count,
                        base_plan: Box::new(optimized_base_plan),
                    }
                }
            }
            // For all other nodes, return.
            _ => plan,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asc;
    use crate::query::plan::LogicalPlan;

    #[test]
    fn push_down_limit_to_sort() {
        let table_scan = LogicalPlan::Scan {
            table_name: "employees".to_string(),
            alias: None,
            filter: None,
            schema: std::sync::Arc::new(crate::schema::Schema::new()),
        };

        let sort_plan = table_scan.order_by(vec![asc!("id")]);
        let limit = LogicalPlan::Limit {
            count: 5,
            base_plan: Box::new(sort_plan),
        };

        let rule = LimitPushdownRule;
        let optimized = rule.optimize(limit);

        assert!(
            matches!(optimized, LogicalPlan::Sort { limit: Some(5), .. }),
            "Expected Sort node with limit Some(5), got {:?}",
            optimized
        );
    }
}
