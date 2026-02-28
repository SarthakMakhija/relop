use crate::query::optimizer::OptimizerRule;
use crate::query::plan::predicate::Predicate;
use crate::query::plan::LogicalPlan;

/// A rule that pushes `Filter` nodes down into `Scan` nodes.
pub(crate) struct PredicatePushdownRule;

impl OptimizerRule for PredicatePushdownRule {
    /// Optimizes the provided logical plan by pushing `Filter` nodes as close to the data source (`Scan` nodes) as possible.
    ///
    /// This optimization is performed bottom-up. It traverses to the leaves of the `LogicalPlan` tree first, and then
    /// applies predicate pushdown rules upon returning.
    ///
    /// The most complex scenario handled here is pushing predicates through a `Join` node.
    /// When a `Filter` wraps a `Join`, the optimizer splits the filter's predicate by `AND` and checks which
    /// child node (`left` or `right`) each conjunct belongs to based on the schema.
    ///
    /// # Pushing Filters Through Joins
    ///
    /// Given a plan where a `Filter` wraps a `Join`:
    /// ```text
    ///       [Filter (e.id > 10 AND d.id = 5 AND e.id = d.id)]
    ///                        |
    ///                     [Join]
    ///                    /      \
    ///            [Scan (e)]    [Scan (d)]
    /// ```
    ///
    /// The optimizer will separate the compound predicate and push the valid parts down to the correct children,
    /// creating a new `Filter` node around the remaining, un-pushable conditions directly above the `Join`:
    /// ```text
    ///                    [Filter (e.id = d.id)]
    ///                             |
    ///                          [Join]
    ///                         /      \
    ///      [Filter (e.id > 10)]      [Filter (d.id = 5)]
    ///               |                         |
    ///           [Scan (e)]                [Scan (d)]
    /// ```
    ///
    /// The recursively optimized children (the new `Filter` nodes above `Scan`) will then have their own `Filter`
    /// merged directly into the `Scan` nodes in subsequent recursive passes or base matches.
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        let plan = plan.map_children(|logical_plan| self.optimize(logical_plan));

        match plan {
            LogicalPlan::Filter {
                base_plan,
                predicate,
            } => match *base_plan {
                LogicalPlan::Join { left, right, on } => {
                    let (pushed_left, pushed_right, remaining) =
                        try_push_down(predicate, &left, &right);

                    let new_left = if let Some(left_predicate) = pushed_left {
                        self.optimize(LogicalPlan::Filter {
                            base_plan: left,
                            predicate: left_predicate,
                        })
                    } else {
                        *left
                    };

                    let new_right = if let Some(right_predicate) = pushed_right {
                        self.optimize(LogicalPlan::Filter {
                            base_plan: right,
                            predicate: right_predicate,
                        })
                    } else {
                        *right
                    };

                    let optimized_join = LogicalPlan::Join {
                        left: Box::new(new_left),
                        right: Box::new(new_right),
                        on,
                    };

                    if let Some(remaining_predicate) = remaining {
                        LogicalPlan::Filter {
                            base_plan: Box::new(optimized_join),
                            predicate: remaining_predicate,
                        }
                    } else {
                        optimized_join
                    }
                }
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

/// Attempts to push parts of an AND-separated predicate down to the left and right children.
/// Returns a tuple of `(Option<Left Predicate>, Option<Right Predicate>, Option<Unpushable Predicate>)`.
fn try_push_down(
    predicate: Predicate,
    left_plan: &LogicalPlan,
    right_plan: &LogicalPlan,
) -> (Option<Predicate>, Option<Predicate>, Option<Predicate>) {
    let left_schema_optional = left_plan.schema();
    let right_schema_optional = right_plan.schema();

    // If schemas aren't available, we can't safely push down.
    if left_schema_optional.is_none() || right_schema_optional.is_none() {
        return (None, None, Some(predicate));
    }

    //SAFETY: already validated that neither of left_schema or right_schema is None.
    let left_schema = left_schema_optional.unwrap();
    let right_schema = right_schema_optional.unwrap();

    let predicates = predicate.split_by_and();

    let mut left_predicates = Vec::new();
    let mut right_predicates = Vec::new();
    let mut unpushable_predicates = Vec::new();

    for pred in predicates {
        let belongs_to_left = pred.belongs_to(&left_schema);
        let belongs_to_right = pred.belongs_to(&right_schema);
        if belongs_to_left {
            left_predicates.push(pred);
        } else if belongs_to_right {
            right_predicates.push(pred);
        } else {
            unpushable_predicates.push(pred);
        }
    }

    (
        combine_predicates(left_predicates),
        combine_predicates(right_predicates),
        combine_predicates(unpushable_predicates),
    )
}

/// Combines a list of predicates into a single `Predicate::And`, or returns `None` if the list is empty.
fn combine_predicates(mut predicates: Vec<Predicate>) -> Option<Predicate> {
    match predicates.len() {
        0 => None,
        1 => Some(predicates.remove(0)),
        _ => Some(Predicate::And(predicates)),
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

    #[test]
    fn push_down_filter_through_join_left_and_right() {
        use crate::schema;
        use crate::types::column_type::ColumnType;
        use std::sync::Arc;

        let employees_plan = LogicalPlan::Scan {
            table_name: "employees".to_string(),
            alias: Some("e".to_string()),
            filter: None,
            schema: Arc::new(schema!["id" => ColumnType::Int].unwrap()),
        };

        let departments_plan = LogicalPlan::Scan {
            table_name: "departments".to_string(),
            alias: Some("d".to_string()),
            filter: None,
            schema: Arc::new(schema!["id" => ColumnType::Int].unwrap()),
        };

        let plan = employees_plan
            .join(departments_plan, None)
            .filter(Predicate::And(vec![
                Predicate::comparison(
                    Literal::ColumnReference("e.id".to_string()),
                    LogicalOperator::Greater,
                    Literal::Int(10),
                ),
                Predicate::comparison(
                    Literal::ColumnReference("d.id".to_string()),
                    LogicalOperator::Eq,
                    Literal::Int(5),
                ),
            ]));

        let optimizer = PredicatePushdownRule;
        let optimized_plan = optimizer.optimize(plan);

        let expected_plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                table_name: "employees".to_string(),
                alias: Some("e".to_string()),
                filter: Some(Predicate::comparison(
                    Literal::ColumnReference("e.id".to_string()),
                    LogicalOperator::Greater,
                    Literal::Int(10),
                )),
                schema: Arc::new(schema!["id" => ColumnType::Int].unwrap()),
            }),
            right: Box::new(LogicalPlan::Scan {
                table_name: "departments".to_string(),
                alias: Some("d".to_string()),
                filter: Some(Predicate::comparison(
                    Literal::ColumnReference("d.id".to_string()),
                    LogicalOperator::Eq,
                    Literal::Int(5),
                )),
                schema: Arc::new(schema!["id" => ColumnType::Int].unwrap()),
            }),
            on: None,
        };

        assert_eq!(optimized_plan, expected_plan);
    }

    #[test]
    fn push_down_filter_through_join_unpushable() {
        use crate::schema;
        use crate::types::column_type::ColumnType;
        use std::sync::Arc;

        let plan = LogicalPlan::Scan {
            table_name: "employees".to_string(),
            alias: Some("e".to_string()),
            filter: None,
            schema: Arc::new(schema!["id" => ColumnType::Int].unwrap()),
        }
        .join(
            LogicalPlan::Scan {
                table_name: "departments".to_string(),
                alias: Some("d".to_string()),
                filter: None,
                schema: Arc::new(schema!["id" => ColumnType::Int].unwrap()),
            },
            None,
        )
        .filter(Predicate::And(vec![
            Predicate::comparison(
                Literal::ColumnReference("e.id".to_string()),
                LogicalOperator::Greater,
                Literal::Int(10),
            ),
            Predicate::comparison(
                Literal::ColumnReference("e.id".to_string()),
                LogicalOperator::Eq,
                Literal::ColumnReference("d.id".to_string()),
            ),
        ]));

        let optimizer = PredicatePushdownRule;
        let optimized_plan = optimizer.optimize(plan);

        // The e.id > 10 should be pushed left, but the e.id = d.id will remain on top
        let expected_plan = LogicalPlan::Filter {
            base_plan: Box::new(LogicalPlan::Join {
                left: Box::new(LogicalPlan::Scan {
                    table_name: "employees".to_string(),
                    alias: Some("e".to_string()),
                    filter: Some(Predicate::comparison(
                        Literal::ColumnReference("e.id".to_string()),
                        LogicalOperator::Greater,
                        Literal::Int(10),
                    )),
                    schema: Arc::new(schema!["id" => ColumnType::Int].unwrap()),
                }),
                right: Box::new(LogicalPlan::Scan {
                    table_name: "departments".to_string(),
                    alias: Some("d".to_string()),
                    filter: None,
                    schema: Arc::new(schema!["id" => ColumnType::Int].unwrap()),
                }),
                on: None,
            }),
            predicate: Predicate::comparison(
                Literal::ColumnReference("e.id".to_string()),
                LogicalOperator::Eq,
                Literal::ColumnReference("d.id".to_string()),
            ),
        };

        assert_eq!(optimized_plan, expected_plan);
    }

    #[test]
    fn push_down_filter_through_three_table_join() {
        use crate::schema;
        use crate::types::column_type::ColumnType;
        use std::sync::Arc;

        let employees_plan = LogicalPlan::Scan {
            table_name: "employees".to_string(),
            alias: Some("e".to_string()),
            filter: None,
            schema: Arc::new(schema!["id" => ColumnType::Int].unwrap()),
        };

        let departments_plan = LogicalPlan::Scan {
            table_name: "departments".to_string(),
            alias: Some("d".to_string()),
            filter: None,
            schema: Arc::new(schema!["id" => ColumnType::Int].unwrap()),
        };

        let locations_plan = LogicalPlan::Scan {
            table_name: "locations".to_string(),
            alias: Some("l".to_string()),
            filter: None,
            schema: Arc::new(schema!["id" => ColumnType::Int].unwrap()),
        };

        let plan = employees_plan
            .join(departments_plan, None)
            .join(locations_plan, None)
            .filter(Predicate::And(vec![
                Predicate::comparison(
                    Literal::ColumnReference("e.id".to_string()),
                    LogicalOperator::Greater,
                    Literal::Int(10),
                ),
                Predicate::comparison(
                    Literal::ColumnReference("l.id".to_string()),
                    LogicalOperator::Eq,
                    Literal::Int(5),
                ),
                Predicate::comparison(
                    Literal::ColumnReference("e.id".to_string()),
                    LogicalOperator::Eq,
                    Literal::ColumnReference("d.id".to_string()),
                ),
            ]));

        let optimizer = PredicatePushdownRule;
        let optimized_plan = optimizer.optimize(plan);

        // e.id > 10 should be pushed down to `employees` scan (which is inside the first join).
        // l.id = 5 should be pushed down to `locations` scan.
        // e.id = d.id should be pushed down to the first join (employees join departments).
        let expected_plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Filter {
                base_plan: Box::new(LogicalPlan::Join {
                    left: Box::new(LogicalPlan::Scan {
                        table_name: "employees".to_string(),
                        alias: Some("e".to_string()),
                        filter: Some(Predicate::comparison(
                            Literal::ColumnReference("e.id".to_string()),
                            LogicalOperator::Greater,
                            Literal::Int(10),
                        )),
                        schema: Arc::new(schema!["id" => ColumnType::Int].unwrap()),
                    }),
                    right: Box::new(LogicalPlan::Scan {
                        table_name: "departments".to_string(),
                        alias: Some("d".to_string()),
                        filter: None,
                        schema: Arc::new(schema!["id" => ColumnType::Int].unwrap()),
                    }),
                    on: None,
                }),
                predicate: Predicate::comparison(
                    Literal::ColumnReference("e.id".to_string()),
                    LogicalOperator::Eq,
                    Literal::ColumnReference("d.id".to_string()),
                ),
            }),
            right: Box::new(LogicalPlan::Scan {
                table_name: "locations".to_string(),
                alias: Some("l".to_string()),
                filter: Some(Predicate::comparison(
                    Literal::ColumnReference("l.id".to_string()),
                    LogicalOperator::Eq,
                    Literal::Int(5),
                )),
                schema: Arc::new(schema!["id" => ColumnType::Int].unwrap()),
            }),
            on: None,
        };

        assert_eq!(optimized_plan, expected_plan);
    }
}
