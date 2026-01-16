use crate::query::executor::error::ExecutionError;
use crate::query::parser::ast::{BinaryOperator, Literal, WhereClause};

use crate::storage::row_view::RowView;
use crate::types::column_value::ColumnValue;

/// `Predicate` represents a filter condition in a logical plan.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Predicate {
    /// A comparison predicate (e.g., `age > 30`).
    Comparison {
        /// The column name to compare.
        column_name: String,
        /// The logical comparison operator.
        operator: LogicalOperator,
        /// The literal value to compare against.
        literal: Literal,
    },
}

impl From<WhereClause> for Predicate {
    fn from(clause: WhereClause) -> Self {
        match clause {
            WhereClause::Comparison {
                column_name,
                operator,
                literal,
            } => Predicate::Comparison {
                column_name,
                operator: operator.into(),
                literal,
            },
        }
    }
}

impl Predicate {
    /// Evaluates the predicate against a given `RowView`.
    ///
    /// Returns `Ok(true)` if the row satisfies the predicate, `Ok(false)` otherwise.
    /// Returns an `ExecutionError` if the column cannot be found.
    pub(crate) fn matches(&self, row_view: &RowView) -> Result<bool, ExecutionError> {
        match self {
            Predicate::Comparison {
                column_name,
                operator,
                literal,
            } => {
                let column_value = row_view
                    .column_value_by(column_name)
                    .ok_or(ExecutionError::UnknownColumn(column_name.to_string()))?;

                operator.apply(column_value, literal)
            }
        }
    }
}

/// `LogicalOperator` defines the logical comparison operators supported in a predicate.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LogicalOperator {
    /// Equal to `=`.
    Eq,
    /// Not equal to `!=`.
    NotEq,
    /// Greater than `>`.
    Greater,
    /// Greater than or equal to `>=`.
    GreaterEq,
    /// Less than `<`.
    Lesser,
    /// Less than or equal to `<=`.
    LesserEq,
}

impl From<BinaryOperator> for LogicalOperator {
    fn from(operator: BinaryOperator) -> Self {
        match operator {
            BinaryOperator::Eq => LogicalOperator::Eq,
            BinaryOperator::Greater => LogicalOperator::Greater,
            BinaryOperator::GreaterEq => LogicalOperator::GreaterEq,
            BinaryOperator::Lesser => LogicalOperator::Lesser,
            BinaryOperator::LesserEq => LogicalOperator::LesserEq,
            BinaryOperator::NotEq => LogicalOperator::NotEq,
            _ => panic!("unsupported binary operator"),
        }
    }
}

impl LogicalOperator {
    /// Applies the logical operator to compare a column value and a literal.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - If the comparison evaluates to true.
    /// * `Ok(false)` - If the comparison evaluates to false.
    /// * `Err(ExecutionError::TypeMismatchInComparison)` - If the types of the column value and literal do not match.
    pub fn apply(&self, lhs: &ColumnValue, rhs: &Literal) -> Result<bool, ExecutionError> {
        match (lhs, rhs) {
            (ColumnValue::Int(left), Literal::Int(right)) => Ok(match self {
                LogicalOperator::Eq => left == right,
                LogicalOperator::NotEq => left != right,
                LogicalOperator::Greater => left > right,
                LogicalOperator::GreaterEq => left >= right,
                LogicalOperator::Lesser => left < right,
                LogicalOperator::LesserEq => left <= right,
            }),
            (ColumnValue::Text(left), Literal::Text(right)) => Ok(match self {
                LogicalOperator::Eq => left == right,
                LogicalOperator::NotEq => left != right,
                LogicalOperator::Greater => left > right,
                LogicalOperator::GreaterEq => left >= right,
                LogicalOperator::Lesser => left < right,
                LogicalOperator::LesserEq => left <= right,
            }),
            _ => Err(ExecutionError::TypeMismatchInComparison),
        }
    }
}

#[cfg(test)]
impl Predicate {
    pub(crate) fn comparison(
        column_name: &str,
        operator: LogicalOperator,
        literal: Literal,
    ) -> Self {
        Predicate::Comparison {
            column_name: column_name.to_string(),
            operator,
            literal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{BinaryOperator, Literal};

    #[test]
    fn logical_operator_from_eq_operator() {
        assert_eq!(
            LogicalOperator::from(BinaryOperator::Eq),
            LogicalOperator::Eq
        );
    }

    #[test]
    fn logical_operator_from_not_eq_operator() {
        assert_eq!(
            LogicalOperator::from(BinaryOperator::NotEq),
            LogicalOperator::NotEq
        );
    }

    #[test]
    fn logical_operator_from_greater_operator() {
        assert_eq!(
            LogicalOperator::from(BinaryOperator::Greater),
            LogicalOperator::Greater
        );
    }

    #[test]
    fn logical_operator_from_greater_eq_operator() {
        assert_eq!(
            LogicalOperator::from(BinaryOperator::GreaterEq),
            LogicalOperator::GreaterEq
        );
    }

    #[test]
    fn logical_operator_from_lesser_operator() {
        assert_eq!(
            LogicalOperator::from(BinaryOperator::Lesser),
            LogicalOperator::Lesser
        );
    }

    #[test]
    fn logical_operator_from_lesser_eq_operator() {
        assert_eq!(
            LogicalOperator::from(BinaryOperator::LesserEq),
            LogicalOperator::LesserEq
        );
    }

    #[test]
    #[should_panic(expected = "unsupported binary operator")]
    fn attempt_to_create_logical_operator_from_like() {
        let _ = LogicalOperator::from(BinaryOperator::Like);
    }

    #[test]
    fn apply_eq_on_integers_true() {
        assert!(LogicalOperator::Eq
            .apply(&ColumnValue::int(10), &Literal::Int(10))
            .unwrap());
    }

    #[test]
    fn apply_eq_on_integers_false() {
        assert!(!LogicalOperator::Eq
            .apply(&ColumnValue::int(10), &Literal::Int(5))
            .unwrap());
    }

    #[test]
    fn apply_not_eq_on_integers_true() {
        assert!(LogicalOperator::NotEq
            .apply(&ColumnValue::int(10), &Literal::Int(5))
            .unwrap());
    }

    #[test]
    fn apply_not_eq_on_integers_false() {
        assert!(!LogicalOperator::NotEq
            .apply(&ColumnValue::int(10), &Literal::Int(10))
            .unwrap());
    }

    #[test]
    fn apply_greater_on_integers_true() {
        assert!(LogicalOperator::Greater
            .apply(&ColumnValue::int(10), &Literal::Int(5))
            .unwrap());
    }

    #[test]
    fn apply_greater_on_integers_false() {
        assert!(!LogicalOperator::Greater
            .apply(&ColumnValue::int(5), &Literal::Int(10))
            .unwrap());
    }

    #[test]
    fn apply_greater_eq_on_integers_true_greater() {
        assert!(LogicalOperator::GreaterEq
            .apply(&ColumnValue::int(10), &Literal::Int(5))
            .unwrap());
    }

    #[test]
    fn apply_greater_eq_on_integers_true_eq() {
        assert!(LogicalOperator::GreaterEq
            .apply(&ColumnValue::int(10), &Literal::Int(10))
            .unwrap());
    }

    #[test]
    fn apply_greater_eq_on_integers_false() {
        assert!(!LogicalOperator::GreaterEq
            .apply(&ColumnValue::int(5), &Literal::Int(10))
            .unwrap());
    }

    #[test]
    fn apply_lesser_on_integers_true() {
        assert!(LogicalOperator::Lesser
            .apply(&ColumnValue::int(5), &Literal::Int(10))
            .unwrap());
    }

    #[test]
    fn apply_lesser_on_integers_false() {
        assert!(!LogicalOperator::Lesser
            .apply(&ColumnValue::int(10), &Literal::Int(5))
            .unwrap());
    }

    #[test]
    fn apply_lesser_eq_on_integers_true_lesser() {
        assert!(LogicalOperator::LesserEq
            .apply(&ColumnValue::int(5), &Literal::Int(10))
            .unwrap());
    }

    #[test]
    fn apply_lesser_eq_on_integers_true_eq() {
        assert!(LogicalOperator::LesserEq
            .apply(&ColumnValue::int(10), &Literal::Int(10))
            .unwrap());
    }

    #[test]
    fn apply_lesser_eq_on_integers_false() {
        assert!(!LogicalOperator::LesserEq
            .apply(&ColumnValue::int(10), &Literal::Int(5))
            .unwrap());
    }

    #[test]
    fn apply_eq_on_strings_true() {
        assert!(LogicalOperator::Eq
            .apply(
                &ColumnValue::text("relop"),
                &Literal::Text("relop".to_string())
            )
            .unwrap());
    }

    #[test]
    fn apply_eq_on_strings_false() {
        assert!(!LogicalOperator::Eq
            .apply(
                &ColumnValue::text("relop"),
                &Literal::Text("rust".to_string())
            )
            .unwrap());
    }

    #[test]
    fn apply_not_eq_on_strings_true() {
        assert!(LogicalOperator::NotEq
            .apply(
                &ColumnValue::text("relop"),
                &Literal::Text("rust".to_string())
            )
            .unwrap());
    }

    #[test]
    fn apply_not_eq_on_strings_false() {
        assert!(!LogicalOperator::NotEq
            .apply(
                &ColumnValue::text("relop"),
                &Literal::Text("relop".to_string())
            )
            .unwrap());
    }

    #[test]
    fn apply_greater_on_strings_true() {
        assert!(LogicalOperator::Greater
            .apply(
                &ColumnValue::text("rust"),
                &Literal::Text("relop".to_string())
            )
            .unwrap());
    }

    #[test]
    fn apply_greater_on_strings_false() {
        assert!(!LogicalOperator::Greater
            .apply(
                &ColumnValue::text("relop"),
                &Literal::Text("rust".to_string())
            )
            .unwrap());
    }

    #[test]
    fn apply_greater_eq_on_strings_true_greater() {
        assert!(LogicalOperator::GreaterEq
            .apply(
                &ColumnValue::text("rust"),
                &Literal::Text("relop".to_string())
            )
            .unwrap());
    }

    #[test]
    fn apply_greater_eq_on_strings_true_eq() {
        assert!(LogicalOperator::GreaterEq
            .apply(
                &ColumnValue::text("relop"),
                &Literal::Text("relop".to_string())
            )
            .unwrap());
    }

    #[test]
    fn apply_greater_eq_on_strings_false() {
        assert!(!LogicalOperator::GreaterEq
            .apply(
                &ColumnValue::text("relop"),
                &Literal::Text("rust".to_string())
            )
            .unwrap());
    }

    #[test]
    fn apply_lesser_on_strings_true() {
        assert!(LogicalOperator::Lesser
            .apply(
                &ColumnValue::text("relop"),
                &Literal::Text("rust".to_string())
            )
            .unwrap());
    }

    #[test]
    fn apply_lesser_on_strings_false() {
        assert!(!LogicalOperator::Lesser
            .apply(
                &ColumnValue::text("rust"),
                &Literal::Text("relop".to_string())
            )
            .unwrap());
    }

    #[test]
    fn apply_lesser_eq_on_strings_true_lesser() {
        assert!(LogicalOperator::LesserEq
            .apply(
                &ColumnValue::text("relop"),
                &Literal::Text("rust".to_string())
            )
            .unwrap());
    }

    #[test]
    fn apply_lesser_eq_on_strings_true_eq() {
        assert!(LogicalOperator::LesserEq
            .apply(
                &ColumnValue::text("relop"),
                &Literal::Text("relop".to_string())
            )
            .unwrap());
    }

    #[test]
    fn apply_lesser_eq_on_strings_false() {
        assert!(!LogicalOperator::LesserEq
            .apply(
                &ColumnValue::text("rust"),
                &Literal::Text("relop".to_string())
            )
            .unwrap());
    }

    #[test]
    fn apply_logical_operator_type_mismatch() {
        let operator = LogicalOperator::Eq;
        assert!(matches!(
            operator.apply(&ColumnValue::int(10), &Literal::Text("10".to_string())),
            Err(ExecutionError::TypeMismatchInComparison)
        ));
    }
}

#[cfg(test)]
mod predicate_tests {
    use super::*;
    use crate::query::parser::ast::Literal;
    use crate::row;
    use crate::storage::row_view::RowView;
    use crate::test_utils::create_schema;
    use crate::types::column_type::ColumnType;

    #[test]
    fn predicate_from_where_clause() {
        let clause = WhereClause::Comparison {
            column_name: "age".to_string(),
            operator: BinaryOperator::Greater,
            literal: Literal::Int(30),
        };

        let predicate = Predicate::from(clause);
        assert_eq!(
            predicate,
            Predicate::comparison("age", LogicalOperator::Greater, Literal::Int(30))
        );
    }

    #[test]
    fn matches_for_the_row() {
        let schema = create_schema(&[("age", ColumnType::Int)]);
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::comparison("age", LogicalOperator::Eq, Literal::Int(30));
        assert!(predicate.matches(&row_view).unwrap());
    }

    #[test]
    fn does_not_match_for_the_row() {
        let schema = create_schema(&[("age", ColumnType::Int)]);
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::comparison("age", LogicalOperator::Greater, Literal::Int(30));
        assert!(!predicate.matches(&row_view).unwrap());
    }

    #[test]
    fn attempt_to_match_predicate_when_the_column_is_not_present_in_the_row() {
        let schema = create_schema(&[("age", ColumnType::Int)]);
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate =
            Predicate::comparison("height", LogicalOperator::Greater, Literal::Int(170));
        let result = predicate.matches(&row_view);
        assert!(matches!(
            result,
            Err(ExecutionError::UnknownColumn(name)) if name == "height"
        ))
    }

    #[test]
    fn attempt_to_match_predicate_when_there_is_a_column_type_mismatch() {
        let schema = create_schema(&[("age", ColumnType::Int)]);
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate =
            Predicate::comparison("age", LogicalOperator::Eq, Literal::Text("30".to_string()));
        assert!(matches!(
            predicate.matches(&row_view),
            Err(ExecutionError::TypeMismatchInComparison)
        ));
    }
}
