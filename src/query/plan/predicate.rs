use crate::query::executor::error::ExecutionError;
use crate::query::parser::ast::{BinaryOperator, Condition, Literal, WhereClause};

use crate::storage::row_view::RowView;
use crate::types::column_value::ColumnValue;

/// `Predicate` represents a filter condition in a logical plan.
#[derive(Debug)]
pub(crate) enum Predicate {
    Single(LogicalCondition),
    And(Vec<LogicalCondition>),
}

#[derive(Debug)]
pub(crate) enum LogicalCondition {
    /// A comparison predicate (e.g., `age > 30`).
    Comparison {
        /// The column name to compare.
        column_name: String,
        /// The logical comparison operator.
        operator: LogicalOperator,
        /// The literal value to compare against.
        literal: Literal,
    },
    Like {
        /// The column name to match against.
        column_name: String,
        /// The compiled regular expression for the pattern.
        regex: regex::Regex,
    },
}

impl LogicalCondition {
    /// Creates a new `LogicalCondition::Comparison` variant.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The name of the column to compare.
    /// * `operator` - The logical operator to use for comparison.
    /// * `literal` - The literal value to compare against.
    pub(crate) fn comparison(
        column_name: &str,
        operator: LogicalOperator,
        literal: Literal,
    ) -> Self {
        LogicalCondition::Comparison {
            column_name: column_name.to_string(),
            operator,
            literal,
        }
    }

    /// Creates a new `LogicalCondition::Like` variant.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The name of the column to match against.
    /// * `regex` - The compiled regular expression pattern.
    pub(crate) fn like(column_name: &str, regex: regex::Regex) -> Self {
        LogicalCondition::Like {
            column_name: column_name.to_string(),
            regex,
        }
    }

    /// Evaluates the condition against a given `RowView`.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - If the row satisfies the condition.
    /// * `Ok(false)` - If the row does not satisfy the condition.
    /// * `Err(ExecutionError::UnknownColumn)` - If the column is not found in the row.
    /// * `Err(ExecutionError::TypeMismatchInComparison)` - If the types do not match.
    pub(crate) fn matches(&self, row_view: &RowView) -> Result<bool, ExecutionError> {
        match self {
            LogicalCondition::Comparison {
                column_name,
                operator,
                literal,
            } => {
                let column_value = row_view
                    .column_value_by(column_name)
                    .ok_or(ExecutionError::UnknownColumn(column_name.to_string()))?;

                operator.apply(column_value, literal)
            }
            LogicalCondition::Like { column_name, regex } => {
                let column_value = row_view
                    .column_value_by(column_name)
                    .ok_or(ExecutionError::UnknownColumn(column_name.to_string()))?;

                match column_value {
                    ColumnValue::Text(value) => Ok(regex.is_match(value)),
                    _ => Err(ExecutionError::TypeMismatchInComparison),
                }
            }
        }
    }
}

use crate::query::plan::error::PlanningError;

impl TryFrom<WhereClause> for Predicate {
    type Error = PlanningError;

    /// Converts a `WhereClause` into a `Predicate`.
    ///
    /// # Returns
    ///
    /// * `Ok(Predicate)` - If the conversion is successful.
    /// * `Err(PlanningError)` - If the conversion fails (e.g., due to an invalid regex).
    fn try_from(clause: WhereClause) -> Result<Self, Self::Error> {
        match clause {
            WhereClause::Single(condition) => {
                Ok(Predicate::Single(LogicalCondition::try_from(condition)?))
            }
            WhereClause::And(conditions) => {
                let logical_conditions = conditions
                    .into_iter()
                    .map(LogicalCondition::try_from)
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Predicate::And(logical_conditions))
            }
        }
    }
}

impl TryFrom<Condition> for LogicalCondition {
    type Error = PlanningError;

    /// Converts a `Condition` into a `LogicalCondition`.
    ///
    /// # Returns
    ///
    /// * `Ok(LogicalCondition)` - If the conversion is successful.
    /// * `Err(PlanningError)` - If the conversion fails (e.g., due to an invalid regex).
    fn try_from(condition: Condition) -> Result<Self, Self::Error> {
        match condition {
            Condition::Comparison {
                column_name,
                operator,
                literal,
            } => Ok(LogicalCondition::Comparison {
                column_name,
                operator: operator.into(),
                literal,
            }),
            Condition::Like {
                column_name,
                literal,
            } => {
                let regex_pattern = match literal {
                    Literal::Text(pattern) => pattern,
                    _ => {
                        return Err(PlanningError::InvalidRegex(
                            "Like clause requires a string literal".to_string(),
                        ))
                    }
                };
                let regex = regex::Regex::new(&regex_pattern)?;
                Ok(LogicalCondition::Like { column_name, regex })
            }
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
            Predicate::Single(condition) => condition.matches(row_view),
            Predicate::And(conditions) => {
                for condition in conditions {
                    if !condition.matches(row_view)? {
                        return Ok(false);
                    }
                }
                Ok(true)
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

impl Predicate {
    /// Creates a new `Comparison` predicate.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The name of the column to compare.
    /// * `operator` - The logical operator to use for comparison.
    /// * `literal` - The literal value to compare against.
    pub(crate) fn comparison(
        column_name: &str,
        operator: LogicalOperator,
        literal: Literal,
    ) -> Self {
        Predicate::Single(LogicalCondition::comparison(column_name, operator, literal))
    }

    /// Creates a new `Like` predicate.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The name of the column to match against.
    /// * `pattern` - The compiled regular expression pattern.
    pub(crate) fn like(column_name: &str, pattern: regex::Regex) -> Self {
        Predicate::Single(LogicalCondition::like(column_name, pattern))
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
    use crate::schema;
    use crate::storage::row_view::RowView;
    use crate::types::column_type::ColumnType;

    #[test]
    fn create_comparison_predicate() {
        let predicate = Predicate::comparison("age", LogicalOperator::Greater, Literal::Int(18));
        assert!(matches!(
            predicate,
            Predicate::Single(LogicalCondition::Comparison {
                column_name,
                operator: LogicalOperator::Greater,
                literal: Literal::Int(18),
            }) if column_name == "age"
        ));
    }

    #[test]
    fn create_like_predicate() {
        let regex = regex::Regex::new("^J").unwrap();
        let predicate = Predicate::like("name", regex);
        assert!(matches!(
            predicate,
            Predicate::Single(LogicalCondition::Like {
                column_name,
                regex: _,
            }) if column_name == "name"
        ));
    }

    #[test]
    fn predicate_from_where_clause() {
        let clause = WhereClause::comparison("age", BinaryOperator::Greater, Literal::Int(30));

        let predicate = Predicate::try_from(clause).unwrap();
        assert!(matches!(
            predicate,
            Predicate::Single(LogicalCondition::Comparison {column_name, operator, literal})
                if column_name == "age" && operator == LogicalOperator::Greater && literal == Literal::Int(30)
        ));
    }

    #[test]
    fn predicate_from_where_clause_with_invalid_regex_like() {
        let clause = WhereClause::like("name", Literal::Text("[".to_string()));

        let result = Predicate::try_from(clause);
        assert!(matches!(result, Err(PlanningError::InvalidRegex(_))));
    }

    #[test]
    fn predicate_from_where_clause_with_valid_regex_like() {
        let clause = WhereClause::like("name", Literal::Text("J%".to_string()));

        let result = Predicate::try_from(clause);
        assert!(matches!(
            result,
            Ok(Predicate::Single(LogicalCondition::Like { column_name, regex: _ })) if column_name == "name"
        ));
    }

    #[test]
    fn matches_for_the_row() {
        let schema = schema!["age" => ColumnType::Int].unwrap();
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::comparison("age", LogicalOperator::Eq, Literal::Int(30));
        assert!(predicate.matches(&row_view).unwrap());
    }

    #[test]
    fn does_not_match_for_the_row() {
        let schema = schema!["age" => ColumnType::Int].unwrap();
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::comparison("age", LogicalOperator::Greater, Literal::Int(30));
        assert!(!predicate.matches(&row_view).unwrap());
    }

    #[test]
    fn attempt_to_match_predicate_when_the_column_is_not_present_in_the_row() {
        let schema = schema!["age" => ColumnType::Int].unwrap();
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
        let schema = schema!["age" => ColumnType::Int].unwrap();
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

    #[test]
    fn matches_like_pattern() {
        let schema = schema!["name" => ColumnType::Text].unwrap();
        let row = row!["John"];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let regex = regex::Regex::new("^J").unwrap();
        let predicate = Predicate::like("name", regex);
        assert!(predicate.matches(&row_view).unwrap());
    }

    #[test]
    fn does_not_match_like_pattern() {
        let schema = schema!["name" => ColumnType::Text].unwrap();
        let row = row!["Doe"];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let regex = regex::Regex::new("^J").unwrap();
        let predicate = Predicate::like("name", regex);
        assert!(!predicate.matches(&row_view).unwrap());
    }

    #[test]
    fn attempt_to_match_predicate_when_there_is_a_column_type_mismatch_with_like() {
        let schema = schema!["age" => ColumnType::Int].unwrap();
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let regex = regex::Regex::new("^3").unwrap();
        let predicate = Predicate::like("age", regex);

        assert!(matches!(
            predicate.matches(&row_view),
            Err(ExecutionError::TypeMismatchInComparison)
        ));
    }

    #[test]
    fn attempt_to_match_predicate_when_the_column_is_not_present_in_the_row_with_like() {
        let schema = schema!["name" => ColumnType::Text].unwrap();
        let row = row!["John"];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let regex = regex::Regex::new("^J").unwrap();
        let predicate = Predicate::like("unknown", regex);

        assert!(matches!(
            predicate.matches(&row_view),
            Err(ExecutionError::UnknownColumn(col)) if col == "unknown"
        ));
    }

    #[test]
    fn predicate_from_where_clause_with_and() {
        let clause = WhereClause::And(vec![
            Condition::comparison("age", BinaryOperator::Greater, Literal::Int(30)),
            Condition::comparison(
                "city",
                BinaryOperator::Eq,
                Literal::Text("London".to_string()),
            ),
        ]);

        let predicate = Predicate::try_from(clause).unwrap();
        assert!(matches!(
            predicate,
            Predicate::And(conditions)
                if conditions.len() == 2 &&
                matches!(&conditions[0], LogicalCondition::Comparison { column_name, operator, literal }
                    if column_name == "age" && *operator == LogicalOperator::Greater && *literal == Literal::Int(30)) &&
                matches!(&conditions[1], LogicalCondition::Comparison { column_name, operator, literal }
                    if column_name == "city" && *operator == LogicalOperator::Eq && *literal == Literal::Text("London".to_string()))
        ));
    }

    #[test]
    fn attempt_to_create_predicate_from_where_clause_with_and_error() {
        let clause = WhereClause::And(vec![
            Condition::comparison("age", BinaryOperator::Greater, Literal::Int(30)),
            Condition::like("city", Literal::Text("[".to_string())),
        ]);

        let result = Predicate::try_from(clause);
        assert!(matches!(result, Err(PlanningError::InvalidRegex(_))));
    }

    #[test]
    fn matches_for_the_row_with_and() {
        let schema = schema!["age" => ColumnType::Int, "city" => ColumnType::Text].unwrap();
        let row = row![35, "London"];
        let visible_positions = vec![0, 1];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::And(vec![
            LogicalCondition::comparison("age", LogicalOperator::Greater, Literal::Int(30)),
            LogicalCondition::comparison(
                "city",
                LogicalOperator::Eq,
                Literal::Text("London".to_string()),
            ),
        ]);

        assert!(predicate.matches(&row_view).unwrap());
    }

    #[test]
    fn does_not_match_for_the_row_with_and() {
        let schema = schema!["age" => ColumnType::Int, "city" => ColumnType::Text].unwrap();
        let row = row![35, "Paris"];
        let visible_positions = vec![0, 1];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::And(vec![
            LogicalCondition::comparison("age", LogicalOperator::Greater, Literal::Int(30)),
            LogicalCondition::comparison(
                "city",
                LogicalOperator::Eq,
                Literal::Text("London".to_string()),
            ),
        ]);

        assert!(!predicate.matches(&row_view).unwrap());
    }

    #[test]
    fn attempt_to_match_predicate_when_with_and_when_the_column_is_not_present_in_the_row() {
        let schema = schema!["age" => ColumnType::Int, "city" => ColumnType::Text].unwrap();
        let row = row![35, "London"];
        let visible_positions = vec![0, 1];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::And(vec![
            LogicalCondition::comparison("age", LogicalOperator::Greater, Literal::Int(30)),
            LogicalCondition::comparison(
                "country",
                LogicalOperator::Eq,
                Literal::Text("UK".to_string()),
            ),
        ]);

        let result = predicate.matches(&row_view);
        assert!(matches!(
            result,
            Err(ExecutionError::UnknownColumn(col)) if col == "country"
        ));
    }
}

#[cfg(test)]
mod logical_condition_tests {
    use super::*;
    use crate::query::parser::ast::Literal;
    use crate::row;
    use crate::schema;
    use crate::storage::row_view::RowView;
    use crate::types::column_type::ColumnType;

    #[test]
    fn create_comparison_condition() {
        let condition =
            LogicalCondition::comparison("age", LogicalOperator::Greater, Literal::Int(18));

        assert!(matches!(
            condition,
            LogicalCondition::Comparison {
                column_name,
                operator: LogicalOperator::Greater,
                literal: Literal::Int(18),
            } if column_name == "age"
        ));
    }

    #[test]
    fn create_like_condition() {
        let regex = regex::Regex::new("^J").unwrap();
        let condition = LogicalCondition::like("name", regex);

        assert!(matches!(
            condition,
            LogicalCondition::Like {
                column_name,
                regex: _,
            } if column_name == "name"
        ));
    }

    #[test]
    fn matches_comparison() {
        let schema = schema!["age" => ColumnType::Int].unwrap();
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let condition = LogicalCondition::comparison("age", LogicalOperator::Eq, Literal::Int(30));
        assert!(condition.matches(&row_view).unwrap());
    }

    #[test]
    fn does_not_match_comparison() {
        let schema = schema!["age" => ColumnType::Int].unwrap();
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let condition =
            LogicalCondition::comparison("age", LogicalOperator::Greater, Literal::Int(30));
        assert!(!condition.matches(&row_view).unwrap());
    }

    #[test]
    fn matches_like() {
        let schema = schema!["name" => ColumnType::Text].unwrap();
        let row = row!["John"];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let regex = regex::Regex::new("^J").unwrap();
        let condition = LogicalCondition::like("name", regex);
        assert!(condition.matches(&row_view).unwrap());
    }

    #[test]
    fn does_not_match_like() {
        let schema = schema!["name" => ColumnType::Text].unwrap();
        let row = row!["Doe"];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let regex = regex::Regex::new("^J").unwrap();
        let condition = LogicalCondition::like("name", regex);
        assert!(!condition.matches(&row_view).unwrap());
    }

    #[test]
    fn attempt_to_match_condition_with_non_existing_column() {
        let schema = schema!["age" => ColumnType::Int].unwrap();
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let condition =
            LogicalCondition::comparison("height", LogicalOperator::Greater, Literal::Int(170));
        let result = condition.matches(&row_view);

        assert!(matches!(
            result,
            Err(ExecutionError::UnknownColumn(name)) if name == "height"
        ))
    }

    #[test]
    fn attempt_to_match_condition_with_column_type_mismatch() {
        let schema = schema!["age" => ColumnType::Int].unwrap();
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let condition = LogicalCondition::comparison(
            "age",
            LogicalOperator::Eq,
            Literal::Text("30".to_string()),
        );
        assert!(matches!(
            condition.matches(&row_view),
            Err(ExecutionError::TypeMismatchInComparison)
        ));
    }
}
