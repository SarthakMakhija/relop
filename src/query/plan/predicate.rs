use crate::query::executor::error::ExecutionError;
use crate::query::parser::ast::{BinaryOperator, Clause, Expression, Literal, WhereClause};
use crate::query::plan::error::PlanningError;
use crate::schema::Schema;
use crate::storage::row::Row;
use crate::storage::row_filter::RowFilter;
use crate::storage::row_view::RowView;
use crate::types::column_value::ColumnValue;

/// A trait for resolving column values from different sources (e.g., RowView, Row).
pub(crate) trait ValueResolver {
    /// Resolves a literal into a column value.
    fn resolve(&self, literal: &Literal) -> Result<ColumnValue, ExecutionError>;
}

impl ValueResolver for RowView<'_> {
    fn resolve(&self, literal: &Literal) -> Result<ColumnValue, ExecutionError> {
        match literal {
            Literal::Int(value) => Ok(ColumnValue::Int(*value)),
            Literal::Text(value) => Ok(ColumnValue::Text(value.clone())),
            Literal::ColumnReference(column_name) => self
                .column_value_by(column_name)
                .map_err(ExecutionError::Schema)?
                .ok_or(ExecutionError::UnknownColumn(column_name.to_string()))
                .cloned(),
            Literal::ColumnIndex(index) => Ok(self.column_value_at_unchecked(*index).clone()),
        }
    }
}

impl ValueResolver for Row {
    fn resolve(&self, literal: &Literal) -> Result<ColumnValue, ExecutionError> {
        match literal {
            Literal::Int(value) => Ok(ColumnValue::Int(*value)),
            Literal::Text(value) => Ok(ColumnValue::Text(value.clone())),
            Literal::ColumnIndex(index) => self
                .column_value_at(*index)
                .ok_or(ExecutionError::ColumnIndexOutOfBounds(*index))
                .cloned(),
            Literal::ColumnReference(column_name) => {
                Err(ExecutionError::UnboundColumn(column_name.to_string()))
            }
        }
    }
}

/// `Predicate` represents a filter clause in a logical plan.
/// `Predicate` represents a filter clause in a logical plan.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Predicate {
    Single(LogicalClause),
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
}

#[derive(Debug)]
pub(crate) enum LogicalClause {
    /// A comparison clause (e.g., `age > 30`).
    Comparison {
        /// The left-hand side literal.
        lhs: Literal,
        /// The logical comparison operator.
        operator: LogicalOperator,
        /// The right-hand side literal.
        rhs: Literal,
    },
    Like {
        /// The column to match against.
        column: Literal,
        /// The compiled regular expression for the pattern.
        regex: regex::Regex,
    },
}

impl PartialEq for LogicalClause {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Comparison {
                    lhs: first_left,
                    operator: first_operator,
                    rhs: first_right,
                },
                Self::Comparison {
                    lhs: second_left,
                    operator: second_operator,
                    rhs: second_right,
                },
            ) => {
                first_left == second_left
                    && first_operator == second_operator
                    && first_right == second_right
            }
            (
                Self::Like {
                    column: first_column,
                    regex: first_regex,
                },
                Self::Like {
                    column: second_column,
                    regex: second_regex,
                },
            ) => first_column == second_column && first_regex.as_str() == second_regex.as_str(),
            _ => false,
        }
    }
}

impl Eq for LogicalClause {}

impl LogicalClause {
    /// Evaluates the clause against a given `ValueResolver`.
    pub(crate) fn matches<V: ValueResolver>(&self, resolver: &V) -> Result<bool, ExecutionError> {
        match self {
            LogicalClause::Comparison { lhs, operator, rhs } => operator.apply(lhs, rhs, resolver),
            LogicalClause::Like { column, regex } => {
                let column_value = resolver.resolve(column)?;

                match column_value {
                    ColumnValue::Text(value) => Ok(regex.is_match(&value)),
                    _ => Err(ExecutionError::TypeMismatchInComparison),
                }
            }
        }
    }

    /// Binds the clause to a given `Schema`, resolving column names to indices.
    pub(crate) fn bind(self, schema: &Schema) -> Result<Self, PlanningError> {
        match self {
            LogicalClause::Comparison { lhs, operator, rhs } => Ok(LogicalClause::Comparison {
                lhs: bind_literal(lhs, schema)?,
                operator,
                rhs: bind_literal(rhs, schema)?,
            }),
            LogicalClause::Like { column, regex } => Ok(LogicalClause::Like {
                column: bind_literal(column, schema)?,
                regex,
            }),
        }
    }

    /// Returns a list of all column names referenced in the clause.
    pub(crate) fn referenced_column_names(&self) -> Vec<&String> {
        let mut columns = Vec::new();
        match self {
            LogicalClause::Comparison { lhs, rhs, .. } => {
                if let Literal::ColumnReference(name) = lhs {
                    columns.push(name);
                }
                if let Literal::ColumnReference(name) = rhs {
                    columns.push(name);
                }
            }
            LogicalClause::Like { column, .. } => {
                if let Literal::ColumnReference(name) = column {
                    columns.push(name);
                }
            }
        }
        columns
    }
}

fn bind_literal(literal: Literal, schema: &Schema) -> Result<Literal, PlanningError> {
    match literal {
        Literal::ColumnReference(column_name) => {
            let index = schema
                .column_position(&column_name)
                .map_err(|schema_error| PlanningError::ColumnNotFound(schema_error.to_string()))?
                .ok_or_else(|| PlanningError::ColumnNotFound(column_name.clone()))?;
            Ok(Literal::ColumnIndex(index))
        }
        _ => Ok(literal),
    }
}

#[cfg(test)]
impl LogicalClause {
    /// Creates a new `LogicalClause::Comparison` variant.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The name of the column to compare.
    /// * `operator` - The logical operator to use for comparison.
    /// * `literal` - The literal value to compare against.
    pub(crate) fn comparison(lhs: Literal, operator: LogicalOperator, rhs: Literal) -> Self {
        LogicalClause::Comparison { lhs, operator, rhs }
    }

    /// Creates a new `LogicalClause::Like` variant.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The name of the column to match against.
    /// * `regex` - The compiled regular expression pattern.
    pub(crate) fn like(column_name: &str, regex: regex::Regex) -> Self {
        LogicalClause::Like {
            column: Literal::ColumnReference(column_name.to_string()),
            regex,
        }
    }
}

impl TryFrom<WhereClause> for Predicate {
    type Error = PlanningError;

    /// Converts a `WhereClause` into a `Predicate`.
    fn try_from(where_clause: WhereClause) -> Result<Self, Self::Error> {
        Predicate::try_from(where_clause.0)
    }
}

impl TryFrom<Expression> for Predicate {
    type Error = PlanningError;

    /// Converts an `Expression` into a `Predicate`.
    fn try_from(expression: Expression) -> Result<Self, Self::Error> {
        match expression {
            Expression::Single(clause) => Ok(Predicate::Single(LogicalClause::try_from(clause)?)),
            Expression::And(expressions) => {
                let predicates = expressions
                    .into_iter()
                    .map(Predicate::try_from)
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Predicate::And(predicates))
            }
            Expression::Or(expressions) => {
                let predicates = expressions
                    .into_iter()
                    .map(Predicate::try_from)
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Predicate::Or(predicates))
            }
            Expression::Grouped(expression) => Predicate::try_from(*expression),
        }
    }
}

impl TryFrom<Clause> for LogicalClause {
    type Error = PlanningError;

    /// Converts a `Clause` into a `LogicalClause`.
    ///
    /// # Returns
    ///
    /// * `Ok(LogicalClause)` - If the conversion is successful.
    /// * `Err(PlanningError)` - If the conversion fails (e.g., due to an invalid regex).
    fn try_from(clause: Clause) -> Result<Self, Self::Error> {
        match clause {
            Clause::Comparison { lhs, operator, rhs } => Ok(LogicalClause::Comparison {
                lhs,
                operator: operator.into(),
                rhs,
            }),
            Clause::Like {
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
                let regex = regex::Regex::new(&regex_pattern)
                    .map_err(|err| PlanningError::InvalidRegex(err.to_string()))?;

                Ok(LogicalClause::Like {
                    column: Literal::ColumnReference(column_name),
                    regex,
                })
            }
        }
    }
}

impl Predicate {
    /// Evaluates the predicate against a given `ValueResolver`.
    ///
    /// Returns `Ok(true)` if the row satisfies the predicate, `Ok(false)` otherwise.
    /// Returns an `ExecutionError` if the column cannot be found.
    pub(crate) fn matches<R: ValueResolver>(&self, resolver: &R) -> Result<bool, ExecutionError> {
        match self {
            Predicate::Single(clause) => clause.matches(resolver),
            Predicate::And(predicates) => {
                for predicate in predicates {
                    if !predicate.matches(resolver)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Predicate::Or(predicates) => {
                for predicate in predicates {
                    if predicate.matches(resolver)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    /// Binds the predicate to a given `Schema`, resolving column names to indices.
    pub(crate) fn bind(self, schema: &Schema) -> Result<Self, PlanningError> {
        match self {
            Predicate::Single(clause) => Ok(Predicate::Single(clause.bind(schema)?)),
            Predicate::And(predicates) => {
                let bound = predicates
                    .into_iter()
                    .map(|predicate| predicate.bind(schema))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Predicate::And(bound))
            }
            Predicate::Or(predicates) => {
                let bound = predicates
                    .into_iter()
                    .map(|predicate| predicate.bind(schema))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Predicate::Or(bound))
            }
        }
    }

    /// Splits a predicate into a list of predicates, separated by AND.
    /// If the predicate is a single clause or an OR clause, it returns a vector with just itself.
    /// If the predicate is an AND clause, it returns the flattened list of its sub-predicates.
    pub(crate) fn split_by_and(self) -> Vec<Predicate> {
        match self {
            Predicate::And(predicates) => {
                let mut split = Vec::new();
                for predicate in predicates {
                    split.extend(predicate.split_by_and());
                }
                split
            }
            _ => vec![self],
        }
    }
    /// Returns `true` if all columns referenced by this predicate exist in the given schema.
    pub(crate) fn belongs_to(&self, schema: &Schema) -> bool {
        let mut all_columns = Vec::new();
        self.all_column_names(&mut all_columns);

        all_columns
            .iter()
            .all(|column_name| schema.has_column(column_name))
    }

    fn all_column_names<'a>(&'a self, all_columns: &mut Vec<&'a String>) {
        match self {
            Predicate::Single(clause) => all_columns.extend(clause.referenced_column_names()),
            Predicate::And(predicates) | Predicate::Or(predicates) => {
                for predicate in predicates {
                    predicate.all_column_names(all_columns);
                }
            }
        }
    }
}

impl RowFilter for Predicate {
    fn matches(&self, row: &Row) -> bool {
        self.matches(row).unwrap_or(false)
    }
}

#[cfg(test)]
impl Predicate {
    /// Creates a new `Comparison` predicate.
    pub(crate) fn comparison(lhs: Literal, operator: LogicalOperator, rhs: Literal) -> Self {
        Predicate::Single(LogicalClause::comparison(lhs, operator, rhs))
    }

    /// Creates a new `Like` predicate.
    pub(crate) fn like(column_name: &str, pattern: regex::Regex) -> Self {
        Predicate::Single(LogicalClause::like(column_name, pattern))
    }

    /// Creates a new `And` predicate.
    pub(crate) fn and(predicates: Vec<Predicate>) -> Self {
        Predicate::And(predicates)
    }

    /// Creates a new `Or` predicate.
    #[cfg(test)]
    pub(crate) fn or(predicates: Vec<Predicate>) -> Self {
        Predicate::Or(predicates)
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
    /// Lesser than `<`.
    Lesser,
    /// Lesser than or equal to `<=`.
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
    /// Evaluates the comparison between two column values.
    fn evaluate(&self, left: &ColumnValue, right: &ColumnValue) -> Result<bool, ExecutionError> {
        match (left, right) {
            (ColumnValue::Int(left_value), ColumnValue::Int(right_value)) => Ok(match self {
                LogicalOperator::Eq => left_value == right_value,
                LogicalOperator::NotEq => left_value != right_value,
                LogicalOperator::Greater => left_value > right_value,
                LogicalOperator::GreaterEq => left_value >= right_value,
                LogicalOperator::Lesser => left_value < right_value,
                LogicalOperator::LesserEq => left_value <= right_value,
            }),
            (ColumnValue::Text(left_value), ColumnValue::Text(right_value)) => Ok(match self {
                LogicalOperator::Eq => left_value == right_value,
                LogicalOperator::NotEq => left_value != right_value,
                LogicalOperator::Greater => left_value > right_value,
                LogicalOperator::GreaterEq => left_value >= right_value,
                LogicalOperator::Lesser => left_value < right_value,
                LogicalOperator::LesserEq => left_value <= right_value,
            }),
            _ => Err(ExecutionError::TypeMismatchInComparison),
        }
    }

    /// Applies the logical operator to compare values resolved from a `ValueResolver`.
    pub(crate) fn apply<V: ValueResolver>(
        &self,
        lhs: &Literal,
        rhs: &Literal,
        resolver: &V,
    ) -> Result<bool, ExecutionError> {
        let lhs_value = resolver.resolve(lhs)?;
        let rhs_value = resolver.resolve(rhs)?;
        self.evaluate(&lhs_value, &rhs_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{BinaryOperator, Literal};

    #[test]
    fn logical_clause_columns_for_comparison_with_column_on_lhs() {
        let clause = LogicalClause::comparison(
            Literal::ColumnReference("age".to_string()),
            LogicalOperator::Greater,
            Literal::Int(18),
        );
        assert_eq!(vec!["age"], clause.referenced_column_names());
    }

    #[test]
    fn logical_clause_columns_for_comparison_with_columns_on_both_sides() {
        let clause = LogicalClause::comparison(
            Literal::ColumnReference("e.dept_id".to_string()),
            LogicalOperator::Eq,
            Literal::ColumnReference("d.id".to_string()),
        );
        let columns = clause.referenced_column_names();
        assert_eq!(2, columns.len());
        assert!(columns.contains(&&"e.dept_id".to_string()));
        assert!(columns.contains(&&"d.id".to_string()));
    }

    #[test]
    fn logical_clause_columns_for_like() {
        let clause = LogicalClause::like("name", regex::Regex::new("r.*").unwrap());
        assert_eq!(vec!["name"], clause.referenced_column_names());
    }

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
        let schema = crate::schema!["id" => crate::types::column_type::ColumnType::Int].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row![10], &schema, &visible_positions);
        assert!(LogicalOperator::Eq
            .apply(&Literal::Int(10), &Literal::Int(10), &row_view)
            .unwrap());
    }

    #[test]
    fn apply_eq_on_integers_false() {
        let schema = crate::schema!["id" => crate::types::column_type::ColumnType::Int].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row![10], &schema, &visible_positions);
        assert!(!LogicalOperator::Eq
            .apply(&Literal::Int(10), &Literal::Int(5), &row_view)
            .unwrap());
    }

    #[test]
    fn apply_not_eq_on_integers_true() {
        let schema = crate::schema!["id" => crate::types::column_type::ColumnType::Int].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row![10], &schema, &visible_positions);
        assert!(LogicalOperator::NotEq
            .apply(&Literal::Int(10), &Literal::Int(5), &row_view)
            .unwrap());
    }

    #[test]
    fn apply_not_eq_on_integers_false() {
        let schema = crate::schema!["id" => crate::types::column_type::ColumnType::Int].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row![10], &schema, &visible_positions);
        assert!(!LogicalOperator::NotEq
            .apply(&Literal::Int(10), &Literal::Int(10), &row_view)
            .unwrap());
    }

    #[test]
    fn apply_greater_on_integers_true() {
        let schema = crate::schema!["id" => crate::types::column_type::ColumnType::Int].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row![10], &schema, &visible_positions);
        assert!(LogicalOperator::Greater
            .apply(&Literal::Int(10), &Literal::Int(5), &row_view)
            .unwrap());
    }

    #[test]
    fn apply_greater_on_integers_false() {
        let schema = crate::schema!["id" => crate::types::column_type::ColumnType::Int].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row![10], &schema, &visible_positions);
        assert!(!LogicalOperator::Greater
            .apply(&Literal::Int(5), &Literal::Int(10), &row_view)
            .unwrap());
    }

    #[test]
    fn apply_greater_eq_on_integers_true_greater() {
        let schema = crate::schema!["id" => crate::types::column_type::ColumnType::Int].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row![10], &schema, &visible_positions);
        assert!(LogicalOperator::GreaterEq
            .apply(&Literal::Int(10), &Literal::Int(5), &row_view)
            .unwrap());
    }

    #[test]
    fn apply_greater_eq_on_integers_true_eq() {
        let schema = crate::schema!["id" => crate::types::column_type::ColumnType::Int].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row![10], &schema, &visible_positions);
        assert!(LogicalOperator::GreaterEq
            .apply(&Literal::Int(10), &Literal::Int(10), &row_view)
            .unwrap());
    }

    #[test]
    fn apply_greater_eq_on_integers_false() {
        let schema = crate::schema!["id" => crate::types::column_type::ColumnType::Int].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row![10], &schema, &visible_positions);
        assert!(!LogicalOperator::GreaterEq
            .apply(&Literal::Int(5), &Literal::Int(10), &row_view)
            .unwrap());
    }

    #[test]
    fn apply_lesser_on_integers_true() {
        let schema = crate::schema!["id" => crate::types::column_type::ColumnType::Int].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row![10], &schema, &visible_positions);
        assert!(LogicalOperator::Lesser
            .apply(&Literal::Int(5), &Literal::Int(10), &row_view)
            .unwrap());
    }

    #[test]
    fn apply_lesser_on_integers_false() {
        let schema = crate::schema!["id" => crate::types::column_type::ColumnType::Int].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row![10], &schema, &visible_positions);
        assert!(!LogicalOperator::Lesser
            .apply(&Literal::Int(10), &Literal::Int(5), &row_view)
            .unwrap());
    }

    #[test]
    fn apply_lesser_eq_on_integers_true_lesser() {
        let schema = crate::schema!["id" => crate::types::column_type::ColumnType::Int].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row![10], &schema, &visible_positions);
        assert!(LogicalOperator::LesserEq
            .apply(&Literal::Int(5), &Literal::Int(10), &row_view)
            .unwrap());
    }

    #[test]
    fn apply_lesser_eq_on_integers_true_eq() {
        let schema = crate::schema!["id" => crate::types::column_type::ColumnType::Int].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row![10], &schema, &visible_positions);
        assert!(LogicalOperator::LesserEq
            .apply(&Literal::Int(10), &Literal::Int(10), &row_view)
            .unwrap());
    }

    #[test]
    fn apply_lesser_eq_on_integers_false() {
        let schema = crate::schema!["id" => crate::types::column_type::ColumnType::Int].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row![10], &schema, &visible_positions);
        assert!(!LogicalOperator::LesserEq
            .apply(&Literal::Int(10), &Literal::Int(5), &row_view)
            .unwrap());
    }

    #[test]
    fn apply_eq_on_strings_true() {
        let schema = crate::schema!["name" => crate::types::column_type::ColumnType::Text].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row!["relop"], &schema, &visible_positions);
        assert!(LogicalOperator::Eq
            .apply(
                &Literal::Text("relop".to_string()),
                &Literal::Text("relop".to_string()),
                &row_view
            )
            .unwrap());
    }

    #[test]
    fn apply_eq_on_strings_false() {
        let schema = crate::schema!["name" => crate::types::column_type::ColumnType::Text].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row!["relop"], &schema, &visible_positions);
        assert!(!LogicalOperator::Eq
            .apply(
                &Literal::Text("relop".to_string()),
                &Literal::Text("rust".to_string()),
                &row_view
            )
            .unwrap());
    }

    #[test]
    fn apply_not_eq_on_strings_true() {
        let schema = crate::schema!["name" => crate::types::column_type::ColumnType::Text].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row!["relop"], &schema, &visible_positions);
        assert!(LogicalOperator::NotEq
            .apply(
                &Literal::Text("relop".to_string()),
                &Literal::Text("rust".to_string()),
                &row_view
            )
            .unwrap());
    }

    #[test]
    fn apply_not_eq_on_strings_false() {
        let schema = crate::schema!["name" => crate::types::column_type::ColumnType::Text].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row!["relop"], &schema, &visible_positions);
        assert!(!LogicalOperator::NotEq
            .apply(
                &Literal::Text("relop".to_string()),
                &Literal::Text("relop".to_string()),
                &row_view
            )
            .unwrap());
    }

    #[test]
    fn apply_greater_on_strings_true() {
        let schema = crate::schema!["name" => crate::types::column_type::ColumnType::Text].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row!["rust"], &schema, &visible_positions);
        assert!(LogicalOperator::Greater
            .apply(
                &Literal::Text("rust".to_string()),
                &Literal::Text("relop".to_string()),
                &row_view
            )
            .unwrap());
    }

    #[test]
    fn apply_greater_on_strings_false() {
        let schema = crate::schema!["name" => crate::types::column_type::ColumnType::Text].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row!["relop"], &schema, &visible_positions);
        assert!(!LogicalOperator::Greater
            .apply(
                &Literal::Text("relop".to_string()),
                &Literal::Text("rust".to_string()),
                &row_view
            )
            .unwrap());
    }

    #[test]
    fn apply_greater_eq_on_strings_true_greater() {
        let schema = crate::schema!["name" => crate::types::column_type::ColumnType::Text].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row!["rust"], &schema, &visible_positions);
        assert!(LogicalOperator::GreaterEq
            .apply(
                &Literal::Text("rust".to_string()),
                &Literal::Text("relop".to_string()),
                &row_view
            )
            .unwrap());
    }

    #[test]
    fn apply_greater_eq_on_strings_true_eq() {
        let schema = crate::schema!["name" => crate::types::column_type::ColumnType::Text].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row!["relop"], &schema, &visible_positions);
        assert!(LogicalOperator::GreaterEq
            .apply(
                &Literal::Text("relop".to_string()),
                &Literal::Text("relop".to_string()),
                &row_view
            )
            .unwrap());
    }

    #[test]
    fn apply_greater_eq_on_strings_false() {
        let schema = crate::schema!["name" => crate::types::column_type::ColumnType::Text].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row!["relop"], &schema, &visible_positions);
        assert!(!LogicalOperator::GreaterEq
            .apply(
                &Literal::Text("relop".to_string()),
                &Literal::Text("rust".to_string()),
                &row_view
            )
            .unwrap());
    }

    #[test]
    fn apply_lesser_on_strings_true() {
        let schema = crate::schema!["name" => crate::types::column_type::ColumnType::Text].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row!["relop"], &schema, &visible_positions);
        assert!(LogicalOperator::Lesser
            .apply(
                &Literal::Text("relop".to_string()),
                &Literal::Text("rust".to_string()),
                &row_view
            )
            .unwrap());
    }

    #[test]
    fn apply_lesser_on_strings_false() {
        let schema = crate::schema!["name" => crate::types::column_type::ColumnType::Text].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row!["rust"], &schema, &visible_positions);
        assert!(!LogicalOperator::Lesser
            .apply(
                &Literal::Text("rust".to_string()),
                &Literal::Text("relop".to_string()),
                &row_view
            )
            .unwrap());
    }

    #[test]
    fn apply_lesser_eq_on_strings_true_lesser() {
        let schema = crate::schema!["name" => crate::types::column_type::ColumnType::Text].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row!["relop"], &schema, &visible_positions);
        assert!(LogicalOperator::LesserEq
            .apply(
                &Literal::Text("relop".to_string()),
                &Literal::Text("rust".to_string()),
                &row_view
            )
            .unwrap());
    }

    #[test]
    fn apply_lesser_eq_on_strings_true_eq() {
        let schema = crate::schema!["name" => crate::types::column_type::ColumnType::Text].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row!["relop"], &schema, &visible_positions);
        assert!(LogicalOperator::LesserEq
            .apply(
                &Literal::Text("relop".to_string()),
                &Literal::Text("relop".to_string()),
                &row_view
            )
            .unwrap());
    }

    #[test]
    fn apply_lesser_eq_on_strings_false() {
        let schema = crate::schema!["name" => crate::types::column_type::ColumnType::Text].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row!["rust"], &schema, &visible_positions);
        assert!(!LogicalOperator::LesserEq
            .apply(
                &Literal::Text("rust".to_string()),
                &Literal::Text("relop".to_string()),
                &row_view
            )
            .unwrap());
    }

    #[test]
    fn apply_logical_operator_type_mismatch() {
        let schema = crate::schema!["id" => crate::types::column_type::ColumnType::Int].unwrap();
        let visible_positions = vec![0];
        let row_view = RowView::new(crate::row![10], &schema, &visible_positions);
        let operator = LogicalOperator::Eq;
        assert!(matches!(
            operator.apply(
                &Literal::Int(10),
                &Literal::Text("10".to_string()),
                &row_view
            ),
            Err(ExecutionError::TypeMismatchInComparison)
        ));
    }

    #[test]
    fn apply_eq_with_column_reference() {
        let schema = crate::schema![
            "last_name" => crate::types::column_type::ColumnType::Text
        ]
        .unwrap();
        let visible_positions = vec![0, 1];
        let row_view = RowView::new(crate::row!["relop"], &schema, &visible_positions);

        assert!(LogicalOperator::Eq
            .apply(
                &Literal::ColumnReference("last_name".to_string()),
                &Literal::ColumnReference("last_name".to_string()),
                &row_view
            )
            .unwrap());
    }

    #[test]
    fn apply_eq_with_column_reference_false() {
        let schema = crate::schema![
            "first_name" => crate::types::column_type::ColumnType::Text,
            "last_name" => crate::types::column_type::ColumnType::Text
        ]
        .unwrap();
        let visible_positions = vec![0, 1];
        let row_view = RowView::new(crate::row!["relop", "query"], &schema, &visible_positions);

        assert!(!LogicalOperator::Eq
            .apply(
                &Literal::ColumnReference("first_name".to_string()),
                &Literal::ColumnReference("last_name".to_string()),
                &row_view
            )
            .unwrap());
    }
}

#[cfg(test)]
mod logical_operator_tests {
    use super::*;

    #[test]
    fn evaluate_int_equal() {
        assert!(LogicalOperator::Eq
            .evaluate(&ColumnValue::int(1), &ColumnValue::int(1))
            .unwrap(),);
    }

    #[test]
    fn evaluate_int_not_equal() {
        assert!(!LogicalOperator::Eq
            .evaluate(&ColumnValue::int(1), &ColumnValue::int(2))
            .unwrap(),);
    }

    #[test]
    fn evaluate_int_greater() {
        assert!(LogicalOperator::Greater
            .evaluate(&ColumnValue::int(2), &ColumnValue::int(1))
            .unwrap(),);
    }

    #[test]
    fn evaluate_int_greater_equal() {
        assert!(LogicalOperator::GreaterEq
            .evaluate(&ColumnValue::int(2), &ColumnValue::int(2))
            .unwrap(),);
    }

    #[test]
    fn evaluate_int_lesser() {
        assert!(LogicalOperator::Lesser
            .evaluate(&ColumnValue::int(1), &ColumnValue::int(2))
            .unwrap(),);
    }

    #[test]
    fn evaluate_int_lesser_equal() {
        assert!(LogicalOperator::LesserEq
            .evaluate(&ColumnValue::int(1), &ColumnValue::int(1))
            .unwrap(),);
    }

    #[test]
    fn evaluate_text_equal() {
        assert!(LogicalOperator::Eq
            .evaluate(&ColumnValue::text("a"), &ColumnValue::text("a"))
            .unwrap(),);
    }

    #[test]
    fn evaluate_text_not_equal() {
        assert!(LogicalOperator::NotEq
            .evaluate(&ColumnValue::text("a"), &ColumnValue::text("b"))
            .unwrap(),);
    }

    #[test]
    fn evaluate_type_mismatch() {
        let result = LogicalOperator::Eq.evaluate(&ColumnValue::int(1), &ColumnValue::text("1"));
        assert!(matches!(
            result,
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
    use crate::types::column_type::ColumnType;

    #[test]
    fn create_comparison_predicate() {
        let predicate = Predicate::comparison(
            Literal::ColumnReference("age".to_string()),
            LogicalOperator::Greater,
            Literal::Int(18),
        );
        assert!(matches!(
            predicate,
            Predicate::Single(LogicalClause::Comparison {
                ref lhs,
                operator: LogicalOperator::Greater,
                rhs: Literal::Int(18),
            }) if matches!(lhs, Literal::ColumnReference(ref name) if name == "age")
        ));
    }

    #[test]
    fn create_like_predicate() {
        let regex = regex::Regex::new("^J").unwrap();
        let predicate = Predicate::like("name", regex);
        assert!(matches!(
            predicate,
            Predicate::Single(LogicalClause::Like {
                column,
                regex: _,
            }) if matches!(column, Literal::ColumnReference(ref name) if name == "name")
        ));
    }

    #[test]
    fn predicate_from_where_clause() {
        let clause = WhereClause::comparison(
            Literal::ColumnReference("age".to_string()),
            BinaryOperator::Greater,
            Literal::Int(30),
        );

        let predicate = Predicate::try_from(clause).unwrap();
        assert!(matches!(
            predicate,
            Predicate::Single(LogicalClause::Comparison { ref lhs, ref operator, ref rhs })
                if matches!(lhs, Literal::ColumnReference(ref name) if name == "age")
                && *operator == LogicalOperator::Greater
                && *rhs == Literal::Int(30)
        ));
    }

    #[test]
    fn predicate_from_grouped_expression() {
        let expr = Expression::grouped(Expression::single(Clause::comparison(
            Literal::ColumnReference("age".to_string()),
            BinaryOperator::Greater,
            Literal::Int(30),
        )));

        let predicate = Predicate::try_from(expr).unwrap();
        assert!(matches!(
            predicate,
            Predicate::Single(LogicalClause::Comparison { ref lhs, ref operator, ref rhs })
                if matches!(lhs, Literal::ColumnReference(ref name) if name == "age")
                && *operator == LogicalOperator::Greater
                && *rhs == Literal::Int(30)
        ));
    }

    #[test]
    fn predicate_from_nested_grouped_expression() {
        let expr =
            Expression::grouped(Expression::grouped(Expression::single(Clause::comparison(
                Literal::ColumnReference("age".to_string()),
                BinaryOperator::Greater,
                Literal::Int(30),
            ))));

        let predicate = Predicate::try_from(expr).unwrap();
        assert!(matches!(
            predicate,
            Predicate::Single(LogicalClause::Comparison { ref lhs, ref operator, ref rhs })
                if matches!(lhs, Literal::ColumnReference(ref name) if name == "age")
                && *operator == LogicalOperator::Greater
                && *rhs == Literal::Int(30)
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
            Ok(Predicate::Single(LogicalClause::Like { column, regex: _ })) if matches!(column, Literal::ColumnReference(ref name) if name == "name")
        ));
    }

    #[test]
    fn matches_for_the_row() {
        let schema = schema!["age" => ColumnType::Int].unwrap();
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::comparison(
            Literal::ColumnReference("age".to_string()),
            LogicalOperator::Eq,
            Literal::Int(30),
        );
        assert!(predicate.matches(&row_view).unwrap());
    }

    #[test]
    fn does_not_match_for_the_row() {
        let schema = schema!["age" => ColumnType::Int].unwrap();
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::comparison(
            Literal::ColumnReference("age".to_string()),
            LogicalOperator::Greater,
            Literal::Int(30),
        );
        assert!(!predicate.matches(&row_view).unwrap());
    }

    #[test]
    fn attempt_to_match_predicate_when_the_column_is_not_present_in_the_row() {
        let schema = schema!["age" => ColumnType::Int].unwrap();
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::comparison(
            Literal::ColumnReference("height".to_string()),
            LogicalOperator::Greater,
            Literal::Int(170),
        );
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

        let predicate = Predicate::comparison(
            Literal::ColumnReference("age".to_string()),
            LogicalOperator::Eq,
            Literal::Text("30".to_string()),
        );
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
        let clause = WhereClause::and(vec![
            Expression::single(Clause::comparison(
                Literal::ColumnReference("age".to_string()),
                BinaryOperator::Greater,
                Literal::Int(30),
            )),
            Expression::single(Clause::comparison(
                Literal::ColumnReference("city".to_string()),
                BinaryOperator::Eq,
                Literal::Text("London".to_string()),
            )),
        ]);

        let predicate = Predicate::try_from(clause).unwrap();
        assert!(matches!(
            predicate,
            Predicate::And(clauses)
                if clauses.len() == 2 &&
                matches!(&clauses[0], Predicate::Single(LogicalClause::Comparison { ref lhs, ref operator, ref rhs })
                    if matches!(lhs, Literal::ColumnReference(ref name) if name == "age") && *operator == LogicalOperator::Greater && *rhs == Literal::Int(30)) &&
                matches!(&clauses[1], Predicate::Single(LogicalClause::Comparison { ref lhs, ref operator, ref rhs } )
                    if matches!(lhs, Literal::ColumnReference(ref name) if name == "city") && *operator == LogicalOperator::Eq && *rhs == Literal::Text("London".to_string()))
        ));
    }

    #[test]
    fn attempt_to_create_predicate_from_where_clause_with_and_error() {
        let clause = WhereClause::and(vec![
            Expression::single(Clause::comparison(
                Literal::ColumnReference("age".to_string()),
                BinaryOperator::Greater,
                Literal::Int(30),
            )),
            Expression::single(Clause::like("city", Literal::Text("[".to_string()))),
        ]);

        let result = Predicate::try_from(clause);
        assert!(matches!(result, Err(PlanningError::InvalidRegex(_))));
    }

    #[test]
    fn predicate_from_where_clause_with_or() {
        let clause = WhereClause::or(vec![
            Expression::single(Clause::comparison(
                Literal::ColumnReference("age".to_string()),
                BinaryOperator::Greater,
                Literal::Int(30),
            )),
            Expression::single(Clause::comparison(
                Literal::ColumnReference("city".to_string()),
                BinaryOperator::Eq,
                Literal::Text("London".to_string()),
            )),
        ]);

        let predicate = Predicate::try_from(clause).unwrap();
        assert!(matches!(
            predicate,
            Predicate::Or(clauses)
                if clauses.len() == 2 &&
                matches!(&clauses[0], Predicate::Single(LogicalClause::Comparison { ref lhs, ref operator, ref rhs })
                    if matches!(lhs, Literal::ColumnReference(ref name) if name == "age") && *operator == LogicalOperator::Greater && *rhs == Literal::Int(30)) &&
                matches!(&clauses[1], Predicate::Single(LogicalClause::Comparison { ref lhs, ref operator, ref rhs } )
                    if matches!(lhs, Literal::ColumnReference(ref name) if name == "city") && *operator == LogicalOperator::Eq && *rhs == Literal::Text("London".to_string()))
        ));
    }

    #[test]
    fn matches_for_the_row_with_and() {
        let schema = schema!["age" => ColumnType::Int, "city" => ColumnType::Text].unwrap();
        let row = row![35, "London"];
        let visible_positions = vec![0, 1];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::and(vec![
            Predicate::comparison(
                Literal::ColumnReference("age".to_string()),
                LogicalOperator::Greater,
                Literal::Int(30),
            ),
            Predicate::comparison(
                Literal::ColumnReference("city".to_string()),
                LogicalOperator::Eq,
                Literal::Text("London".to_string()),
            ),
        ]);

        assert!(predicate.matches(&row_view).unwrap());
    }

    #[test]
    fn matches_for_the_row_with_nested_and() {
        let schema = schema![
            "age" => ColumnType::Int,
            "city" => ColumnType::Text,
            "country" => ColumnType::Text
        ]
        .unwrap();
        let row = row![35, "London", "UK"];
        let visible_positions = vec![0, 1, 2];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::and(vec![
            Predicate::comparison(
                Literal::ColumnReference("age".to_string()),
                LogicalOperator::Greater,
                Literal::Int(30),
            ),
            Predicate::and(vec![
                Predicate::comparison(
                    Literal::ColumnReference("city".to_string()),
                    LogicalOperator::Eq,
                    Literal::Text("London".to_string()),
                ),
                Predicate::comparison(
                    Literal::ColumnReference("country".to_string()),
                    LogicalOperator::Eq,
                    Literal::Text("UK".to_string()),
                ),
            ]),
        ]);

        assert!(predicate.matches(&row_view).unwrap());
    }

    #[test]
    fn does_not_match_for_the_row_with_and() {
        let schema = schema!["age" => ColumnType::Int, "city" => ColumnType::Text].unwrap();
        let row = row![35, "Paris"];
        let visible_positions = vec![0, 1];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::and(vec![
            Predicate::comparison(
                Literal::ColumnReference("age".to_string()),
                LogicalOperator::Greater,
                Literal::Int(30),
            ),
            Predicate::comparison(
                Literal::ColumnReference("city".to_string()),
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

        let predicate = Predicate::and(vec![
            Predicate::comparison(
                Literal::ColumnReference("age".to_string()),
                LogicalOperator::Greater,
                Literal::Int(30),
            ),
            Predicate::comparison(
                Literal::ColumnReference("country".to_string()),
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

    #[test]
    fn matches_for_the_row_with_or() {
        let schema = schema!["age" => ColumnType::Int, "city" => ColumnType::Text].unwrap();
        let row = row![25, "London"];
        let visible_positions = vec![0, 1];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::or(vec![
            Predicate::comparison(
                Literal::ColumnReference("age".to_string()),
                LogicalOperator::Greater,
                Literal::Int(30),
            ),
            Predicate::comparison(
                Literal::ColumnReference("city".to_string()),
                LogicalOperator::Eq,
                Literal::Text("London".to_string()),
            ),
        ]);

        assert!(predicate.matches(&row_view).unwrap());
    }

    #[test]
    fn does_not_match_for_the_row_with_or() {
        let schema = schema!["age" => ColumnType::Int, "city" => ColumnType::Text].unwrap();
        let row = row![25, "Paris"];
        let visible_positions = vec![0, 1];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::or(vec![
            Predicate::comparison(
                Literal::ColumnReference("age".to_string()),
                LogicalOperator::Greater,
                Literal::Int(30),
            ),
            Predicate::comparison(
                Literal::ColumnReference("city".to_string()),
                LogicalOperator::Eq,
                Literal::Text("London".to_string()),
            ),
        ]);

        assert!(!predicate.matches(&row_view).unwrap());
    }

    #[test]
    fn matches_for_the_row_with_nested_or() {
        let schema = schema![
            "age" => ColumnType::Int,
            "city" => ColumnType::Text,
            "country" => ColumnType::Text
        ]
        .unwrap();
        let row = row![25, "Paris", "FR"];
        let visible_positions = vec![0, 1, 2];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let predicate = Predicate::or(vec![
            Predicate::comparison(
                Literal::ColumnReference("age".to_string()),
                LogicalOperator::Greater,
                Literal::Int(30),
            ),
            Predicate::or(vec![
                Predicate::comparison(
                    Literal::ColumnReference("city".to_string()),
                    LogicalOperator::Eq,
                    Literal::Text("London".to_string()),
                ),
                Predicate::comparison(
                    Literal::ColumnReference("country".to_string()),
                    LogicalOperator::Eq,
                    Literal::Text("FR".to_string()),
                ),
            ]),
        ]);

        assert!(predicate.matches(&row_view).unwrap());
    }
}

#[cfg(test)]
mod logical_clause_tests {
    use super::*;
    use crate::query::parser::ast::Literal;
    use crate::row;
    use crate::schema;
    use crate::schema::Schema;
    use crate::storage::row_view::RowView;
    use crate::types::column_type::ColumnType;

    #[test]
    fn create_comparison_clause() {
        let clause = LogicalClause::comparison(
            Literal::ColumnReference("age".to_string()),
            LogicalOperator::Greater,
            Literal::Int(18),
        );

        assert!(matches!(
            clause,
            LogicalClause::Comparison {
                ref lhs,
                operator: LogicalOperator::Greater,
                rhs: Literal::Int(18),
            } if matches!(lhs, Literal::ColumnReference(ref name) if name == "age")
        ));
    }

    #[test]
    fn create_like_clause() {
        let regex = regex::Regex::new("^J").unwrap();
        let clause = LogicalClause::like("name", regex);

        assert!(matches!(
            clause,
            LogicalClause::Like {
                column,
                regex: _,
            } if matches!(column, Literal::ColumnReference(ref name) if name == "name")
        ));
    }

    #[test]
    fn matches_comparison() {
        let schema = schema!["age" => ColumnType::Int].unwrap();
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let clause = LogicalClause::comparison(
            Literal::ColumnReference("age".to_string()),
            LogicalOperator::Eq,
            Literal::Int(30),
        );
        assert!(clause.matches(&row_view).unwrap());
    }

    #[test]
    fn does_not_match_comparison() {
        let schema = schema!["age" => ColumnType::Int].unwrap();
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let clause = LogicalClause::comparison(
            Literal::ColumnReference("age".to_string()),
            LogicalOperator::Greater,
            Literal::Int(30),
        );
        assert!(!clause.matches(&row_view).unwrap());
    }

    #[test]
    fn matches_like() {
        let schema = schema!["name" => ColumnType::Text].unwrap();
        let row = row!["John"];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let regex = regex::Regex::new("^J").unwrap();
        let clause = LogicalClause::like("name", regex);
        assert!(clause.matches(&row_view).unwrap());
    }

    #[test]
    fn does_not_match_like() {
        let schema = schema!["name" => ColumnType::Text].unwrap();
        let row = row!["Doe"];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let regex = regex::Regex::new("^J").unwrap();
        let clause = LogicalClause::like("name", regex);
        assert!(!clause.matches(&row_view).unwrap());
    }

    #[test]
    fn attempt_to_match_clause_with_non_existing_column() {
        let schema = schema!["age" => ColumnType::Int].unwrap();
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let clause = LogicalClause::comparison(
            Literal::ColumnReference("height".to_string()),
            LogicalOperator::Greater,
            Literal::Int(170),
        );
        let result = clause.matches(&row_view);

        assert!(matches!(
            result,
            Err(ExecutionError::UnknownColumn(name)) if name == "height"
        ))
    }

    #[test]
    fn attempt_to_match_clause_with_column_type_mismatch() {
        let schema = schema!["age" => ColumnType::Int].unwrap();
        let row = row![30];
        let visible_positions = vec![0];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let clause = LogicalClause::comparison(
            Literal::ColumnReference("age".to_string()),
            LogicalOperator::Eq,
            Literal::Text("30".to_string()),
        );
        assert!(matches!(
            clause.matches(&row_view),
            Err(ExecutionError::TypeMismatchInComparison)
        ));
    }

    #[test]
    fn matches_for_the_row_with_two_column_references() {
        let schema = schema!["rank" => ColumnType::Int, "degree" => ColumnType::Int].unwrap();
        let row = row![30, 30];
        let visible_positions = vec![0, 1];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let clause = LogicalClause::comparison(
            Literal::ColumnReference("rank".to_string()),
            LogicalOperator::Eq,
            Literal::ColumnReference("degree".to_string()),
        );
        assert!(clause.matches(&row_view).unwrap());
    }

    #[test]
    fn matches_for_the_row_with_ambiguous_column_lookup() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("employees.id", ColumnType::Int)
            .unwrap()
            .add_column("departments.id", ColumnType::Int)
            .unwrap();
        let row = row![1, 2];
        let visible_positions = vec![0, 1];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let clause = LogicalClause::comparison(
            Literal::ColumnReference("id".to_string()),
            LogicalOperator::Eq,
            Literal::Int(1),
        );
        let result = clause.matches(&row_view);
        assert!(matches!(
            result,
            Err(ExecutionError::Schema(schema::error::SchemaError::AmbiguousColumnName(ref column_name))) if column_name == "id"
        ));
    }

    #[test]
    fn matches_for_the_row_with_ambiguous_column_lookup_in_like() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("employees.name", ColumnType::Text)
            .unwrap()
            .add_column("departments.name", ColumnType::Text)
            .unwrap();
        let row = row!["relop", "engineering"];
        let visible_positions = vec![0, 1];
        let row_view = RowView::new(row, &schema, &visible_positions);

        let clause = LogicalClause::Like {
            column: Literal::ColumnReference("name".to_string()),
            regex: regex::Regex::new("relop").unwrap(),
        };
        let result = clause.matches(&row_view);
        assert!(matches!(
            result,
            Err(ExecutionError::Schema(schema::error::SchemaError::AmbiguousColumnName(ref column_name))) if column_name == "name"
        ));
    }

    #[test]
    fn comparison_clauses_are_equal() {
        let clause1 = LogicalClause::comparison(
            Literal::ColumnReference("age".to_string()),
            LogicalOperator::Greater,
            Literal::Int(18),
        );
        let clause2 = LogicalClause::comparison(
            Literal::ColumnReference("age".to_string()),
            LogicalOperator::Greater,
            Literal::Int(18),
        );

        assert_eq!(clause1, clause2);
    }

    #[test]
    fn comparison_clauses_are_not_equal() {
        let clause1 = LogicalClause::comparison(
            Literal::ColumnReference("age".to_string()),
            LogicalOperator::Greater,
            Literal::Int(18),
        );
        let clause2 = LogicalClause::comparison(
            Literal::ColumnReference("age".to_string()),
            LogicalOperator::Lesser,
            Literal::Int(18),
        );

        assert_ne!(clause1, clause2);
    }

    #[test]
    fn like_clauses_are_equal() {
        let clause1 = LogicalClause::like("name", regex::Regex::new("^J").unwrap());
        let clause2 = LogicalClause::like("name", regex::Regex::new("^J").unwrap());

        assert_eq!(clause1, clause2);
    }

    #[test]
    fn like_clauses_are_not_equal() {
        let clause1 = LogicalClause::like("name", regex::Regex::new("^J").unwrap());
        let clause2 = LogicalClause::like("name", regex::Regex::new("^P").unwrap());

        assert_ne!(clause1, clause2);
    }
}

#[cfg(test)]
mod row_view_value_resolver_tests {
    use super::*;
    use crate::schema::Schema;
    use crate::types::column_type::ColumnType;

    #[test]
    fn resolve_by_name() {
        let schema = Schema::new().add_column("age", ColumnType::Int).unwrap();
        let row = Row::filled(vec![ColumnValue::int(30)]);
        let row_view = RowView::new(row, &schema, &[0]);

        let literal = Literal::ColumnReference("age".to_string());
        let value = row_view.resolve(&literal).unwrap();
        assert_eq!(value, ColumnValue::int(30));
    }

    #[test]
    fn resolve_by_index() {
        let schema = Schema::new().add_column("age", ColumnType::Int).unwrap();
        let row = Row::filled(vec![ColumnValue::int(30)]);
        let row_view = RowView::new(row, &schema, &[0]);

        let literal = Literal::ColumnIndex(0);
        let value = row_view.resolve(&literal).unwrap();
        assert_eq!(value, ColumnValue::int(30));
    }
}

#[cfg(test)]
mod row_value_resolver_tests {
    use super::*;

    #[test]
    fn resolve_by_index() {
        let row = Row::filled(vec![ColumnValue::int(30)]);
        let literal = Literal::ColumnIndex(0);
        let value = row.resolve(&literal).unwrap();
        assert_eq!(value, ColumnValue::int(30));
    }

    #[test]
    fn resolve_unbound_error() {
        let row = Row::filled(vec![ColumnValue::int(30)]);
        let literal = Literal::ColumnReference("age".to_string());
        let result = row.resolve(&literal);
        assert!(matches!(result, Err(ExecutionError::UnboundColumn(_))));
    }
}

#[cfg(test)]
mod row_filter_tests {
    use super::*;

    #[test]
    fn predicate_matches_row() {
        let predicate = Predicate::comparison(
            Literal::ColumnIndex(0),
            LogicalOperator::Greater,
            Literal::Int(25),
        );
        let row = Row::filled(vec![ColumnValue::int(30)]);
        assert!(RowFilter::matches(&predicate, &row));
    }

    #[test]
    fn predicate_does_not_match_row() {
        let predicate = Predicate::comparison(
            Literal::ColumnIndex(0),
            LogicalOperator::Greater,
            Literal::Int(35),
        );
        let row = Row::filled(vec![ColumnValue::int(30)]);
        assert!(!RowFilter::matches(&predicate, &row));
    }

    #[test]
    fn predicate_unbound_column_returns_false() {
        let predicate = Predicate::comparison(
            Literal::ColumnReference("age".to_string()),
            LogicalOperator::Eq,
            Literal::Int(30),
        );
        let row = Row::filled(vec![ColumnValue::int(30)]);
        assert!(!RowFilter::matches(&predicate, &row));
    }
}

#[cfg(test)]
mod bind_tests {
    use super::*;
    use crate::types::column_type::ColumnType;
    use regex::Regex;

    #[test]
    fn bind_comparison() {
        let schema = crate::schema![
            "id" => ColumnType::Int,
            "name" => ColumnType::Text
        ]
        .unwrap();

        let predicate = Predicate::comparison(
            Literal::ColumnReference("name".to_string()),
            LogicalOperator::Eq,
            Literal::Text("Alice".to_string()),
        );

        let bound_predicate = predicate.bind(&schema).unwrap();

        let expected = Predicate::comparison(
            Literal::ColumnIndex(1),
            LogicalOperator::Eq,
            Literal::Text("Alice".to_string()),
        );

        assert_eq!(bound_predicate, expected);
    }

    #[test]
    fn bind_like() {
        let schema = crate::schema![
            "id" => ColumnType::Int,
            "name" => ColumnType::Text
        ]
        .unwrap();

        let regex = regex::Regex::new("^A").unwrap();
        let predicate = Predicate::like("name", regex.clone());

        let bound_predicate = predicate.bind(&schema).unwrap();

        let expected = Predicate::Single(LogicalClause::Like {
            column: Literal::ColumnIndex(1),
            regex: Regex::new("^A").unwrap(),
        });

        assert_eq!(bound_predicate, expected);
    }

    #[test]
    fn bind_and_or() {
        let schema = crate::schema![
            "id" => ColumnType::Int,
            "age" => ColumnType::Int
        ]
        .unwrap();

        let predicate = Predicate::or(vec![
            Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            ),
            Predicate::and(vec![Predicate::comparison(
                Literal::ColumnReference("age".to_string()),
                LogicalOperator::Greater,
                Literal::Int(18),
            )]),
        ]);

        let bound_predicate = predicate.bind(&schema).unwrap();

        let expected = Predicate::or(vec![
            Predicate::comparison(
                Literal::ColumnIndex(0),
                LogicalOperator::Eq,
                Literal::Int(1),
            ),
            Predicate::and(vec![Predicate::comparison(
                Literal::ColumnIndex(1),
                LogicalOperator::Greater,
                Literal::Int(18),
            )]),
        ]);

        assert_eq!(bound_predicate, expected);
    }

    #[test]
    fn split_single_clause_by_and() {
        let predicate = Predicate::comparison(
            Literal::ColumnReference("id".to_string()),
            LogicalOperator::Eq,
            Literal::Int(1),
        );

        let split_predicates = predicate.split_by_and();
        assert_eq!(1, split_predicates.len());
        assert_eq!(
            Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            ),
            split_predicates[0]
        );
    }

    #[test]
    fn split_an_or_clause_by_and() {
        let predicate = Predicate::or(vec![
            Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            ),
            Predicate::comparison(
                Literal::ColumnReference("age".to_string()),
                LogicalOperator::Greater,
                Literal::Int(18),
            ),
        ]);

        let split_predicates = predicate.split_by_and();
        assert_eq!(1, split_predicates.len());
    }

    #[test]
    fn split_and_clause_by_and_by_flattening() {
        let predicate = Predicate::and(vec![
            Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            ),
            Predicate::and(vec![
                Predicate::comparison(
                    Literal::ColumnReference("age".to_string()),
                    LogicalOperator::Greater,
                    Literal::Int(18),
                ),
                Predicate::comparison(
                    Literal::ColumnReference("role".to_string()),
                    LogicalOperator::Eq,
                    Literal::Text("admin".to_string()),
                ),
            ]),
        ]);

        let split_predicates = predicate.split_by_and();
        assert_eq!(3, split_predicates.len());
        assert_eq!(
            Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            ),
            split_predicates[0]
        );
        assert_eq!(
            Predicate::comparison(
                Literal::ColumnReference("age".to_string()),
                LogicalOperator::Greater,
                Literal::Int(18),
            ),
            split_predicates[1]
        );
    }

    #[test]
    fn bind_column_not_found() {
        let schema = crate::schema!["id" => ColumnType::Int].unwrap();

        let predicate = Predicate::comparison(
            Literal::ColumnReference("name".to_string()),
            LogicalOperator::Eq,
            Literal::Text("Alice".to_string()),
        );

        let result = predicate.bind(&schema);

        assert!(matches!(result, Err(PlanningError::ColumnNotFound(_))));
    }

    #[test]
    fn predicate_belongs_to_schema() {
        let schema = crate::schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap();

        let predicate = Predicate::and(vec![
            Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            ),
            Predicate::like("name", Regex::new("A.*").unwrap()),
        ]);

        assert!(predicate.belongs_to(&schema));
    }

    #[test]
    fn predicate_with_alias_belongs_to_schema() {
        let schema = crate::schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap();
        let schema = schema.with_prefix("e");

        let predicate = Predicate::and(vec![
            Predicate::comparison(
                Literal::ColumnReference("e.id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            ),
            Predicate::like("e.name", Regex::new("A.*").unwrap()),
        ]);

        assert!(predicate.belongs_to(&schema));
    }

    #[test]
    fn predicate_does_not_belong_to_schema() {
        let schema = crate::schema!["id" => ColumnType::Int].unwrap();

        let predicate = Predicate::and(vec![
            Predicate::comparison(
                Literal::ColumnReference("id".to_string()),
                LogicalOperator::Eq,
                Literal::Int(1),
            ),
            Predicate::comparison(
                Literal::ColumnReference("age".to_string()),
                LogicalOperator::Greater,
                Literal::Int(18),
            ),
        ]);

        assert!(!predicate.belongs_to(&schema));
    }
}
