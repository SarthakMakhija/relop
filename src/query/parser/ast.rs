use crate::query::lexer::token::{Token, TokenType};
use crate::query::parser::error::ParseError;
use crate::query::parser::ordering_key::OrderingKey;
use crate::query::parser::projection::Projection;

/// `Ast` represents the Abstract Syntax Tree for SQL statements.
#[derive(Debug)]
pub(crate) enum Ast {
    /// Represents a `SHOW TABLES` statement.
    ShowTables,
    /// Represents a `DESCRIBE TABLE` statement.
    DescribeTable {
        /// The name of the table to describe.
        table_name: String,
    },
    /// Represents a `SELECT` statement.
    Select {
        /// The source to select from (table or join).
        source: TableSource,
        /// The projection (columns or all) to select.
        projection: Projection,
        /// The WHERE filter criteria.
        where_clause: Option<WhereClause>,
        /// The ORDER BY clause, defining the columns and directions used to order rows.
        order_by: Option<Vec<OrderingKey>>,
        /// The LIMIT (max records) to return.
        limit: Option<usize>,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum TableSource {
    Table(String),
    Join {
        left: Box<TableSource>,
        right: Box<TableSource>,
        on: Expression,
    },
}

impl TableSource {
    pub(crate) fn table(name: &str) -> Self {
        TableSource::Table(name.to_string())
    }
}

/// `WhereClause` represents the filtering criteria in a SELECT statement.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct WhereClause(pub(crate) Expression);

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Expression {
    Single(Clause),
    And(Vec<Expression>),
    Or(Vec<Expression>),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Clause {
    /// A comparison expression (e.g., `id = 1`, `age > 25`).
    Comparison {
        /// The column name to compare.
        column_name: String,
        /// The comparison operator.
        operator: BinaryOperator,
        /// The literal value to compare against.
        literal: Literal,
    },
    /// A LIKE expression (e.g., `name like 'John%'`).
    Like {
        /// The column name to match.
        column_name: String,
        /// The literal pattern to match against (e.g., "John%").
        literal: Literal,
    },
}

impl Expression {
    /// Creates a new `Expression::Single` variant.
    pub fn single(clause: Clause) -> Self {
        Expression::Single(clause)
    }

    /// Creates a new `Expression::And` variant.
    pub fn and(expressions: Vec<Expression>) -> Self {
        Expression::And(expressions)
    }

    /// Creates a new `Expression::Or` variant.
    pub fn or(expressions: Vec<Expression>) -> Self {
        Expression::Or(expressions)
    }
}

impl Clause {
    /// Creates a new `Clause::Comparison` variant.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The name of the column to compare.
    /// * `operator` - The binary operator to use.
    /// * `literal` - The literal value to compare against.
    pub fn comparison(column_name: &str, operator: BinaryOperator, literal: Literal) -> Self {
        Clause::Comparison {
            column_name: column_name.to_string(),
            operator,
            literal,
        }
    }

    /// Creates a new `Clause::Like` variant.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The name of the column to match.
    /// * `literal` - The literal pattern to match against.
    pub fn like(column_name: &str, literal: Literal) -> Self {
        Clause::Like {
            column_name: column_name.to_string(),
            literal,
        }
    }
}

impl WhereClause {
    /// Creates a new `WhereClause` with an AND expression.
    pub fn and(expressions: Vec<Expression>) -> Self {
        WhereClause(Expression::and(expressions))
    }

    /// Creates a new `WhereClause` with an AND expression.
    pub fn or(expressions: Vec<Expression>) -> Self {
        WhereClause(Expression::or(expressions))
    }
}

#[cfg(test)]
impl WhereClause {
    /// Creates a new `WhereClause` with a comparison.
    pub fn comparison(column_name: &str, operator: BinaryOperator, literal: Literal) -> Self {
        WhereClause(Expression::single(Clause::comparison(
            column_name,
            operator,
            literal,
        )))
    }

    /// Creates a new `WhereClause` with a LIKE criteria.
    pub fn like(column_name: &str, literal: Literal) -> Self {
        WhereClause(Expression::single(Clause::like(column_name, literal)))
    }
}

/// `BinaryOperator` defines the binary operators supported in a WHERE clause.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum BinaryOperator {
    /// Equal to `=`.
    Eq,
    /// Greater than `>`.
    Greater,
    /// Greater than or equal to `>=`.
    GreaterEq,
    /// Less than `<`.
    Lesser,
    /// Less than or equal to `<=`.
    LesserEq,
    /// Not equal to `!=`.
    NotEq,
    /// Like
    Like,
}

impl BinaryOperator {
    /// Converts a `Token` into an `BinaryOperator`.
    ///
    /// Returns `Some(BinaryOperator)` if the token represents a valid binary operator,
    /// otherwise returns `None`.
    pub(crate) fn from_token(token: &Token) -> Result<Self, ParseError> {
        match token.token_type() {
            TokenType::Equal => Ok(BinaryOperator::Eq),
            TokenType::Greater => Ok(BinaryOperator::Greater),
            TokenType::GreaterEqual => Ok(BinaryOperator::GreaterEq),
            TokenType::Lesser => Ok(BinaryOperator::Lesser),
            TokenType::LesserEqual => Ok(BinaryOperator::LesserEq),
            TokenType::NotEqual => Ok(BinaryOperator::NotEq),
            _ if token.is_keyword("like") => Ok(BinaryOperator::Like),
            _ => Err(ParseError::UnexpectedToken {
                expected: "operator".to_string(),
                found: token.lexeme().to_string(),
            }),
        }
    }
}

/// `Literal` represents a concrete value used in expressions.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Literal {
    /// An integer literal.
    Int(i64),
    /// A text string literal.
    Text(String),
}

impl Literal {
    /// Converts a `Token` into a `Literal`.
    ///
    /// # Returns
    ///
    /// * `Ok(Literal::Text)` - If the token is a string literal.
    /// * `Ok(Literal::Int)` - If the token is a whole number.
    /// * `Err(ParseError::NumericLiteralOutOfRange)` - If the number is too large (should theoretically be handled by lexer, but good for safety).
    /// * `Err(ParseError::UnexpectedToken)` - If the token is not a literal.
    pub(crate) fn from_token(token: &Token) -> Result<Self, ParseError> {
        if token.is_string_literal() {
            return Ok(Literal::Text(token.lexeme().to_string()));
        }
        if token.is_a_whole_number() {
            let value = token
                .lexeme()
                .parse::<i64>()
                .map_err(|_| ParseError::NumericLiteralOutOfRange(token.lexeme().to_string()))?;

            return Ok(Literal::Int(value));
        }
        Err(ParseError::UnexpectedToken {
            expected: "literal".to_string(),
            found: token.lexeme().to_string(),
        })
    }
}

#[cfg(test)]
mod operator_tests {
    use crate::query::lexer::token::{Token, TokenType};
    use crate::query::parser::ast::BinaryOperator;
    use crate::query::parser::error::ParseError;

    #[test]
    fn from_token_equal() {
        let token = Token::equal();
        assert_eq!(BinaryOperator::from_token(&token), Ok(BinaryOperator::Eq));
    }

    #[test]
    fn from_token_greater() {
        let token = Token::greater();
        assert_eq!(
            BinaryOperator::from_token(&token),
            Ok(BinaryOperator::Greater)
        );
    }

    #[test]
    fn from_token_greater_equal() {
        let token = Token::greater_equal();
        assert_eq!(
            BinaryOperator::from_token(&token),
            Ok(BinaryOperator::GreaterEq)
        );
    }

    #[test]
    fn from_token_lesser() {
        let token = Token::lesser();
        assert_eq!(
            BinaryOperator::from_token(&token),
            Ok(BinaryOperator::Lesser)
        );
    }

    #[test]
    fn from_token_lesser_equal() {
        let token = Token::lesser_equal();
        assert_eq!(
            BinaryOperator::from_token(&token),
            Ok(BinaryOperator::LesserEq)
        );
    }

    #[test]
    fn from_token_not_equal() {
        let token = Token::not_equal();
        assert_eq!(
            BinaryOperator::from_token(&token),
            Ok(BinaryOperator::NotEq)
        );
    }

    #[test]
    fn from_token_like() {
        let token = Token::new("like", TokenType::Keyword);
        assert_eq!(BinaryOperator::from_token(&token), Ok(BinaryOperator::Like));
    }

    #[test]
    fn from_token_semicolon() {
        let token = Token::semicolon();
        let result = BinaryOperator::from_token(&token);

        assert!(matches!(
            result,
            Err(ParseError::UnexpectedToken { expected, found } ) if expected == "operator" && found == ";"
        ));
    }
}

#[cfg(test)]
mod literal_tests {
    use crate::query::lexer::token::{Token, TokenType};
    use crate::query::parser::ast::Literal;
    use crate::query::parser::error::ParseError;

    #[test]
    fn from_token_string_literal() {
        let token = Token::new("relop", TokenType::StringLiteral);
        let literal = Literal::from_token(&token).unwrap();
        assert!(matches!(literal, Literal::Text(text) if text == "relop"));
    }

    #[test]
    fn from_token_integer_literal() {
        let token = Token::new("42", TokenType::WholeNumber);
        let literal = Literal::from_token(&token).unwrap();
        assert!(matches!(literal, Literal::Int(val) if val == 42));
    }

    #[test]
    fn from_token_invalid_literal() {
        let token = Token::new("select", TokenType::Keyword);
        let result = Literal::from_token(&token);
        assert!(matches!(
            result,
            Err(ParseError::UnexpectedToken { expected, found }) if expected == "literal" && found == "select"
        ));
    }

    #[test]
    fn from_token_integer_literal_out_of_range() {
        let token = Token::new("9999999999999999999999", TokenType::WholeNumber);
        let result = Literal::from_token(&token);
        assert!(matches!(
            result,
            Err(ParseError::NumericLiteralOutOfRange(value)) if value == "9999999999999999999999"
        ));
    }
}
#[cfg(test)]
mod where_clause_tests {
    use crate::query::parser::ast::{BinaryOperator, Clause, Expression, Literal, WhereClause};

    #[test]
    fn create_comparison() {
        let where_clause =
            WhereClause::comparison("age", BinaryOperator::Greater, Literal::Int(25));

        assert_eq!(
            where_clause,
            WhereClause(Expression::single(Clause::Comparison {
                column_name: "age".to_string(),
                operator: BinaryOperator::Greater,
                literal: Literal::Int(25),
            }))
        );
    }

    #[test]
    fn create_like() {
        let where_clause = WhereClause::like("name", Literal::Text("John%".to_string()));

        assert_eq!(
            where_clause,
            WhereClause(Expression::single(Clause::Like {
                column_name: "name".to_string(),
                literal: Literal::Text("John%".to_string()),
            }))
        );
    }

    #[test]
    fn create_or() {
        let where_clause = WhereClause(Expression::or(vec![
            Expression::single(Clause::comparison(
                "age",
                BinaryOperator::Greater,
                Literal::Int(25),
            )),
            Expression::single(Clause::like("name", Literal::Text("John%".to_string()))),
        ]));

        assert_eq!(
            where_clause,
            WhereClause(Expression::or(vec![
                Expression::single(Clause::Comparison {
                    column_name: "age".to_string(),
                    operator: BinaryOperator::Greater,
                    literal: Literal::Int(25),
                }),
                Expression::single(Clause::Like {
                    column_name: "name".to_string(),
                    literal: Literal::Text("John%".to_string()),
                }),
            ]))
        );
    }
}

#[cfg(test)]
mod clause_tests {
    use crate::query::parser::ast::{BinaryOperator, Clause, Literal};

    #[test]
    fn create_comparison_clause() {
        let clause = Clause::comparison("age", BinaryOperator::Greater, Literal::Int(25));
        assert_eq!(
            clause,
            Clause::Comparison {
                column_name: "age".to_string(),
                operator: BinaryOperator::Greater,
                literal: Literal::Int(25),
            }
        );
    }

    #[test]
    fn create_like_clause() {
        let clause = Clause::like("name", Literal::Text("John%".to_string()));
        assert_eq!(
            clause,
            Clause::Like {
                column_name: "name".to_string(),
                literal: Literal::Text("John%".to_string()),
            }
        );
    }
}
