use crate::query::lexer::token::{Token, TokenType};
use crate::query::parser::error::ParseError;
use crate::query::parser::ordering_key::OrderingKey;
use crate::query::parser::projection::Projection;

/// `Ast` represents the Abstract Syntax Tree for SQL statements.
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
        /// The name of the table to select from.
        table_name: String,
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

/// `Where` represents the filtering criteria in a SELECT statement.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum WhereClause {
    /// A comparison expression (e.g., `id = 1`, `age > 25`).
    Comparison {
        /// The column name to compare.
        column_name: String,
        /// The comparison operator.
        operator: Operator,
        /// The literal value to compare against.
        literal: Literal,
    },
}

/// `Operator` defines the comparison operators supported in a WHERE clause.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Operator {
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
}

impl Operator {
    /// Converts a `Token` into an `Operator`.
    ///
    /// Returns `Some(Operator)` if the token represents a valid comparison operator,
    /// otherwise returns `None`.
    pub(crate) fn from_token(token: &Token) -> Result<Self, ParseError> {
        match token.token_type() {
            TokenType::Equal => Ok(Operator::Eq),
            TokenType::Greater => Ok(Operator::Greater),
            TokenType::GreaterEqual => Ok(Operator::GreaterEq),
            TokenType::Lesser => Ok(Operator::Lesser),
            TokenType::LesserEqual => Ok(Operator::LesserEq),
            TokenType::NotEqual => Ok(Operator::NotEq),
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
    use crate::query::lexer::token::Token;
    use crate::query::parser::ast::Operator;
    use crate::query::parser::error::ParseError;

    #[test]
    fn from_token_equal() {
        let token = Token::equal();
        assert_eq!(Operator::from_token(&token), Ok(Operator::Eq));
    }

    #[test]
    fn from_token_greater() {
        let token = Token::greater();
        assert_eq!(Operator::from_token(&token), Ok(Operator::Greater));
    }

    #[test]
    fn from_token_greater_equal() {
        let token = Token::greater_equal();
        assert_eq!(Operator::from_token(&token), Ok(Operator::GreaterEq));
    }

    #[test]
    fn from_token_lesser() {
        let token = Token::lesser();
        assert_eq!(Operator::from_token(&token), Ok(Operator::Lesser));
    }

    #[test]
    fn from_token_lesser_equal() {
        let token = Token::lesser_equal();
        assert_eq!(Operator::from_token(&token), Ok(Operator::LesserEq));
    }

    #[test]
    fn from_token_not_equal() {
        let token = Token::not_equal();
        assert_eq!(Operator::from_token(&token), Ok(Operator::NotEq));
    }

    #[test]
    fn from_token_semicolon() {
        let token = Token::semicolon();
        let result = Operator::from_token(&token);

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
