pub(crate) mod ast;
pub mod error;
pub(crate) mod ordering_key;
pub(crate) mod projection;

use crate::query::lexer::token::{Token, TokenStream, TokenType};
use crate::query::lexer::token_cursor::TokenCursor;
use crate::query::parser::ast::{Ast, BinaryOperator, Clause, Expression, Literal, WhereClause};
use crate::query::parser::error::ParseError;
use crate::query::parser::ordering_key::{OrderingDirection, OrderingKey};
use crate::query::parser::projection::Projection;

/// `Parser` is responsible for parsing a stream of tokens into an Abstract Syntax Tree (AST).
pub(crate) struct Parser {
    cursor: TokenCursor,
}

impl Parser {
    /// Creates a new `Parser` from a `TokenStream`.
    pub(crate) fn new(stream: TokenStream) -> Parser {
        Self {
            cursor: stream.cursor(),
        }
    }

    /// Parses the token stream into an `Ast`.
    ///
    /// The grammar is available in `docs/grammar.ebnf`.
    ///
    /// # Returns
    ///
    /// * `Ok(Ast)` - The parsed Abstract Syntax Tree.
    /// * `Err(ParseError)` - If a syntax error is encountered.
    pub(crate) fn parse(&mut self) -> Result<Ast, ParseError> {
        let ast = self.parse_statement()?;
        self.expect_end_of_stream()?;
        Ok(ast)
    }

    fn parse_statement(&mut self) -> Result<Ast, ParseError> {
        match self.cursor.peek() {
            Some(token) => {
                if token.matches(TokenType::Keyword, "show") {
                    self.parse_show_tables()
                } else if token.matches(TokenType::Keyword, "describe") {
                    self.parse_describe_table()
                } else if token.matches(TokenType::Keyword, "select") {
                    self.parse_select()
                } else {
                    Err(ParseError::UnsupportedToken {
                        expected: "show | describe | select".to_string(),
                        found: token.lexeme().to_string(),
                    })
                }
            }
            None => Err(ParseError::NoTokens),
        }
    }

    fn parse_show_tables(&mut self) -> Result<Ast, ParseError> {
        self.expect_keyword("show")?;
        self.expect_keyword("tables")?;
        let _ = self.eat_if(|token| token.is_semicolon());

        Ok(Ast::ShowTables)
    }

    fn parse_describe_table(&mut self) -> Result<Ast, ParseError> {
        self.expect_keyword("describe")?;
        self.expect_keyword("table")?;
        let table_name = self.expect_identifier()?;
        let _ = self.eat_if(|token| token.is_semicolon());

        Ok(Ast::DescribeTable {
            table_name: table_name.to_string(),
        })
    }

    fn parse_select(&mut self) -> Result<Ast, ParseError> {
        self.expect_keyword("select")?;
        let projection = self.expect_projection()?;
        self.expect_keyword("from")?;
        let source = self.expect_table_source()?;
        let where_clause = self.maybe_where_clause()?;
        let order_by = self.maybe_order_by()?;
        let limit = self.maybe_limit()?;
        let _ = self.eat_if(|token| token.is_semicolon());

        Ok(Ast::Select {
            source,
            projection,
            where_clause,
            order_by,
            limit,
        })
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<(), ParseError> {
        match self.cursor.next() {
            Some(token) if token.matches(TokenType::Keyword, keyword) => Ok(()),
            Some(token) => Err(ParseError::UnexpectedToken {
                expected: keyword.to_string(),
                found: token.lexeme().to_string(),
            }),
            None => Err(ParseError::UnexpectedEndOfInput),
        }
    }

    fn expect_identifier(&mut self) -> Result<String, ParseError> {
        match self.cursor.next() {
            Some(token) if token.is_identifier() => Ok(token.lexeme().to_string()),
            Some(token) => Err(ParseError::UnexpectedToken {
                expected: "identifier".to_string(),
                found: token.lexeme().to_string(),
            }),
            None => Err(ParseError::UnexpectedEndOfInput),
        }
    }

    fn expect_projection(&mut self) -> Result<Projection, ParseError> {
        if self.eat_if(|token| token.is_star()) {
            return Ok(Projection::All);
        }
        let columns = self.expect_columns()?;
        Ok(Projection::Columns(columns))
    }

    fn expect_columns(&mut self) -> Result<Vec<String>, ParseError> {
        let mut columns = Vec::new();

        let first = match self.cursor.next() {
            Some(token) if token.is_identifier() => token.lexeme().to_string(),
            Some(token) => {
                return Err(ParseError::UnexpectedToken {
                    expected: "identifier".to_string(),
                    found: token.lexeme().to_string(),
                });
            }
            None => return Err(ParseError::UnexpectedEndOfInput),
        };
        columns.push(first);

        while self.eat_if(|token| token.is_comma()) {
            let column = self.expect_identifier()?;
            columns.push(column);
        }
        Ok(columns)
    }

    fn expect_table_source(&mut self) -> Result<ast::TableSource, ParseError> {
        let left_table = self.expect_identifier()?;
        let mut source = ast::TableSource::table(&left_table);

        while self.eat_if(|token| token.is_keyword("join")) {
            let right_table = self.expect_identifier()?;
            let mut on = None;

            if self.eat_if(|token| token.is_keyword("on")) {
                let on_expressions = self.expect_clauses()?;
                if on_expressions.len() == 1 {
                    on = Some(on_expressions.into_iter().next().unwrap());
                } else {
                    on = Some(Expression::And(on_expressions));
                }
            }
            source = ast::TableSource::Join {
                left: Box::new(source),
                right: Box::new(ast::TableSource::table(&right_table)),
                on,
            };
        }
        Ok(source)
    }

    fn maybe_where_clause(&mut self) -> Result<Option<WhereClause>, ParseError> {
        let is_where_clause = self.eat_if(|token| token.is_keyword("where"));
        if is_where_clause {
            let clauses = self.expect_clauses()?;
            return if clauses.len() == 1 {
                Ok(Some(WhereClause(clauses.into_iter().next().unwrap())))
            } else {
                Ok(Some(WhereClause::and(clauses)))
            };
        }
        Ok(None)
    }

    fn expect_clauses(&mut self) -> Result<Vec<Expression>, ParseError> {
        let clause = self.expect_clause()?;
        let mut expressions = Vec::new();
        expressions.push(Expression::single(clause));

        while self.eat_if(|token| token.matches(TokenType::Keyword, "and")) {
            let clause = self.expect_clause()?;
            expressions.push(Expression::single(clause));
        }
        Ok(expressions)
    }

    fn expect_clause(&mut self) -> Result<Clause, ParseError> {
        let lhs = self.expect_literal()?;
        let operator = self.expect_operator()?;

        match operator {
            BinaryOperator::Like => {
                if let Literal::ColumnReference(column_name) = lhs {
                    let rhs = self.expect_literal()?;
                    Ok(Clause::like(&column_name, rhs))
                } else {
                    Err(ParseError::UnexpectedToken {
                        expected: "column name".to_string(),
                        found: format!("{:?}", lhs),
                    })
                }
            }
            _ => {
                let rhs = self.expect_literal()?;
                Ok(Clause::comparison(lhs, operator, rhs))
            }
        }
    }

    fn expect_operator(&mut self) -> Result<BinaryOperator, ParseError> {
        match self.cursor.next() {
            Some(token) => BinaryOperator::from_token(token),
            None => Err(ParseError::UnexpectedEndOfInput),
        }
    }

    fn expect_literal(&mut self) -> Result<Literal, ParseError> {
        match self.cursor.next() {
            Some(token) => Literal::from_token(token),
            None => Err(ParseError::UnexpectedEndOfInput),
        }
    }

    fn maybe_order_by(&mut self) -> Result<Option<Vec<OrderingKey>>, ParseError> {
        let is_order = self.eat_if(|token| token.is_keyword("order"));
        if is_order {
            let mut ordering_keys = Vec::new();
            self.expect_keyword("by")?;

            let ordering_key = self.expect_ordering_key()?;
            ordering_keys.push(ordering_key);

            while self.eat_if(|token| token.is_comma()) {
                let ordering_key = self.expect_ordering_key()?;
                ordering_keys.push(ordering_key);
            }
            return Ok(Some(ordering_keys));
        }
        Ok(None)
    }

    fn expect_ordering_key(&mut self) -> Result<OrderingKey, ParseError> {
        let column_name = self.expect_identifier()?;
        Ok(OrderingKey::new(column_name, self.ordering_direction()))
    }

    fn ordering_direction(&mut self) -> OrderingDirection {
        if self.eat_if(|token| token.is_keyword("asc")) {
            OrderingDirection::Ascending
        } else if self.eat_if(|token| token.is_keyword("desc")) {
            OrderingDirection::Descending
        } else {
            OrderingDirection::Ascending
        }
    }

    fn maybe_limit(&mut self) -> Result<Option<usize>, ParseError> {
        let is_limit_clause = self.eat_if(|token| token.is_keyword("limit"));
        if is_limit_clause {
            let limit_value = self.expect_whole_number()?;
            let value = limit_value
                .parse::<usize>()
                .map_err(|_| ParseError::LimitOutOfRange(limit_value))?;

            if value == 0 {
                return Err(ParseError::ZeroLimit);
            }
            return Ok(Some(value));
        }
        Ok(None)
    }

    fn expect_whole_number(&mut self) -> Result<String, ParseError> {
        match self.cursor.next() {
            Some(token) if token.is_a_whole_number() => Ok(token.lexeme().to_string()),
            Some(_token) => Err(ParseError::NoLimitValue),
            None => Err(ParseError::UnexpectedEndOfInput),
        }
    }

    fn eat_if<F: Fn(&Token) -> bool>(&mut self, predicate: F) -> bool {
        if let Some(token) = self.cursor.peek() {
            if predicate(token) {
                self.cursor.next();
                return true;
            }
        }
        false
    }

    fn expect_end_of_stream(&mut self) -> Result<(), ParseError> {
        match self.cursor.next() {
            Some(token) if token.is_end_of_stream() => Ok(()),
            Some(token) => Err(ParseError::UnexpectedToken {
                expected: "end of stream".to_string(),
                found: token.lexeme().to_string(),
            }),
            None => Err(ParseError::UnexpectedEndOfInput),
        }
    }
}

#[cfg(test)]
mod show_tables_tests {
    use super::*;
    use crate::query::lexer::token::Token;

    #[test]
    fn parse_show_tables() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("show", TokenType::Keyword));
        stream.add(Token::new("tables", TokenType::Keyword));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(matches!(ast, Ast::ShowTables));
    }

    #[test]
    fn parse_show_tables_with_semicolon() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("show", TokenType::Keyword));
        stream.add(Token::new("tables", TokenType::Keyword));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(matches!(ast, Ast::ShowTables));
    }

    #[test]
    fn attempt_to_parse_with_no_tokens() {
        let stream = TokenStream::new();

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::NoTokens)));
    }

    #[test]
    fn attempt_to_parse_with_unsupported_token() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("unsupported", TokenType::Keyword));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnsupportedToken {expected, found}) if expected == "show | describe | select" && found == "unsupported")
        );
    }

    #[test]
    fn attempt_to_parse_invalid_show_tables() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("show", TokenType::Keyword));
        stream.add(Token::new("invalid", TokenType::Keyword));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "tables" && found == "invalid" )
        );
    }

    #[test]
    fn attempt_to_parse_invalid_show_tables_with_no_token_after_show() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("show", TokenType::Keyword));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::UnexpectedEndOfInput)));
    }

    #[test]
    fn attempt_to_parse_invalid_show_tables_with_end_of_stream_token_after_show() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("show", TokenType::Keyword));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "tables" && found.is_empty())
        );
    }

    #[test]
    fn attempt_to_parse_with_missing_end_of_stream_token() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("show", TokenType::Keyword));
        stream.add(Token::new("tables", TokenType::Keyword));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::UnexpectedEndOfInput)));
    }

    #[test]
    fn attempt_to_parse_with_another_token_instead_of_end_of_stream_token() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("show", TokenType::Keyword));
        stream.add(Token::new("tables", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "end of stream" && found == "employees")
        );
    }

    #[test]
    fn attempt_to_parse_with_another_token_instead_of_end_of_stream_token_with_semicolon() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("show", TokenType::Keyword));
        stream.add(Token::new("tables", TokenType::Keyword));
        stream.add(Token::semicolon());
        stream.add(Token::new("employees", TokenType::Identifier));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "end of stream" && found == "employees")
        );
    }
}

#[cfg(test)]
mod describe_table_tests {
    use super::*;
    use crate::query::lexer::token::Token;

    #[test]
    fn parse_describe_table() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("describe", TokenType::Keyword));
        stream.add(Token::new("table", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(matches!(ast, Ast::DescribeTable { table_name } if table_name == "employees"));
    }

    #[test]
    fn parse_describe_table_with_semicolon() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("describe", TokenType::Keyword));
        stream.add(Token::new("table", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(matches!(ast, Ast::DescribeTable { table_name } if table_name == "employees"));
    }

    #[test]
    fn attempt_to_parse_with_no_tokens() {
        let stream = TokenStream::new();

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::NoTokens)));
    }

    #[test]
    fn attempt_to_parse_invalid_describe_table() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("describe", TokenType::Keyword));
        stream.add(Token::new("invalid", TokenType::Keyword));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "table" && found == "invalid" )
        );
    }

    #[test]
    fn attempt_to_parse_invalid_describe_table_with_no_token_after_describe() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("describe", TokenType::Keyword));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::UnexpectedEndOfInput)));
    }

    #[test]
    fn attempt_to_parse_invalid_describe_table_with_end_of_stream_token_after_describe() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("describe", TokenType::Keyword));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "table" && found.is_empty())
        );
    }

    #[test]
    fn attempt_to_parse_with_missing_end_of_stream_token() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("describe", TokenType::Keyword));
        stream.add(Token::new("table", TokenType::Keyword));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::UnexpectedEndOfInput)));
    }

    #[test]
    fn attempt_to_parse_with_another_token_instead_of_end_of_stream_token() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("describe", TokenType::Keyword));
        stream.add(Token::new("table", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("invalid", TokenType::Identifier));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "end of stream" && found == "invalid")
        );
    }

    #[test]
    fn attempt_to_parse_with_another_keyword_token_instead_of_identifier() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("describe", TokenType::Keyword));
        stream.add(Token::new("table", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("select", TokenType::Keyword));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "end of stream" && found == "select")
        );
    }

    #[test]
    fn attempt_to_parse_with_another_token_instead_of_end_of_stream_token_with_semicolon() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("describe", TokenType::Keyword));
        stream.add(Token::new("table", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::semicolon());
        stream.add(Token::new("invalid", TokenType::Identifier));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "end of stream" && found == "invalid")
        );
    }
}

#[cfg(test)]
mod select_star_tests {
    use super::*;
    use crate::query::lexer::token::Token;

    #[test]
    fn parse_select_star() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(
            matches!(ast, Ast::Select { source, projection, .. } if source == ast::TableSource::table("employees") && projection == Projection::All)
        );
    }

    #[test]
    fn parse_select_star_with_semicolon() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(
            matches!(ast, Ast::Select { source, projection, .. } if source == ast::TableSource::table("employees") && projection == Projection::All)
        );
    }

    #[test]
    fn attempt_to_parse_with_no_tokens() {
        let stream = TokenStream::new();

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::NoTokens)));
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_missing_star() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "identifier" && found == "from" )
        );
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_no_token_after_select() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::UnexpectedEndOfInput)));
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_missing_from() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("employees", TokenType::Keyword));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "from" && found == "employees" )
        );
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_no_tokens_after_star() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::UnexpectedEndOfInput)));
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_invalid_token_after_from() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "identifier" && found == "*" )
        );
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_no_tokens_after_from() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::UnexpectedEndOfInput)));
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_invalid_tokens_after_semicolon() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::semicolon());
        stream.add(Token::new("invalid", TokenType::Identifier));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "end of stream" && found == "invalid")
        );
    }
}

#[cfg(test)]
mod select_projection_tests {
    use super::*;
    use crate::query::lexer::token::Token;

    #[test]
    fn parse_select_projection() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new(",", TokenType::Comma));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(matches!(ast, Ast::Select { source, projection, .. }
                if source == ast::TableSource::table("employees") && projection == Projection::Columns(vec!["name".to_string(), "id".to_string()])));
    }

    #[test]
    fn parse_select_projection_with_single_column() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(matches!(ast, Ast::Select { source, projection, .. }
                if source == ast::TableSource::table("employees") && projection == Projection::Columns(vec!["name".to_string()])));
    }

    #[test]
    fn parse_select_projection_with_semicolon() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new(",", TokenType::Comma));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(matches!(ast, Ast::Select { source, projection, .. }
                if source == ast::TableSource::table("employees") && projection == Projection::Columns(vec!["name".to_string(), "id".to_string()])));
    }

    #[test]
    fn attempt_to_parse_invalid_select_projection_with_missing_comma() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "from" && found == "id" )
        );
    }

    #[test]
    fn attempt_to_parse_with_no_tokens() {
        let stream = TokenStream::new();

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::NoTokens)));
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_missing_projection() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "identifier" && found == "from" )
        );
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_no_token_after_select() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::UnexpectedEndOfInput)));
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_missing_from() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("employees", TokenType::Keyword));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "from" && found == "employees" )
        );
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_no_tokens_after_projection() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::UnexpectedEndOfInput)));
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_invalid_token_after_from() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "identifier" && found == "*" )
        );
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_no_tokens_after_from() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::UnexpectedEndOfInput)));
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_invalid_tokens_after_semicolon() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::semicolon());
        stream.add(Token::new("invalid", TokenType::Identifier));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "end of stream" && found == "invalid")
        );
    }
}

#[cfg(test)]
mod select_where_with_single_comparison_tests {
    use super::*;
    use crate::query::lexer::token::Token;

    #[test]
    fn parse_select_with_where_single_comparison() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("where", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("=", TokenType::Equal));
        stream.add(Token::new("relop", TokenType::StringLiteral));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(
            matches!(ast, Ast::Select { source, projection, where_clause, .. }
                if source == ast::TableSource::table("employees") &&
                    projection == Projection::All &&
                    matches!(&where_clause, Some(ref wc)
                        if *wc == WhereClause::comparison(
                            Literal::ColumnReference("name".to_string()),
                            BinaryOperator::Eq,
                            Literal::Text("relop".to_string())
                        )
                    )
            )
        );
    }

    #[test]
    fn parse_select_with_where_single_comparison_and_semicolon() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("where", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("like", TokenType::Keyword));
        stream.add(Token::new("rel%", TokenType::StringLiteral));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(
            matches!(ast, Ast::Select { source, projection, where_clause, .. }
                if source == ast::TableSource::table("employees") &&
                    projection == Projection::All &&
                    matches!(&where_clause, Some(ref wc)
                         if *wc == WhereClause::like(
                             "name",
                             Literal::Text("rel%".to_string())
                         )
                    )
            )
        );
    }

    #[test]
    fn attempt_to_parse_with_no_tokens() {
        let stream = TokenStream::new();

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::NoTokens)));
    }

    #[test]
    fn attempt_to_parse_select_with_where_but_missing_identifier_after_where() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("where", TokenType::Keyword));
        stream.add(Token::new("=", TokenType::Equal));
        stream.add(Token::new("relop", TokenType::StringLiteral));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(
            result,
            Err(ParseError::UnexpectedToken {
                expected,
                found,
            }) if expected == "identifier" && found == "=" ));
    }

    #[test]
    fn attempt_to_parse_select_with_where_but_no_tokens_after_where() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("where", TokenType::Keyword));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::UnexpectedEndOfInput)));
    }

    #[test]
    fn attempt_to_parse_select_with_where_but_missing_operator() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("where", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("relop", TokenType::StringLiteral));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(
            result,
            Err(ParseError::UnexpectedToken {
                expected,
                found,
            }) if expected == "operator" && found == "relop" ));
    }

    #[test]
    fn attempt_to_parse_select_with_where_but_no_tokens_after_where_column_name() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("where", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::UnexpectedEndOfInput)));
    }

    #[test]
    fn attempt_to_parse_select_with_where_but_missing_literal() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("where", TokenType::Keyword));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new(">", TokenType::Greater));
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(
            result,
            Err(ParseError::UnexpectedToken {
                expected,
                found,
            }) if expected == "identifier" && found == "select" ));
    }

    #[test]
    fn attempt_to_parse_select_with_where_but_literal_out_of_range() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("where", TokenType::Keyword));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new(">", TokenType::Greater));
        stream.add(Token::new("999999999999999999999", TokenType::WholeNumber));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(
            result,
            Err(ParseError::NumericLiteralOutOfRange(value)) if value == "999999999999999999999" ));
    }

    #[test]
    fn attempt_to_parse_select_with_where_but_no_tokens_after_operator() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("where", TokenType::Keyword));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new(">", TokenType::Greater));

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::UnexpectedEndOfInput)));
    }
}

#[cfg(test)]
mod select_where_with_and_tests {
    use super::*;
    use crate::query::lexer::token::Token;

    #[test]
    fn parse_select_with_where_with_and_comparison() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("where", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("=", TokenType::Equal));
        stream.add(Token::new("relop", TokenType::StringLiteral));
        stream.add(Token::new("and", TokenType::Keyword));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new("=", TokenType::Equal));
        stream.add(Token::new("2", TokenType::WholeNumber));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(
            matches!(ast, Ast::Select { source, projection, where_clause, .. }
                if source == ast::TableSource::table("employees") &&
                    projection == Projection::All &&
                    matches!(&where_clause, Some(WhereClause(Expression::And(expressions)))
                        if expressions.len() == 2 &&
                        expressions[0] == Expression::single(Clause::comparison(
                            Literal::ColumnReference("name".to_string()),
                            BinaryOperator::Eq,
                            Literal::Text("relop".to_string())
                        )) &&
                        expressions[1] == Expression::single(Clause::comparison(
                            Literal::ColumnReference("id".to_string()),
                            BinaryOperator::Eq,
                            Literal::Int(2)
                        ))
                    )
            )
        );
    }

    #[test]
    fn parse_select_with_where_with_and_comparison_involving_like() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("where", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("like", TokenType::Keyword));
        stream.add(Token::new("rel%", TokenType::StringLiteral));
        stream.add(Token::new("and", TokenType::Keyword));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new("=", TokenType::Equal));
        stream.add(Token::new("2", TokenType::WholeNumber));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(
            matches!(ast, Ast::Select { source, projection, where_clause, .. }
                if source == ast::TableSource::table("employees") &&
                    projection == Projection::All &&
                    matches!(&where_clause, Some(WhereClause(Expression::And(expressions)))
                        if expressions.len() == 2 &&
                        expressions[0] == Expression::single(Clause::like(
                            "name",
                            Literal::Text("rel%".to_string())
                        )) &&
                        expressions[1] == Expression::single(Clause::comparison(
                            Literal::ColumnReference("id".to_string()),
                            BinaryOperator::Eq,
                            Literal::Int(2)
                        ))
                    )
            )
        );
    }

    #[test]
    fn attempt_to_parse_select_with_where_with_invalid_like() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("where", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("like", TokenType::Keyword));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(
            result,
            Err(ParseError::UnexpectedToken { expected, found }) if expected == "identifier" && found == ";"
        ));
    }

    #[test]
    fn attempt_to_parse_select_with_where_with_like_having_no_column_name() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("where", TokenType::Keyword));
        stream.add(Token::new("1", TokenType::WholeNumber));
        stream.add(Token::new("like", TokenType::Keyword));
        stream.add(Token::new("rel%", TokenType::StringLiteral));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(
            result,
            Err(ParseError::UnexpectedToken { expected, found }) if expected == "column name" && found == "Int(1)"
        ));
    }

    #[test]
    fn attempt_to_parse_select_with_no_clause_after_and() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("where", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("like", TokenType::Keyword));
        stream.add(Token::new("rel%", TokenType::StringLiteral));
        stream.add(Token::new("and", TokenType::Keyword));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(
            result,
            Err(ParseError::UnexpectedToken {expected, found}) if expected == "identifier" && found == ";"
        ))
    }
}

#[cfg(test)]
mod select_order_by_tests {
    use super::*;
    use crate::query::lexer::token::Token;
    use crate::{asc, desc};

    #[test]
    fn parse_select_with_order_by_ascending() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("order", TokenType::Keyword));
        stream.add(Token::new("by", TokenType::Keyword));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(
            matches!(ast, Ast::Select { source, projection, order_by, .. }
                    if source == ast::TableSource::table("employees")
                        && projection == Projection::Columns(vec!["id".to_string()])
                        && order_by == Some(vec![asc!("id")])
            )
        )
    }

    #[test]
    fn parse_select_with_order_by_descending() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("order", TokenType::Keyword));
        stream.add(Token::new("by", TokenType::Keyword));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new("desc", TokenType::Keyword));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(
            matches!(ast, Ast::Select { source, projection, order_by, .. }
                    if source == ast::TableSource::table("employees")
                        && projection == Projection::Columns(vec!["id".to_string()])
                        && order_by == Some(vec![desc!("id")])
            )
        )
    }

    #[test]
    fn parse_select_with_order_by_ascending_with_semicolon() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("order", TokenType::Keyword));
        stream.add(Token::new("by", TokenType::Keyword));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new("asc", TokenType::Keyword));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(
            matches!(ast, Ast::Select { source, projection, order_by, .. }
                    if source == ast::TableSource::table("employees")
                        && projection == Projection::Columns(vec!["id".to_string()])
                        && order_by == Some(vec![asc!("id")])
            )
        )
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_missing_comma_between_order_by_columns() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("order", TokenType::Keyword));
        stream.add(Token::new("by", TokenType::Keyword));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "end of stream" && found == "name" )
        );
    }

    #[test]
    fn attempt_to_parse_with_no_tokens() {
        let stream = TokenStream::new();

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::NoTokens)));
    }

    #[test]
    fn attempt_to_parse_invalid_select_with_missing_by_after_order() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("order", TokenType::Keyword));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken{expected, found}) if expected == "by" && found == "id" )
        );
    }
}

#[cfg(test)]
mod select_tests_with_limit {
    use super::*;
    use crate::query::lexer::token::Token;

    #[test]
    fn parse_select_with_limit() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new(",", TokenType::Comma));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("limit", TokenType::Keyword));
        stream.add(Token::new("10", TokenType::WholeNumber));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(matches!(ast,
            Ast::Select { source, projection, where_clause: _, order_by: _, limit }
                if source == ast::TableSource::table("employees")
                    && projection == Projection::Columns(vec!["name".to_string(), "id".to_string()])
                    && limit == Some(10)
        ));
    }

    #[test]
    fn parse_select_with_limit_and_semicolon() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new(",", TokenType::Comma));
        stream.add(Token::new("id", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("limit", TokenType::Keyword));
        stream.add(Token::new("10", TokenType::WholeNumber));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(matches!(ast,
            Ast::Select { source, projection, where_clause: _, order_by: _, limit }
                if source == ast::TableSource::table("employees")
                    && projection == Projection::Columns(vec!["name".to_string(), "id".to_string()])
                    && limit == Some(10)
        ));
    }

    #[test]
    fn attempt_to_parse_with_no_tokens() {
        let stream = TokenStream::new();

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(result, Err(ParseError::NoTokens)));
    }

    #[test]
    fn attempt_to_parse_select_with_limit_without_limit_value() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("limit", TokenType::Keyword));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);

        let result = parser.parse();
        assert!(matches!(result, Err(ParseError::NoLimitValue)));
    }

    #[test]
    fn attempt_to_parse_select_with_zero_limit_value() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("limit", TokenType::Keyword));
        stream.add(Token::new("0", TokenType::WholeNumber));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);

        let result = parser.parse();
        assert!(matches!(result, Err(ParseError::ZeroLimit)));
    }

    #[test]
    fn attempt_to_parse_select_with_limit_value_out_of_range() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("limit", TokenType::Keyword));
        stream.add(Token::new("99999999999999999999", TokenType::WholeNumber));
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);

        let result = parser.parse();
        assert!(
            matches!(result, Err(ParseError::LimitOutOfRange(value)) if value == "99999999999999999999")
        );
    }

    #[test]
    fn attempt_to_parse_select_with_no_tokens_after_limit() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("name", TokenType::Identifier));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("limit", TokenType::Keyword));

        let mut parser = Parser::new(stream);

        let result = parser.parse();
        assert!(matches!(result, Err(ParseError::UnexpectedEndOfInput)));
    }
}

#[cfg(test)]
mod column_reference_tests {
    use super::*;
    use crate::query::lexer::token::Token;
    use crate::query::parser::ast::{Ast, BinaryOperator, Clause, Expression, Literal};

    #[test]
    fn parse_select_with_column_to_column_comparison() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("where", TokenType::Keyword));
        stream.add(Token::new("first_name", TokenType::Identifier));
        stream.add(Token::equal());
        stream.add(Token::new("last_name", TokenType::Identifier));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(
            matches!(ast, Ast::Select { ref source, ref where_clause, .. }
                if matches!(source, ast::TableSource::Table(ref name) if name == "employees")
                && matches!(where_clause, Some(WhereClause(Expression::Single(Clause::Comparison { ref lhs, ref operator, ref rhs })))
                    if matches!(lhs, Literal::ColumnReference(ref name) if name == "first_name")
                    && *operator == BinaryOperator::Eq
                    && matches!(rhs, Literal::ColumnReference(ref name) if name == "last_name")
                )
            )
        );
    }
}

#[cfg(test)]
mod select_join_tests {
    use super::*;
    use crate::query::lexer::token::Token;
    use crate::query::parser::ast::{
        Ast, BinaryOperator, Clause, Expression, Literal, TableSource,
    };

    #[test]
    fn parse_select_with_join() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("join", TokenType::Keyword));
        stream.add(Token::new("departments", TokenType::Identifier));
        stream.add(Token::new("on", TokenType::Keyword));
        stream.add(Token::new("employees.id", TokenType::Identifier));
        stream.add(Token::equal());
        stream.add(Token::new("departments.employee_id", TokenType::Identifier));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(matches!(
            ast,
            Ast::Select { ref source, .. }
            if matches!(
                source,
                TableSource::Join { left, right, on }
                if matches!(left.as_ref(), TableSource::Table(name) if name == "employees")
                && matches!(right.as_ref(), TableSource::Table(name) if name == "departments")
                && matches!(
                    on,
                    Some(Expression::Single(Clause::Comparison { lhs, operator, rhs }))
                    if matches!(lhs, Literal::ColumnReference(column_name) if column_name == "employees.id")
                    && *operator == BinaryOperator::Eq
                    && matches!(rhs, Literal::ColumnReference(column_name) if column_name == "departments.employee_id")
                )
            )
        ));
    }

    #[test]
    fn parse_select_with_join_multiple_conditions_in_on() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("join", TokenType::Keyword));
        stream.add(Token::new("departments", TokenType::Identifier));
        stream.add(Token::new("on", TokenType::Keyword));
        stream.add(Token::new("employee_id", TokenType::Identifier));
        stream.add(Token::equal());
        stream.add(Token::new("department_id", TokenType::Identifier));
        stream.add(Token::new("and", TokenType::Keyword));
        stream.add(Token::new("status", TokenType::Identifier));
        stream.add(Token::equal());
        stream.add(Token::new("ACTIVE", TokenType::StringLiteral));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(matches!(
            ast,
            Ast::Select { ref source, .. }
            if matches!(
                source,
                TableSource::Join { left, right, on }
                if matches!(left.as_ref(), TableSource::Table(name) if name == "employees")
                && matches!(right.as_ref(), TableSource::Table(name) if name == "departments")
                && matches!(
                    on,
                    Some(Expression::And(expressions))
                    if expressions.len() == 2
                    && matches!(
                        &expressions[0],
                        Expression::Single(Clause::Comparison { lhs, operator, rhs })
                        if matches!(lhs, Literal::ColumnReference(column_name) if column_name == "employee_id")
                        && *operator == BinaryOperator::Eq
                        && matches!(rhs, Literal::ColumnReference(column_name) if column_name == "department_id")
                    )
                    && matches!(
                        &expressions[1],
                        Expression::Single(Clause::Comparison { lhs, operator, rhs })
                        if matches!(lhs, Literal::ColumnReference(column_name) if column_name == "status")
                        && *operator == BinaryOperator::Eq
                        && matches!(rhs, Literal::Text(column_name) if column_name == "ACTIVE")
                    )
                )
            )
        ));
    }

    #[test]
    fn parse_select_with_join_but_no_on_clause() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("join", TokenType::Keyword));
        stream.add(Token::new("departments", TokenType::Identifier));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(matches!(
            ast,
            Ast::Select { ref source, .. }
            if matches!(
                source,
                TableSource::Join { left, right, on }
                if matches!(left.as_ref(), TableSource::Table(name) if name == "employees")
                && matches!(right.as_ref(), TableSource::Table(name) if name == "departments")
                && on.is_none()
            )
        ));
    }

    #[test]
    fn attempt_to_parse_select_with_join_but_missing_right_table() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("join", TokenType::Keyword));
        stream.add(Token::new("on", TokenType::Keyword));
        stream.add(Token::new("employee_id", TokenType::Identifier));
        stream.add(Token::equal());
        stream.add(Token::new("department_id", TokenType::Identifier));
        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let result = parser.parse();

        assert!(matches!(
            result,
            Err(ParseError::UnexpectedToken { expected, found })
            if expected == "identifier" && found == "on"
        ));
    }

    #[test]
    fn parse_select_with_multiple_joins() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("select", TokenType::Keyword));
        stream.add(Token::new("*", TokenType::Star));
        stream.add(Token::new("from", TokenType::Keyword));
        stream.add(Token::new("employees", TokenType::Identifier));
        stream.add(Token::new("join", TokenType::Keyword));
        stream.add(Token::new("departments", TokenType::Identifier));
        stream.add(Token::new("on", TokenType::Keyword));
        stream.add(Token::new("employee_id", TokenType::Identifier));
        stream.add(Token::equal());
        stream.add(Token::new("department_id", TokenType::Identifier));
        stream.add(Token::new("join", TokenType::Keyword));
        stream.add(Token::new("roles", TokenType::Identifier));
        stream.add(Token::new("on", TokenType::Keyword));
        stream.add(Token::new("role_id", TokenType::Identifier));
        stream.add(Token::equal());
        stream.add(Token::new("id", TokenType::Identifier));

        stream.add(Token::end_of_stream());

        let mut parser = Parser::new(stream);
        let ast = parser.parse().unwrap();

        assert!(matches!(
            ast,
            Ast::Select { ref source, .. }
            if matches!(
                source,
                TableSource::Join { left: left_outer, right: right_outer, on: on_outer }
                if matches!(
                    left_outer.as_ref(),
                    TableSource::Join { left: left_inner, right: right_inner, on: on_inner }
                    if matches!(left_inner.as_ref(), TableSource::Table(name) if name == "employees")
                    && matches!(right_inner.as_ref(), TableSource::Table(name) if name == "departments")
                    && matches!(
                        on_inner,
                        Some(Expression::Single(Clause::Comparison { lhs, operator, rhs }))
                        if matches!(lhs, Literal::ColumnReference(column_name) if column_name == "employee_id")
                        && *operator == BinaryOperator::Eq
                        && matches!(rhs, Literal::ColumnReference(column_name) if column_name == "department_id")
                    )
                )
                && matches!(right_outer.as_ref(), TableSource::Table(name) if name == "roles")
                && matches!(
                    on_outer,
                    Some(Expression::Single(Clause::Comparison { lhs, operator, rhs }))
                    if matches!(lhs, Literal::ColumnReference(column_name) if column_name == "role_id")
                    && *operator == BinaryOperator::Eq
                    && matches!(rhs, Literal::ColumnReference(column_name) if column_name == "id")
                )
            )
        ));
    }
}
