pub(crate) mod ast;
pub mod error;
pub(crate) mod projection;

use crate::query::lexer::token::{Token, TokenStream, TokenType};
use crate::query::lexer::token_cursor::TokenCursor;
use crate::query::parser::ast::Ast;
use crate::query::parser::error::ParseError;
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
        let table_name = self.expect_identifier()?;
        let _ = self.eat_if(|token| token.is_semicolon());

        Ok(Ast::Select {
            table_name: table_name.to_string(),
            projection,
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
            matches!(ast, Ast::Select { table_name, projection } if table_name == "employees" && projection == Projection::All)
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
            matches!(ast, Ast::Select { table_name, projection } if table_name == "employees" && projection == Projection::All)
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

        assert!(matches!(ast, Ast::Select { table_name, projection }
                if table_name == "employees" && projection == Projection::Columns(vec!["name".to_string(), "id".to_string()])));
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

        assert!(matches!(ast, Ast::Select { table_name, projection }
                if table_name == "employees" && projection == Projection::Columns(vec!["name".to_string()])));
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

        assert!(matches!(ast, Ast::Select { table_name, projection }
                if table_name == "employees" && projection == Projection::Columns(vec!["name".to_string(), "id".to_string()])));
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
