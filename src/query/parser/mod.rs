pub(crate) mod ast;
pub(crate) mod error;

use crate::query::lexer::token::{TokenStream, TokenType};
use crate::query::lexer::token_cursor::TokenCursor;
use crate::query::parser::ast::Ast;
use crate::query::parser::error::ParseError;

pub(crate) struct Parser {
    cursor: TokenCursor,
}

impl Parser {
    pub(crate) fn new(stream: TokenStream) -> Parser {
        Self {
            cursor: stream.cursor(),
        }
    }

    pub(crate) fn parse(&mut self) -> Result<Ast, ParseError> {
        let ast = self.parse_statement()?;
        self.expect_end_of_stream()?;
        Ok(ast)
    }

    fn parse_statement(&mut self) -> Result<Ast, ParseError> {
        if let Some(token) = self.cursor.peek() {
            return if token.matches(TokenType::Keyword, "show") {
                self.parse_show_tables()
            } else if token.matches(TokenType::Keyword, "describe") {
                self.parse_describe_table()
            } else {
                Err(ParseError::UnsupportedToken {
                    expected: "show | describe | select".to_string(),
                    found: token.lexeme().to_string(),
                })
            };
        }
        Err(ParseError::NoTokens)
    }

    fn parse_show_tables(&mut self) -> Result<Ast, ParseError> {
        self.expect_keyword("show")?;
        self.expect_keyword("tables")?;
        self.maybe_semicolon();

        Ok(Ast::ShowTables)
    }

    fn parse_describe_table(&mut self) -> Result<Ast, ParseError> {
        self.expect_keyword("describe")?;
        self.expect_keyword("table")?;
        let table_name = self.expect_identifier()?;
        self.maybe_semicolon();

        Ok(Ast::DescribeTable {
            table_name: table_name.to_string(),
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

    fn maybe_semicolon(&mut self) {
        if let Some(token) = self.cursor.peek() {
            if token.is_semicolon() {
                self.cursor.next();
            }
        }
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
