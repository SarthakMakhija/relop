pub(crate) mod error;
pub(crate) mod token;

use crate::query::lexer::error::LexError;
use crate::query::lexer::token::{Token, TokenStream, TokenType};
use std::collections::HashSet;

pub(crate) struct Lexer {
    input: Vec<char>,
    position: usize,
    keywords: HashSet<String>,
}

impl Lexer {
    pub(crate) fn new(source: &str) -> Self {
        Self::new_with_keywords(source, &["show", "tables", "describe", "table"])
    }

    pub(crate) fn new_with_keywords(source: &str, keywords: &[&str]) -> Self {
        Self {
            input: source.chars().collect(),
            position: 0,
            keywords: keywords.iter().map(|keyword| keyword.to_string()).collect(),
        }
    }

    pub(crate) fn lex(&mut self) -> Result<TokenStream, LexError> {
        let mut stream = TokenStream::new();
        while let Some(char) = self.peek() {
            match char {
                ch if ch.is_whitespace() => self.eat(),
                ch if Self::looks_like_an_identifier(ch) => {
                    stream.add(self.identifier_or_keyword());
                }
                ';' => {
                    stream.add(Token::semicolon());
                    self.eat();
                }
                _ => {
                    return Err(LexError::UnexpectedCharacter(char));
                }
            }
        }
        stream.add(Token::end_of_stream());
        Ok(stream)
    }

    fn eat(&mut self) {
        let _ = self.advance();
    }

    fn advance(&mut self) -> Option<char> {
        let char = self.peek();
        if char.is_some() {
            self.position += 1;
        }
        char
    }

    fn peek(&self) -> Option<char> {
        self.input.get(self.position).copied()
    }

    fn identifier_or_keyword(&mut self) -> Token {
        let mut lexeme = String::new();
        
        while let Some(ch) = self.peek() {
            if Self::looks_like_an_identifier(ch) {
                let _ = self.advance();
                lexeme.push(ch);
            } else {
                break;
            }
        }

        let is_keyword = self.keywords.contains(&lexeme.to_lowercase());

        if is_keyword {
            Token::new(lexeme, TokenType::Keyword)
        } else {
            Token::new(lexeme, TokenType::Identifier)
        }
    }

    fn looks_like_an_identifier(ch: char) -> bool {
        ch.is_ascii_alphanumeric() || ch == '_'
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_lex {
        ($input:expr, [$(($ty:expr, $lex:expr)),* $(,)?]) => {{
            let tokens = Lexer::new($input).lex().unwrap();
            let expected = vec![$(($ty, $lex)),*];

            assert_eq!(expected.len(), tokens.len());

            for (index, (token_type, lexeme)) in expected.iter().enumerate() {
                let token = tokens.token_at(index).unwrap();
                assert_eq!(*token_type, token.token_type());
                assert_eq!(*lexeme, token.lexeme());
            }
        }};
    }

    #[test]
    fn lex_show_tables() {
        assert_lex!(
            "SHOW TABLES;",
            [
                (TokenType::Keyword, "SHOW"),
                (TokenType::Keyword, "TABLES"),
                (TokenType::Semicolon, ";"),
                (TokenType::EndOfStream, ""),
            ]
        )
    }

    #[test]
    fn lex_describe_table() {
        assert_lex!(
            "DESCRIBE TABLE employees",
            [
                (TokenType::Keyword, "DESCRIBE"),
                (TokenType::Keyword, "TABLE"),
                (TokenType::Identifier, "employees"),
                (TokenType::EndOfStream, ""),
            ]
        )
    }

    #[test]
    fn unrecognized_character() {
        let result = Lexer::new("select +").lex();
        assert!(matches!(
            result,
            Err(LexError::UnexpectedCharacter(ch)) if ch == '+'
        ));
    }
}
