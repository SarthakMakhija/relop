pub mod error;
mod keywords;
pub(crate) mod token;
pub(crate) mod token_cursor;

use crate::query::lexer::error::LexError;
use crate::query::lexer::keywords::Keywords;
use crate::query::lexer::token::{Token, TokenStream, TokenType};

/// `Lexer` is responsible for lexical analysis of the input source string.
/// It converts a sequence of characters into a sequence of tokens (`TokenStream`).
///
/// It holds the input characters, current position, and a set of keywords for identification.
pub(crate) struct Lexer {
    input: Vec<char>,
    position: usize,
    keywords: Keywords,
}

impl Lexer {
    /// Creates a new `Lexer` with the default set of SQL keywords.
    ///
    /// # Arguments
    ///
    /// * `source` - The input string to be lexed.
    pub(crate) fn new_with_default_keywords(source: &str) -> Self {
        Self::new(source, Keywords::new_with_default_keywords())
    }

    /// Creates a new `Lexer` with a custom set of keywords.
    ///
    /// # Arguments
    ///
    /// * `source` - The input string to be lexed.
    /// * `keywords` - The `Keywords` instance to use for identifying reserved words.
    pub(crate) fn new(source: &str, keywords: Keywords) -> Self {
        Self {
            input: source.chars().collect(),
            position: 0,
            keywords,
        }
    }

    /// Performs lexical analysis on the input and returns a `TokenStream`.
    ///
    /// It iterates through the input characters, recognizing tokens such as whitespace,
    /// punctuation (semicolon, comma, star), identifiers, and keywords.
    ///
    /// # Returns
    ///
    /// * `Ok(TokenStream)` - A stream of tokens representing the input.
    /// * `Err(LexError)` - If an unexpected character is encountered.
    pub(crate) fn lex(&mut self) -> Result<TokenStream, LexError> {
        let mut stream = TokenStream::new();
        while let Some(char) = self.peek() {
            match char {
                ch if ch.is_whitespace() => self.eat(),
                ';' => self.capture_token(&mut stream, Token::semicolon()),
                '*' => self.capture_token(&mut stream, Token::star()),
                ',' => self.capture_token(&mut stream, Token::comma()),
                ch if Self::looks_like_an_identifier(ch) => {
                    stream.add(self.identifier_or_keyword())
                }
                _ => {
                    return Err(LexError::UnexpectedCharacter(char));
                }
            }
        }
        stream.add(Token::end_of_stream());
        Ok(stream)
    }

    fn capture_token(&mut self, stream: &mut TokenStream, token: Token) {
        stream.add(token);
        self.eat();
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

        let is_keyword = self.keywords.contains(lexeme.as_str());

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
            let tokens = Lexer::new_with_default_keywords($input).lex().unwrap();
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
    fn lex_select_star() {
        assert_lex!(
            "SELECT * FROM employees",
            [
                (TokenType::Keyword, "SELECT"),
                (TokenType::Star, "*"),
                (TokenType::Keyword, "FROM"),
                (TokenType::Identifier, "employees"),
                (TokenType::EndOfStream, ""),
            ]
        )
    }

    #[test]
    fn lex_select_with_projection() {
        assert_lex!(
            "SELECT id,name FROM employees",
            [
                (TokenType::Keyword, "SELECT"),
                (TokenType::Identifier, "id"),
                (TokenType::Comma, ","),
                (TokenType::Identifier, "name"),
                (TokenType::Keyword, "FROM"),
                (TokenType::Identifier, "employees"),
                (TokenType::EndOfStream, ""),
            ]
        )
    }

    #[test]
    fn lex_select_with_projection_separated_by_spaces() {
        assert_lex!(
            "SELECT id,name, address,pin FROM employees",
            [
                (TokenType::Keyword, "SELECT"),
                (TokenType::Identifier, "id"),
                (TokenType::Comma, ","),
                (TokenType::Identifier, "name"),
                (TokenType::Comma, ","),
                (TokenType::Identifier, "address"),
                (TokenType::Comma, ","),
                (TokenType::Identifier, "pin"),
                (TokenType::Keyword, "FROM"),
                (TokenType::Identifier, "employees"),
                (TokenType::EndOfStream, ""),
            ]
        )
    }

    #[test]
    fn unrecognized_character() {
        let result = Lexer::new_with_default_keywords("select +").lex();
        assert!(matches!(
            result,
            Err(LexError::UnexpectedCharacter(ch)) if ch == '+'
        ));
    }
}
