use crate::query::lexer::token::{Token, TokenStream};

/// `TokenCursor` allows for traversing a `TokenStream` one token at a time.
/// It maintains an index to the current token position.
pub(crate) struct TokenCursor {
    stream: TokenStream,
    index: usize,
}

impl TokenCursor {
    /// Creates a new `TokenCursor` for the given `TokenStream`.
    pub(crate) fn new(stream: TokenStream) -> TokenCursor {
        TokenCursor { stream, index: 0 }
    }

    /// Returns the current token and advances the cursor to the next position.
    ///
    /// Returns `Some(Token)` if a token exists at the current position, or `None` if
    /// the end of the stream has been reached.
    pub(crate) fn next(&mut self) -> Option<&Token> {
        let token = self.stream.token_at(self.index);
        if token.is_some() {
            self.index += 1;
        }
        token
    }

    /// Returns the current token without advancing the cursor.
    pub(crate) fn peek(&self) -> Option<&Token> {
        self.stream.token_at(self.index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::lexer::token::TokenType;

    #[test]
    fn move_to_next_token() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("show", TokenType::Keyword));

        let mut cursor = TokenCursor::new(stream);
        let token = cursor.next().unwrap();

        assert_eq!(TokenType::Keyword, token.token_type());
        assert_eq!("show", token.lexeme());
    }

    #[test]
    fn move_to_next_token_with_more_than_one_token() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("show", TokenType::Keyword));
        stream.add(Token::new("tables", TokenType::Keyword));

        let mut cursor = TokenCursor::new(stream);

        let token = cursor.next().unwrap();
        assert_eq!(TokenType::Keyword, token.token_type());
        assert_eq!("show", token.lexeme());

        let token = cursor.next().unwrap();
        assert_eq!(TokenType::Keyword, token.token_type());
        assert_eq!("tables", token.lexeme());
    }

    #[test]
    fn move_to_next_token_with_no_further_tokens() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("show", TokenType::Keyword));

        let mut cursor = TokenCursor::new(stream);
        let token = cursor.next().unwrap();

        assert_eq!(TokenType::Keyword, token.token_type());
        assert_eq!("show", token.lexeme());
        assert!(cursor.peek().is_none());
    }

    #[test]
    fn peek_at_current_token() {
        let mut stream = TokenStream::new();
        stream.add(Token::new("show", TokenType::Keyword));

        let cursor = TokenCursor::new(stream);
        let token = cursor.peek().unwrap();

        assert_eq!(TokenType::Keyword, token.token_type());
        assert_eq!("show", token.lexeme());
    }
}
