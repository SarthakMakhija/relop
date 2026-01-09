pub(crate) struct TokenStream {
    tokens: Vec<Token>,
}

pub(crate) struct Token {
    lexeme: String,
    token_type: TokenType,
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) enum TokenType {
    Identifier,
    Keyword,
    Semicolon,
    EndOfStream,
}

impl Token {
    pub(crate) fn new<S: Into<String>>(lexeme: S, token_type: TokenType) -> Token {
        Token {
            lexeme: lexeme.into(),
            token_type,
        }
    }

    pub(crate) fn end_of_stream() -> Token {
        Token::new("", TokenType::EndOfStream)
    }

    pub(crate) fn semicolon() -> Token {
        Token::new(";", TokenType::Semicolon)
    }

    pub(crate) fn lexeme(&self) -> &str {
        &self.lexeme
    }

    pub(crate) fn token_type(&self) -> TokenType {
        self.token_type
    }

    pub(crate) fn matches(&self, token_type: TokenType, text: &str) -> bool {
        self.lexeme.eq_ignore_ascii_case(text) && self.token_type == token_type
    }
}

impl TokenStream {
    pub(crate) fn new() -> TokenStream {
        Self { tokens: Vec::new() }
    }

    pub(crate) fn add(&mut self, token: Token) {
        self.tokens.push(token);
    }

    pub(crate) fn len(&self) -> usize {
        self.tokens.len()
    }

    pub(crate) fn token_at(&self, index: usize) -> Option<&Token> {
        self.tokens.get(index)
    }
}

#[cfg(test)]
mod tests {
    use crate::query::lexer::token::{Token, TokenStream, TokenType};

    #[test]
    fn add_a_token() {
        let mut stream = TokenStream::new();
        stream.add(Token::semicolon());

        assert_eq!(1, stream.len());
    }

    #[test]
    fn add_two_tokens() {
        let mut stream = TokenStream::new();
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        assert_eq!(2, stream.len());
    }

    #[test]
    fn get_token_at() {
        let mut stream = TokenStream::new();
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        assert_eq!(TokenType::Semicolon, stream.token_at(0).unwrap().token_type);
        assert_eq!(
            TokenType::EndOfStream,
            stream.token_at(1).unwrap().token_type
        );
    }

    #[test]
    fn attempt_to_get_token_at_index_beyond_available_tokens() {
        let mut stream = TokenStream::new();
        stream.add(Token::semicolon());
        stream.add(Token::end_of_stream());

        assert!(stream.token_at(2).is_none());
    }
}

#[cfg(test)]
mod token_tests {
    use crate::query::lexer::token::{Token, TokenType};

    #[test]
    fn semicolon_token() {
        let token = Token::semicolon();
        assert_eq!(";", token.lexeme());
        assert_eq!(TokenType::Semicolon, token.token_type());
    }

    #[test]
    fn end_of_stream_token() {
        let token = Token::end_of_stream();
        assert_eq!("", token.lexeme());
        assert_eq!(TokenType::EndOfStream, token.token_type());
    }

    #[test]
    fn keyword_token() {
        let token = Token::new("select", TokenType::Keyword);
        assert_eq!("select", token.lexeme());
        assert_eq!(TokenType::Keyword, token.token_type());
    }

    #[test]
    fn identifier_token() {
        let token = Token::new("employees", TokenType::Identifier);
        assert_eq!("employees", token.lexeme());
        assert_eq!(TokenType::Identifier, token.token_type());
    }

    #[test]
    fn matches_keyword_token() {
        let token = Token::new("SELECT", TokenType::Keyword);
        assert!(token.matches(TokenType::Keyword, "select"));
    }

    #[test]
    fn does_not_match_keyword_token() {
        let token = Token::new("SELECT", TokenType::Keyword);
        assert!(!token.matches(TokenType::Keyword, "DESCRIBE"));
    }

    #[test]
    fn does_not_match_keyword_token_because_the_token_is_an_identifier() {
        let token = Token::new("employees", TokenType::Identifier);
        assert!(!token.matches(TokenType::Keyword, "DESCRIBE"));
    }
}
