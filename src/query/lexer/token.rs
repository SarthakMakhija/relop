use crate::query::lexer::token_cursor::TokenCursor;

/// `TokenStream` represents a sequence of tokens produced by the lexer.
pub(crate) struct TokenStream {
    tokens: Vec<Token>,
}

/// `Token` represents a single unit of meaning in the source code,
/// such as an identifier, a keyword, or a punctuation mark.
pub(crate) struct Token {
    lexeme: String,
    token_type: TokenType,
}

/// `TokenType` defines the various categories of tokens that can be recognized.
#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) enum TokenType {
    /// An identifier (e.g., table name, column name).
    Identifier,
    /// A reserved keyword (e.g., SELECT, FROM).
    Keyword,
    /// A semicolon `;`, used to terminate statements.
    Semicolon,
    /// An asterisk `*`, used for "select all".
    Star,
    /// A comma `,`, used for separating items in a list.
    Comma,
    /// A left parentheses, `(`.
    LeftParentheses,
    /// A right parentheses, `)`.
    RightParentheses,
    /// A whole number (e.g.; 100, 120)
    WholeNumber,
    /// A string literal (e.g.; 'relop')
    StringLiteral,
    /// Equal operator `=`.
    Equal,
    /// Greater than or equal operator `>=`.
    GreaterEqual,
    /// Greater than operator `>`.
    Greater,
    /// Less than or equal operator `<=`.
    LesserEqual,
    /// Less than operator `<`.
    Lesser,
    /// Not equal operator `!=`.
    NotEqual,
    /// Indicates the end of the token stream.
    EndOfStream,
}

impl Token {
    /// Creates a new `Token` with the given lexeme and type.
    pub(crate) fn new<S: Into<String>>(lexeme: S, token_type: TokenType) -> Token {
        Token {
            lexeme: lexeme.into(),
            token_type,
        }
    }

    /// Creates a token representing the end of the input stream.
    pub(crate) fn end_of_stream() -> Token {
        Token::new("", TokenType::EndOfStream)
    }

    /// Creates a semicolon token `;`.
    pub(crate) fn semicolon() -> Token {
        Token::new(";", TokenType::Semicolon)
    }

    /// Creates an asterisk token `*`.
    pub(crate) fn star() -> Token {
        Token::new("*", TokenType::Star)
    }

    /// Creates an equal to token `=`.
    pub(crate) fn equal() -> Token {
        Token::new("=", TokenType::Equal)
    }

    /// Creates a greater than or equal token `>=`.
    pub(crate) fn greater_equal() -> Token {
        Token::new(">=", TokenType::GreaterEqual)
    }

    /// Creates a greater than token `>`.
    pub(crate) fn greater() -> Token {
        Token::new(">", TokenType::Greater)
    }

    /// Creates a less than or equal token `<=`.
    pub(crate) fn lesser_equal() -> Token {
        Token::new("<=", TokenType::LesserEqual)
    }

    /// Creates a less than token `<`.
    pub(crate) fn lesser() -> Token {
        Token::new("<", TokenType::Lesser)
    }

    /// Creates a not equal token `!=`.
    pub(crate) fn not_equal() -> Token {
        Token::new("!=", TokenType::NotEqual)
    }

    /// Creates a comma token `,`.
    pub(crate) fn comma() -> Token {
        Token::new(",", TokenType::Comma)
    }

    /// Creates a left parentheses token `(`.
    pub(crate) fn left_parentheses() -> Token {
        Token::new("(", TokenType::LeftParentheses)
    }

    /// Creates a right parentheses token `)`.
    pub(crate) fn right_parentheses() -> Token {
        Token::new(")", TokenType::RightParentheses)
    }

    /// Returns the string representation of the token.
    pub(crate) fn lexeme(&self) -> &str {
        &self.lexeme
    }

    /// Checks if the token matches a specific type and case-insensitive text.
    pub(crate) fn matches(&self, token_type: TokenType, text: &str) -> bool {
        self.lexeme.eq_ignore_ascii_case(text) && self.token_type == token_type
    }

    /// Checks if the token is a semicolon `;`.
    pub(crate) fn is_semicolon(&self) -> bool {
        self.lexeme == ";" && self.token_type == TokenType::Semicolon
    }

    /// Checks if the token is an asterisk `*`.
    pub(crate) fn is_star(&self) -> bool {
        self.lexeme == "*" && self.token_type == TokenType::Star
    }

    /// Checks if the token is a comma `,`.
    pub(crate) fn is_comma(&self) -> bool {
        self.lexeme == "," && self.token_type == TokenType::Comma
    }

    /// Checks if the token is left parentheses `(`.
    pub(crate) fn is_left_parentheses(&self) -> bool {
        self.lexeme == "(" && self.token_type == TokenType::LeftParentheses
    }

    /// Checks if the token is right parentheses `)`.
    pub(crate) fn is_right_parentheses(&self) -> bool {
        self.lexeme == ")" && self.token_type == TokenType::RightParentheses
    }

    /// Checks if the token represents the end of the stream.
    pub(crate) fn is_end_of_stream(&self) -> bool {
        self.token_type == TokenType::EndOfStream
    }

    /// Checks if the token is an identifier.
    pub(crate) fn is_identifier(&self) -> bool {
        !self.lexeme.is_empty() && self.token_type == TokenType::Identifier
    }

    /// Checks if the token is a keyword.
    pub(crate) fn is_keyword(&self, keyword: &str) -> bool {
        !self.lexeme.is_empty()
            && self.token_type == TokenType::Keyword
            && self.lexeme.eq_ignore_ascii_case(keyword)
    }

    /// Checks if the token is a whole number.
    pub(crate) fn is_a_whole_number(&self) -> bool {
        !self.lexeme.is_empty() && self.token_type == TokenType::WholeNumber
    }

    /// Checks if the token is a whole number.
    pub(crate) fn is_string_literal(&self) -> bool {
        !self.lexeme.is_empty() && self.token_type == TokenType::StringLiteral
    }

    /// Returns the type of the token.
    pub(crate) fn token_type(&self) -> TokenType {
        self.token_type
    }
}

impl TokenStream {
    /// Creates a new, empty `TokenStream`.
    pub(crate) fn new() -> TokenStream {
        Self { tokens: Vec::new() }
    }

    /// Adds a token to the stream.
    pub(crate) fn add(&mut self, token: Token) {
        self.tokens.push(token);
    }

    /// Retrieves the token at the specified index.
    pub(crate) fn token_at(&self, index: usize) -> Option<&Token> {
        self.tokens.get(index)
    }

    /// Creates a cursor for iterating over the tokens in this stream.
    pub(crate) fn cursor(self) -> TokenCursor {
        TokenCursor::new(self)
    }
}

#[cfg(test)]
impl TokenStream {
    pub(crate) fn len(&self) -> usize {
        self.tokens.len()
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
    fn star_token() {
        let token = Token::star();
        assert_eq!("*", token.lexeme());
        assert_eq!(TokenType::Star, token.token_type());
    }

    #[test]
    fn comma_token() {
        let token = Token::comma();
        assert_eq!(",", token.lexeme());
        assert_eq!(TokenType::Comma, token.token_type());
    }

    #[test]
    fn left_parentheses_token() {
        let token = Token::left_parentheses();
        assert_eq!("(", token.lexeme());
        assert_eq!(TokenType::LeftParentheses, token.token_type());
    }

    #[test]
    fn right_parentheses_token() {
        let token = Token::right_parentheses();
        assert_eq!(")", token.lexeme());
        assert_eq!(TokenType::RightParentheses, token.token_type());
    }

    #[test]
    fn equal_token() {
        let token = Token::equal();
        assert_eq!("=", token.lexeme());
        assert_eq!(TokenType::Equal, token.token_type());
    }

    #[test]
    fn greater_equal_token() {
        let token = Token::greater_equal();
        assert_eq!(">=", token.lexeme());
        assert_eq!(TokenType::GreaterEqual, token.token_type());
    }

    #[test]
    fn greater_token() {
        let token = Token::greater();
        assert_eq!(">", token.lexeme());
        assert_eq!(TokenType::Greater, token.token_type());
    }

    #[test]
    fn lesser_equal_token() {
        let token = Token::lesser_equal();
        assert_eq!("<=", token.lexeme());
        assert_eq!(TokenType::LesserEqual, token.token_type());
    }

    #[test]
    fn lesser_token() {
        let token = Token::lesser();
        assert_eq!("<", token.lexeme());
        assert_eq!(TokenType::Lesser, token.token_type());
    }

    #[test]
    fn not_equal_token() {
        let token = Token::not_equal();
        assert_eq!("!=", token.lexeme());
        assert_eq!(TokenType::NotEqual, token.token_type());
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

    #[test]
    fn is_a_semicolon_token() {
        let token = Token::new(";", TokenType::Semicolon);
        assert!(token.is_semicolon());
    }

    #[test]
    fn is_not_a_semicolon_token() {
        let token = Token::new("select", TokenType::Keyword);
        assert!(!token.is_semicolon());
    }

    #[test]
    fn is_end_of_stream_token() {
        let token = Token::end_of_stream();
        assert!(token.is_end_of_stream());
    }

    #[test]
    fn is_star_token() {
        let token = Token::star();
        assert!(token.is_star());
    }

    #[test]
    fn is_not_star_token() {
        let token = Token::semicolon();
        assert!(!token.is_star());
    }

    #[test]
    fn is_not_end_of_stream_token() {
        let token = Token::new("select", TokenType::Keyword);
        assert!(!token.is_end_of_stream());
    }

    #[test]
    fn is_a_comma_token() {
        let token = Token::new(",", TokenType::Comma);
        assert!(token.is_comma());
    }

    #[test]
    fn is_not_a_comma_token() {
        let token = Token::new("select", TokenType::Keyword);
        assert!(!token.is_comma());
    }

    #[test]
    fn is_left_parentheses_token() {
        let token = Token::new("(", TokenType::LeftParentheses);
        assert!(token.is_left_parentheses());
    }

    #[test]
    fn is_not_left_parentheses_token() {
        let token = Token::new("select", TokenType::Keyword);
        assert!(!token.is_left_parentheses());
    }

    #[test]
    fn is_right_parentheses_token() {
        let token = Token::new(")", TokenType::RightParentheses);
        assert!(token.is_right_parentheses());
    }

    #[test]
    fn is_not_right_parentheses_token() {
        let token = Token::new("select", TokenType::Keyword);
        assert!(!token.is_right_parentheses());
    }

    #[test]
    fn is_a_whole_number_token() {
        let token = Token::new("10", TokenType::WholeNumber);
        assert!(token.is_a_whole_number());
    }

    #[test]
    fn is_not_a_whole_number_token() {
        let token = Token::new("select", TokenType::Keyword);
        assert!(!token.is_a_whole_number());
    }

    #[test]
    fn is_a_string_literal_token() {
        let token = Token::new("relop", TokenType::StringLiteral);
        assert!(token.is_string_literal());
    }

    #[test]
    fn is_not_a_string_literal_token() {
        let token = Token::new("select", TokenType::Keyword);
        assert!(!token.is_string_literal());
    }

    #[test]
    fn is_a_keyword() {
        let token = Token::new("select", TokenType::Keyword);
        assert!(token.is_keyword("select"));
    }

    #[test]
    fn is_not_a_keyword_token() {
        let token = Token::new("employees", TokenType::Identifier);
        assert!(!token.is_keyword("select"));
    }

    #[test]
    fn is_an_identifier_token() {
        let token = Token::new("employees", TokenType::Identifier);
        assert!(token.is_identifier());
    }

    #[test]
    fn is_not_an_identifier_token() {
        let token = Token::new("select", TokenType::Keyword);
        assert!(!token.is_identifier());
    }
}
