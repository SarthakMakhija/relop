#[derive(Debug)]
pub enum ParseError {
    UnsupportedToken { expected: String, found: String },
    NoTokens,
    UnexpectedToken { expected: String, found: String },
    UnexpectedEndOfInput,
}
