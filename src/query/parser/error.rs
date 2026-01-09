#[derive(Debug)]
pub(crate) enum ParseError {
    UnsupportedToken { expected: String, found: String },
    NoTokens,
    UnexpectedToken { expected: String, found: String },
    UnexpectedEndOfInput,
}
