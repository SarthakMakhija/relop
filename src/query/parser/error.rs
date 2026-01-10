/// Represents errors that can occur during parsing.
#[derive(Debug)]
pub enum ParseError {
    /// Indicates that an unsupported token was encountered where another was expected.
    UnsupportedToken {
        /// The expected token description.
        expected: String,
        /// The actual token found.
        found: String,
    },
    /// Indicates that no tokens were available to parse.
    NoTokens,
    /// Indicates that an unexpected token was encountered.
    UnexpectedToken {
        /// The expected token description.
        expected: String,
        /// The actual token found.
        found: String,
    },
    /// Indicates that the input ended unexpectedly.
    UnexpectedEndOfInput,
}
