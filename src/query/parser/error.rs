/// Represents errors that can occur during parsing.
#[derive(Debug, PartialEq, Eq)]
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
    /// Indicates the limit value was not present.
    NoLimitValue,
    /// Indicates the limit value has exceeded the range.
    LimitOutOfRange(String),
    /// Indicates the limit value is zero.
    ZeroLimit,
    /// Indicates that the input ended unexpectedly.
    UnexpectedEndOfInput,
    /// Indicates that the input has exceeded the range of numeric literal.
    NumericLiteralOutOfRange(String),
}
