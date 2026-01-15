/// Represents errors that can occur during lexical analysis.
#[derive(Debug, PartialEq)]
pub enum LexError {
    /// Indicates an unexpected character was encountered in the input.
    UnexpectedCharacter(char),
    /// Indicates an unterminated string literal.
    UnterminatedStringLiteral,
    /// Indicates an unsupported operator.
    UnsupportedOperator(char),
}
