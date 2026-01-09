#[derive(Debug, PartialEq)]
pub(crate) enum LexError {
    UnexpectedCharacter(char),
}