#[derive(Debug, PartialEq)]
pub enum LexError {
    UnexpectedCharacter(char),
}
