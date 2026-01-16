use regex::Error;

/// `PlanningError` represents errors that occur during the logical planning phase.
#[derive(Debug, PartialEq)]
pub enum PlanningError {
    /// Indicates that a provided regular expression in a LIKE clause is invalid.
    InvalidRegex(String),
}

impl From<Error> for PlanningError {
    fn from(error: Error) -> Self {
        PlanningError::InvalidRegex(error.to_string())
    }
}
