#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Projection {
    All,
    Columns(Vec<String>),
}
