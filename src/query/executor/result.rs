pub(crate) enum QueryResult {
    AllTables(Vec<String>),
}

impl QueryResult {
    pub(crate) fn all_tables(&self) -> Option<&Vec<String>> {
        match self {
            QueryResult::AllTables(tables) => Some(tables),
            _ => None,
        }
    }
}
