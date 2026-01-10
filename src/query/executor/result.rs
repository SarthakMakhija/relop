use crate::catalog::table_descriptor::TableDescriptor;

pub(crate) enum QueryResult {
    TableList(Vec<String>),
    TableDescription(TableDescriptor),
}

impl QueryResult {
    pub(crate) fn all_tables(&self) -> Option<&Vec<String>> {
        match self {
            QueryResult::TableList(tables) => Some(tables),
            _ => None,
        }
    }

    pub(crate) fn table_descriptor(&self) -> Option<&TableDescriptor> {
        match self {
            QueryResult::TableDescription(table_descriptor) => Some(table_descriptor),
            _ => None,
        }
    }
}
