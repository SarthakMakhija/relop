use crate::catalog::table_descriptor::TableDescriptor;

pub enum QueryResult {
    TableList(Vec<String>),
    TableDescription(TableDescriptor),
}

impl QueryResult {
    pub fn all_tables(&self) -> Option<&Vec<String>> {
        match self {
            QueryResult::TableList(tables) => Some(tables),
            _ => None,
        }
    }

    pub fn table_descriptor(&self) -> Option<&TableDescriptor> {
        match self {
            QueryResult::TableDescription(table_descriptor) => Some(table_descriptor),
            _ => None,
        }
    }
}
