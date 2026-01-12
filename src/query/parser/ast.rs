use crate::query::parser::projection::Projection;

pub(crate) enum Ast {
    ShowTables,
    DescribeTable {
        table_name: String,
    },
    Select {
        table_name: String,
        projection: Projection,
    },
}
