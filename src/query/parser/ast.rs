pub(crate) enum Ast {
    ShowTables,
    DescribeTable { table_name: String },
}
