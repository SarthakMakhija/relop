pub(crate) enum Ast {
    ShowTables,
    DescribeTable { table_name: String },
    Select { table_name: String },
}
