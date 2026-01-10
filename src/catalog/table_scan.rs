use crate::storage::row::Row;
use crate::storage::table_store::TableStore;
use std::sync::Arc;

/// Iterator that scans all rows in a table.
pub struct TableScan {
    store: Arc<TableStore>,
}

impl TableScan {
    pub(crate) fn new(table_store: Arc<TableStore>) -> TableScan {
        Self { store: table_store }
    }

    /// Returns an iterator over the rows in the table.
    ///
    /// The iterator yields `Row` items in an unspecified order.
    ///
    pub fn iter(&self) -> impl Iterator<Item = Row> + '_ {
        self.store
            .entries()
            .iter()
            .map(|entry| entry.value().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::column_value::ColumnValue;

    #[test]
    fn scan() {
        let store = TableStore::new();
        store.insert(Row::filled(vec![
            ColumnValue::Int(10),
            ColumnValue::Text("relop".to_string()),
        ]));

        let table_scan = TableScan::new(Arc::new(store));
        let rows = table_scan.iter().collect::<Vec<_>>();

        assert_eq!(1, rows.len());

        let inserted_row = rows.first().unwrap();
        let expected_row = Row::filled(vec![
            ColumnValue::Int(10),
            ColumnValue::Text("relop".to_string()),
        ]);

        assert_eq!(&expected_row, inserted_row);
    }
}
