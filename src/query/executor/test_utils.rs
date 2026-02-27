use crate::query::executor::error::ExecutionError;
use crate::query::executor::result_set::{ResultSet, RowViewResult};
use crate::row;
use crate::schema::Schema;
use crate::storage::row_view::RowView;
use std::sync::Arc;

pub struct ErrorResultSet {
    pub schema: Arc<Schema>,
}

impl ResultSet for ErrorResultSet {
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowViewResult> + '_>, ExecutionError> {
        Ok(Box::new(std::iter::once(Err(
            ExecutionError::TypeMismatchInComparison,
        ))))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

pub struct InitErrorResultSet {
    pub schema: Arc<Schema>,
}

impl ResultSet for InitErrorResultSet {
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowViewResult> + '_>, ExecutionError> {
        Err(ExecutionError::TypeMismatchInComparison)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

pub struct JoinResetErrorResultSet {
    pub schema: Arc<Schema>,
    pub visible_positions: Arc<Vec<usize>>,
    pub call_count: std::sync::atomic::AtomicUsize,
}

impl ResultSet for JoinResetErrorResultSet {
    fn iterator(&self) -> Result<Box<dyn Iterator<Item = RowViewResult> + '_>, ExecutionError> {
        let count = self
            .call_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if count == 0 {
            // First call succeeds with one row
            Ok(Box::new(std::iter::once(Ok(RowView::new(
                row![1],
                &self.schema,
                &self.visible_positions,
            )))))
        } else {
            // Subsequent calls fail
            Err(ExecutionError::TypeMismatchInComparison)
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
