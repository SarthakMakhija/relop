use crate::query::parser::ordering_key::{OrderingDirection, OrderingKey};
use crate::schema::Schema;
use crate::storage::error::RowViewComparatorError;
use crate::storage::row::Row;

use crate::types::column_value::ColumnValue;

/// A read-only view over a single row, bound to a table's schema.
///
/// `RowView` provides **name-based access** to column values without exposing
/// internal storage details such as column positions or row layout.
///
/// It pairs:
/// - a concrete [`Row`] containing the actual values, and
/// - a reference to the corresponding [`Schema`] used to resolve column names.
///
/// This abstraction is primarily used by query execution results (e.g. `SELECT *`)
/// to allow clients to retrieve column values by name instead of index.
///
/// # Notes
///
/// - Column lookups are resolved via the table schema at runtime.
/// - No cloning of column values occurs; returned values are borrowed.
/// - `RowView` is intentionally read-only.
pub struct RowView<'a> {
    row: Row,
    schema: &'a Schema,
    visible_positions: &'a [usize],
}

impl<'a> RowView<'a> {
    /// Creates a new `RowView` for the given row and table.
    ///
    /// # Arguments
    ///
    /// * `row` - The row containing column values.
    /// * `schema` - The schema which defines the column layout.
    pub(crate) fn new(row: Row, schema: &'a Schema, visible_positions: &'a [usize]) -> Self {
        Self {
            row,
            schema,
            visible_positions,
        }
    }

    /// Retrieves the value of a column by name.
    ///
    /// # Arguments
    ///
    /// * `column_name` - The name of the column to retrieve.
    ///
    /// # Returns
    ///
    /// * `Some(&ColumnValue)` if the column exists.
    /// * `None` if the column name is not part of the table schema.
    ///
    /// # Notes
    ///
    /// - Column name resolution is case-sensitive.
    /// - This method performs a schema lookup on each call.
    pub fn column_value_by(&self, column_name: &str) -> Option<&ColumnValue> {
        let column_position = self.schema.column_position(column_name)?;
        if self.visible_positions.contains(&column_position) {
            return self.row.column_value_at(column_position);
        }
        None
    }

    /// Retrieves the value of a column by its index.
    ///
    /// # Arguments
    ///
    /// * `index` - The index (or position) of column to retrieve.
    pub(crate) fn column_value_at_unchecked(&self, index: usize) -> &ColumnValue {
        self.row.column_value_at(index).unwrap()
    }

    /// Projects the row view to a new set of visible positions.
    pub(crate) fn project(self, visible_positions: &'a [usize]) -> Self {
        Self {
            row: self.row,
            schema: self.schema,
            visible_positions,
        }
    }
}

/// A comparator for [`RowView`]s that implements multi-column sorting logic.
///
/// `RowViewComparator` is used to order rows based on a sequence of [`OrderingKey`]s.
/// It pre-calculates the positions of the sort columns in the schema to avoid repeated
/// lookups during comparison.
pub(crate) struct RowViewComparator<'a> {
    positions: Vec<usize>,
    ordering_keys: &'a [OrderingKey],
}

impl<'a> RowViewComparator<'a> {
    /// Creates a new `RowViewComparator`.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema of the rows being compared.
    /// * `ordering_keys` - A list of keys defining the sort order (column name and direction).
    ///
    /// # Returns
    ///
    /// * `Ok(RowViewComparator)` if all ordering columns exist in the schema.
    /// * `Err(RowViewComparatorError::UnknownColumn)` if any ordering column is missing.
    pub fn new(
        schema: &Schema,
        ordering_keys: &'a [OrderingKey],
    ) -> Result<Self, RowViewComparatorError> {
        let positions = ordering_keys
            .iter()
            .map(|key| {
                schema
                    .column_position(&key.column)
                    .ok_or_else(|| RowViewComparatorError::UnknownColumn(key.column.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            positions,
            ordering_keys,
        })
    }

    /// Compares two [`RowView`]s according to the configured ordering keys.
    ///
    /// It iterates through the ordering keys in priority order.
    /// The first non-equal comparison determines the result.
    /// If all keys are equal, the rows are considered equal.
    pub fn compare(&self, left: &RowView, right: &RowView) -> std::cmp::Ordering {
        for (column_position, key) in self.positions.iter().zip(self.ordering_keys.iter()) {
            //SAFETY: the column positions are already captured and validated in
            //RowViewComparator's new().
            //So, unwrap() is safe here.
            let left_value = left.column_value_at_unchecked(*column_position);
            let right_value = right.column_value_at_unchecked(*column_position);

            let ordering = left_value.cmp(right_value);

            if ordering != std::cmp::Ordering::Equal {
                return match key.direction {
                    OrderingDirection::Ascending => ordering,
                    OrderingDirection::Descending => ordering.reverse(),
                };
            }
        }
        std::cmp::Ordering::Equal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Schema;
    use crate::types::column_type::ColumnType;

    #[test]
    fn column_value() {
        let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();
        let row = Row::filled(vec![ColumnValue::int(200)]);

        let visible_positions = vec![0];
        let view = RowView::new(row, &schema, &visible_positions);
        assert_eq!(&ColumnValue::int(200), view.column_value_by("id").unwrap());
    }

    #[test]
    fn attempt_to_get_non_existing_column() {
        let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();
        let row = Row::filled(vec![ColumnValue::int(200)]);

        let visible_positions = vec![0];
        let view = RowView::new(row, &schema, &visible_positions);
        assert!(view.column_value_by("name").is_none());
    }

    #[test]
    fn attempt_to_get_a_column_not_in_visible_position() {
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("name", ColumnType::Text)
            .unwrap();
        let row = Row::filled(vec![ColumnValue::int(200), ColumnValue::text("relop")]);

        let visible_positions = vec![1];
        let view = RowView::new(row, &schema, &visible_positions);
        assert!(view.column_value_by("id").is_none());
        assert_eq!(
            &ColumnValue::text("relop"),
            view.column_value_by("name").unwrap()
        );
    }
    #[test]
    fn project_row_view() {
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("name", ColumnType::Text)
            .unwrap();

        let row = Row::filled(vec![ColumnValue::int(200), ColumnValue::text("relop")]);

        let visible_positions = vec![0, 1];
        let view = RowView::new(row, &schema, &visible_positions);
        assert_eq!(&ColumnValue::int(200), view.column_value_by("id").unwrap());
        assert_eq!(
            &ColumnValue::text("relop"),
            view.column_value_by("name").unwrap()
        );

        let projection = vec![1];
        let projected_view = view.project(&projection);
        assert!(projected_view.column_value_by("id").is_none());
        assert_eq!(
            &ColumnValue::text("relop"),
            projected_view.column_value_by("name").unwrap()
        );
    }
}

#[cfg(test)]
mod row_view_comparator_tests {
    use super::*;
    use crate::schema::Schema;
    use crate::types::column_type::ColumnType;
    use crate::{asc, desc};
    use std::cmp::Ordering;

    #[test]
    fn compare_row_views_on_single_column_ascending() {
        let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();

        let ordering_keys = vec![asc!("id")];

        let comparator = RowViewComparator::new(&schema, &ordering_keys).unwrap();

        let row1 = Row::filled(vec![ColumnValue::int(1)]);
        let row2 = Row::filled(vec![ColumnValue::int(2)]);

        let visible_positions = [0];
        let row_view1 = RowView::new(row1, &schema, &visible_positions);
        let row_view2 = RowView::new(row2, &schema, &visible_positions);

        assert_eq!(comparator.compare(&row_view1, &row_view2), Ordering::Less);
    }

    #[test]
    fn compare_row_views_on_multiple_columns_ascending() {
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("rank", ColumnType::Int)
            .unwrap();

        let ordering_keys = vec![asc!("id"), asc!("rank")];

        let comparator = RowViewComparator::new(&schema, &ordering_keys).unwrap();

        let row1 = Row::filled(vec![ColumnValue::int(1), ColumnValue::int(10)]);
        let row2 = Row::filled(vec![ColumnValue::int(2), ColumnValue::int(10)]);

        let visible_positions = [0, 1];
        let row_view1 = RowView::new(row1, &schema, &visible_positions);
        let row_view2 = RowView::new(row2, &schema, &visible_positions);

        assert_eq!(comparator.compare(&row_view1, &row_view2), Ordering::Less);
    }

    #[test]
    fn compare_row_views_on_multiple_columns_with_same_value_ascending() {
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("rank", ColumnType::Int)
            .unwrap();

        let ordering_keys = vec![asc!("id"), asc!("rank")];

        let comparator = RowViewComparator::new(&schema, &ordering_keys).unwrap();

        let row1 = Row::filled(vec![ColumnValue::int(1), ColumnValue::int(10)]);
        let row2 = Row::filled(vec![ColumnValue::int(1), ColumnValue::int(20)]);

        let visible_positions = [0, 1];
        let row_view1 = RowView::new(row1, &schema, &visible_positions);
        let row_view2 = RowView::new(row2, &schema, &visible_positions);

        assert_eq!(comparator.compare(&row_view1, &row_view2), Ordering::Less);
    }

    #[test]
    fn compare_row_views_on_multiple_columns_with_same_value_and_mixed_directions() {
        let schema = Schema::new()
            .add_column("id", ColumnType::Int)
            .unwrap()
            .add_column("rank", ColumnType::Int)
            .unwrap();

        let ordering_keys = vec![asc!("id"), desc!("rank")];

        let comparator = RowViewComparator::new(&schema, &ordering_keys).unwrap();

        let row1 = Row::filled(vec![ColumnValue::int(1), ColumnValue::int(10)]);
        let row2 = Row::filled(vec![ColumnValue::int(1), ColumnValue::int(20)]);

        let visible_positions = [0, 1];
        let row_view1 = RowView::new(row1, &schema, &visible_positions);
        let row_view2 = RowView::new(row2, &schema, &visible_positions);

        assert_eq!(
            comparator.compare(&row_view1, &row_view2),
            Ordering::Greater
        );
    }

    #[test]
    fn attempt_compare_row_views_on_with_non_existing_column() {
        let schema = Schema::new().add_column("id", ColumnType::Int).unwrap();

        let ordering_keys = vec![asc!("id"), desc!("rank")];

        let result = RowViewComparator::new(&schema, &ordering_keys);
        assert!(
            matches!(result, Err(RowViewComparatorError::UnknownColumn(column)) if column == "rank")
        );
    }
}
