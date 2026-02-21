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
    /// * `Ok(Some(&ColumnValue))` if the column exists and is unique.
    /// * `Ok(None)` if the column name is not part of the table schema or is not in visible positions.
    /// * `Err(SchemaError::AmbiguousColumnName)` if the unqualified column name matches multiple columns.
    ///
    /// # Notes
    ///
    /// - Column name resolution is case-sensitive.
    /// - This method performs a schema lookup on each call.
    pub fn column_value_by(
        &self,
        column_name: &str,
    ) -> Result<Option<&ColumnValue>, crate::schema::error::SchemaError> {
        let column_position = self.schema.column_position(column_name)?;
        if let Some(position) = column_position {
            if self.visible_positions.contains(&position) {
                return Ok(self.row.column_value_at(position));
            }
        }
        Ok(None)
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

    /// Merges this `RowView` with another `RowView` to create a new `Row`.
    ///
    /// This is used in join operations where two rows are combined.
    /// Only the visible values from both row views are merged.
    pub(crate) fn merge(&self, other: &RowView) -> Row {
        let mut values =
            Vec::with_capacity(self.visible_positions.len() + other.visible_positions.len());

        for &pos in self.visible_positions {
            // SAFETY: visible_positions are validated at construction to be within bounds of the row.
            values.push(self.row.column_value_at(pos).unwrap().clone());
        }
        for &pos in other.visible_positions {
            // SAFETY: visible_positions are validated at construction to be within bounds of the row.
            values.push(other.row.column_value_at(pos).unwrap().clone());
        }
        Row::filled(values)
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
                    .map_err(|_| RowViewComparatorError::UnknownColumn(key.column.clone()))?
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
    use crate::row;
    use crate::schema;
    use crate::types::column_type::ColumnType;

    #[test]
    fn column_value() {
        let schema = schema!["id" => ColumnType::Int].unwrap();
        let row = row![200];

        let visible_positions = vec![0];
        let view = RowView::new(row, &schema, &visible_positions);
        assert_eq!(
            &ColumnValue::int(200),
            view.column_value_by("id").unwrap().unwrap()
        );
    }

    #[test]
    fn attempt_to_get_non_existing_column() {
        let schema = schema!["id" => ColumnType::Int].unwrap();
        let row = row![200];

        let visible_positions = vec![0];
        let view = RowView::new(row, &schema, &visible_positions);
        assert!(view.column_value_by("name").unwrap().is_none());
    }

    #[test]
    fn attempt_to_get_a_column_not_in_visible_position() {
        let schema = schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap();
        let row = row![200, "relop"];

        let visible_positions = vec![1];
        let view = RowView::new(row, &schema, &visible_positions);
        assert!(view.column_value_by("id").unwrap().is_none());
        assert_eq!(
            &ColumnValue::text("relop"),
            view.column_value_by("name").unwrap().unwrap()
        );
    }
    #[test]
    fn project_row_view() {
        let schema = schema!["id" => ColumnType::Int, "name" => ColumnType::Text].unwrap();

        let row = row![200, "relop"];

        let visible_positions = vec![0, 1];
        let view = RowView::new(row, &schema, &visible_positions);
        assert_eq!(
            &ColumnValue::int(200),
            view.column_value_by("id").unwrap().unwrap()
        );
        assert_eq!(
            &ColumnValue::text("relop"),
            view.column_value_by("name").unwrap().unwrap()
        );

        let projection = vec![1];
        let projected_view = view.project(&projection);
        assert!(projected_view.column_value_by("id").unwrap().is_none());
        assert_eq!(
            &ColumnValue::text("relop"),
            projected_view.column_value_by("name").unwrap().unwrap()
        );
    }

    #[test]
    fn attempt_to_get_ambiguous_column() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("employees.id", ColumnType::Int)
            .unwrap()
            .add_column("departments.id", ColumnType::Int)
            .unwrap();
        let row = row![1, 2];
        let visible_positions = vec![0, 1];
        let view = RowView::new(row, &schema, &visible_positions);

        let result = view.column_value_by("id");
        assert!(matches!(
            result,
            Err(schema::error::SchemaError::AmbiguousColumnName(ref column_name)) if column_name == "id"
        ));
    }

    #[test]
    fn merge_row_views() {
        let left_schema = schema!["id" => ColumnType::Int].unwrap();
        let left_row = row![1];
        let left_visible = vec![0];
        let left_view = RowView::new(left_row, &left_schema, &left_visible);

        let right_schema = schema!["name" => ColumnType::Text].unwrap();
        let right_row = row!["relop"];
        let right_visible = vec![0];
        let right_view = RowView::new(right_row, &right_schema, &right_visible);

        let merged_row = left_view.merge(&right_view);
        assert_eq!(
            merged_row,
            Row::filled(vec![ColumnValue::int(1), ColumnValue::text("relop")])
        );
    }
}

#[cfg(test)]
mod row_view_comparator_tests {
    use super::*;
    use crate::schema;
    use crate::types::column_type::ColumnType;
    use crate::{asc, desc, row};
    use std::cmp::Ordering;

    #[test]
    fn compare_row_views_on_single_column_ascending() {
        let schema = schema!["id" => ColumnType::Int].unwrap();
        let ordering_keys = vec![asc!("id")];
        let comparator = RowViewComparator::new(&schema, &ordering_keys).unwrap();

        let row1 = row![1];
        let row2 = row![2];

        let visible_positions = [0];
        let row_view1 = RowView::new(row1, &schema, &visible_positions);
        let row_view2 = RowView::new(row2, &schema, &visible_positions);

        assert_eq!(comparator.compare(&row_view1, &row_view2), Ordering::Less);
    }

    #[test]
    fn compare_row_views_on_multiple_columns_ascending() {
        let schema = schema!["id" => ColumnType::Int, "rank" => ColumnType::Int].unwrap();
        let ordering_keys = vec![asc!("id"), asc!("rank")];
        let comparator = RowViewComparator::new(&schema, &ordering_keys).unwrap();

        let row1 = row![1, 10];
        let row2 = row![2, 10];

        let visible_positions = [0, 1];
        let row_view1 = RowView::new(row1, &schema, &visible_positions);
        let row_view2 = RowView::new(row2, &schema, &visible_positions);

        assert_eq!(comparator.compare(&row_view1, &row_view2), Ordering::Less);
    }

    #[test]
    fn compare_row_views_on_multiple_columns_with_same_value_ascending() {
        let schema = schema!["id" => ColumnType::Int, "rank" => ColumnType::Int].unwrap();
        let ordering_keys = vec![asc!("id"), asc!("rank")];
        let comparator = RowViewComparator::new(&schema, &ordering_keys).unwrap();

        let row1 = row![1, 10];
        let row2 = row![1, 20];

        let visible_positions = [0, 1];
        let row_view1 = RowView::new(row1, &schema, &visible_positions);
        let row_view2 = RowView::new(row2, &schema, &visible_positions);

        assert_eq!(comparator.compare(&row_view1, &row_view2), Ordering::Less);
    }

    #[test]
    fn compare_row_views_on_multiple_columns_with_same_value_and_mixed_directions() {
        let schema = schema!["id" => ColumnType::Int, "rank" => ColumnType::Int].unwrap();
        let ordering_keys = vec![asc!("id"), desc!("rank")];
        let comparator = RowViewComparator::new(&schema, &ordering_keys).unwrap();

        let row1 = row![1, 10];
        let row2 = row![1, 20];

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
        let schema = schema!["id" => ColumnType::Int].unwrap();
        let ordering_keys = vec![asc!("id"), desc!("rank")];

        let result = RowViewComparator::new(&schema, &ordering_keys);
        assert!(
            matches!(result, Err(RowViewComparatorError::UnknownColumn(column)) if column == "rank")
        );
    }

    #[test]
    fn attempt_compare_row_views_with_ambiguous_column() {
        let mut schema = Schema::new();
        schema = schema
            .add_column("employees.id", ColumnType::Int)
            .unwrap()
            .add_column("departments.id", ColumnType::Int)
            .unwrap();
        let ordering_keys = vec![asc!("id")];

        let result = RowViewComparator::new(&schema, &ordering_keys);
        assert!(
            matches!(result, Err(RowViewComparatorError::UnknownColumn(column)) if column == "id")
        );
    }
}
