pub(crate) struct OrderingKey {
    pub(crate) column: String,
    pub(crate) direction: OrderingDirection,
}

pub(crate) enum OrderingDirection {
    Ascending,
    Descending,
}
