use dbschema::{Filter, FilterPath, TimeRange};
use serde_json::{json, Value};

pub fn filter_active_single() -> Filter {
    FilterPath::new()
        .field("value")
        .field("version")
        .field("active")
        .field("to")
        .eq(Value::Null)
}

pub fn filter_active_dual() -> Filter {
    FilterPath::new()
        .field("value")
        .field("version")
        .field("active")
        .some()
        .field("to")
        .eq(Value::Null)
}

pub fn filter_current_dual() -> Filter {
    FilterPath::new()
        .field("value")
        .field("version")
        .field("current")
        .field("to")
        .eq(Value::Null)
}

// pub fn filter_uncommitted() -> Filter {
//     FilterPath::new()
//         .field("value")
//         .field("version")
//         .field("active")
//         .eq(Value::Null)
//         .and(
//             FilterPath::new()
//                 .field("value")
//                 .field("version")
//                 .field("current")
//                 .field("to")
//                 .eq(Value::Null),
//         )
//         .or(FilterPath::new()
//             .field("value")
//             .field("version")
//             .field("active")
//             .some()
//             .field("to")
//             .eq(Value::Null)
//             .and(Filter::negate(
//                 FilterPath::new()
//                     .field("value")
//                     .field("version")
//                     .field("current")
//                     .field("to")
//                     .eq(Value::Null),
//             )))
// }

pub fn range_filter(range: TimeRange) -> Filter {
    Filter::All(
        IntoIterator::into_iter([
            range
                .to
                .map(|to| FilterPath::new().field("from").le(json!(to))),
            range.from.map(|from| {
                FilterPath::new()
                    .field("to")
                    .eq(Value::Null)
                    .or(FilterPath::new().field("to").some().gt(json!(from)))
            }),
        ])
        .flatten()
        .collect(),
    )
}
