use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// https://stackoverflow.com/questions/60882381/what-is-the-fastest-correct-way-to-detect-that-there-are-no-duplicates-in-a-json
#[derive(PartialEq, Debug)]
pub struct HashValue(pub Value);

impl Eq for HashValue {}

impl Hash for HashValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use Value::*;
        match self.0 {
            Null => state.write_u32(3_221_225_473), // chosen randomly
            Bool(ref b) => b.hash(state),
            Number(ref n) => {
                if let Some(x) = n.as_u64() {
                    x.hash(state);
                } else if let Some(x) = n.as_i64() {
                    x.hash(state);
                } else if let Some(x) = n.as_f64() {
                    // `f64` does not implement `Hash`. However, floats in JSON are guaranteed to be
                    // finite, so we can use the `Hash` implementation in the `ordered-float` crate.
                    ordered_float::NotNan::new(x).unwrap().hash(state);
                }
            }
            String(ref s) => s.hash(state),
            Array(ref v) => {
                for x in v {
                    HashValue(x.clone()).hash(state);
                }
            }
            Object(ref map) => {
                let mut hash = 0;
                for (k, v) in map {
                    // We have no way of building a new hasher of type `H`, so we
                    // hardcode using the default hasher of a hash map.
                    let mut item_hasher = DefaultHasher::new();
                    k.hash(&mut item_hasher);
                    HashValue(v.clone()).hash(&mut item_hasher);
                    hash ^= item_hasher.finish();
                }
                state.write_u64(hash);
            }
        }
    }
}
