/// Convert a string to a valid (part of an) index name.
/// The goal is to get a human-readable index name. The
/// operation is *not* designed to be reversible or to
/// preserve uniqueness, although meaningfully different
/// ids (differing in more than case or punctuation)
/// should remain different after sanitition.
pub fn sanitize_index_id(id: &str) -> String {
    id.chars()
        .flat_map(|c| c.to_lowercase())
        .filter_map(|c| match c {
            'a'..='z' | '0'..='9' | '-' | '.' => Some(c),
            ' ' | '_' => Some('-'),
            _ => None,
        })
        .collect()
}
