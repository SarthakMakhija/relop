/// `Keywords` holds a list of reserved words for the SQL dialect.
/// It provides functionality to check if a valid identifier is a keyword.
pub(crate) struct Keywords {
    words: &'static [&'static str],
}

impl Keywords {
    /// Creates a `Keywords` instance with the default set of reserved words.
    ///
    /// The default keywords include: "show", "tables", "describe", "table", "select", "from" etc.
    pub(crate) fn new_with_default_keywords() -> Keywords {
        Self::new_with_keywords(&[
            "show", "tables", "describe", "table", "select", "from", "limit", "order", "by", "asc",
            "desc",
        ])
    }

    /// Creates a `Keywords` instance with a custom set of reserved words.
    ///
    /// # Arguments
    ///
    /// * `words` - A static slice of static string slices representing the keywords.
    pub(crate) fn new_with_keywords(words: &'static [&'static str]) -> Keywords {
        Self { words }
    }

    /// Checks if the given identifier is a reserved keyword.
    ///
    /// The check is case-insensitive.
    ///
    /// # Arguments
    ///
    /// * `identifier` - The string to check against the keywords.
    ///
    /// # Returns
    ///
    /// `true` if the identifier matches any of the keywords (case-insensitive), `false` otherwise.
    pub(crate) fn contains(&self, identifier: &str) -> bool {
        self.words
            .iter()
            .any(|word| word.eq_ignore_ascii_case(identifier))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_a_keyword() {
        let keywords = Keywords::new_with_keywords(&["select", "from"]);
        assert!(keywords.contains("select"));
    }

    #[test]
    fn is_a_keyword_with_case_ignored() {
        let keywords = Keywords::new_with_keywords(&["select", "from"]);
        assert!(keywords.contains("SELECT"));
    }

    #[test]
    fn is_not_a_keyword() {
        let keywords = Keywords::new_with_keywords(&["select", "from"]);
        assert!(!keywords.contains("table"));
    }
}
