pub(crate) struct Keywords {
    words: &'static [&'static str],
}

impl Keywords {
    pub(crate) fn new_with_default_keywords() -> Keywords {
        Self::new_with_keywords(&["show", "tables", "describe", "table", "select", "from"])
    }

    pub(crate) fn new_with_keywords(words: &'static [&'static str]) -> Keywords {
        Self { words }
    }

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
