#[cfg(test)]
mod unit_tests {
    use super::super::*;

    #[test]
    fn test_commits_query_options_default() {
        let opts = CommitsQueryOptions::default();
        assert!(opts.doc_id.is_none());
        assert!(opts.cid.is_none());
        assert!(opts.depth.is_none());
        assert!(opts.field_name.is_none());
    }
}

#[cfg(test)]
mod additional_tests {
    use cid::Cid;
    use std::str::FromStr;

    #[test]
    fn test_invalid_cid_parsing() {
        let result = Cid::from_str("fhbnjfahfhfhanfhga");
        assert!(result.is_err(), "Invalid CID should fail to parse");
    }

    #[test]
    fn test_valid_cid_parsing() {
        let result = Cid::from_str("bafyreiajq6jmyblg2b6vupjdapzkaodbt7kkwqp4fijekdvydnyxvr4y7q");
        assert!(result.is_ok(), "Valid CID should parse");
    }

    #[test]
    fn test_unknown_cid_parsing() {
        let result = Cid::from_str("bafybeid57gpbwi4i6bg7g35hhhhhhhhhhhhhhhhhhhhhhhdoesnotexist");
        let _ = result;
    }

    #[test]
    fn test_truly_invalid_cid_parsing() {
        let result = Cid::from_str("fhbnjfahfhfhanfhga");
        assert!(result.is_err(), "Truly invalid CID should fail to parse");
    }

    #[test]
    fn test_looks_like_cidv1() {
        use crate::commits_fetcher::CommitsFetcher;
        use storage::backends::memory::MemoryStore;

        assert!(CommitsFetcher::<MemoryStore>::looks_like_cidv1(
            "bafybeid57gpbwi4i6bg7g35hhhhhhhhhhhhhhhhhhhhhhhdoesnotexist"
        ));
        assert!(CommitsFetcher::<MemoryStore>::looks_like_cidv1(
            "bafyreiajq6jmyblg2b6vupjdapzkaodbt7kkwqp4fijekdvydnyxvr4y7q"
        ));

        assert!(!CommitsFetcher::<MemoryStore>::looks_like_cidv1(
            "fhbnjfahfhfhanfhga"
        ));
        assert!(!CommitsFetcher::<MemoryStore>::looks_like_cidv1("short"));
        assert!(!CommitsFetcher::<MemoryStore>::looks_like_cidv1(
            "randomtext"
        ));
    }
}
