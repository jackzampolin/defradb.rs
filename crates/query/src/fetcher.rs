//! Compatibility facade for query execution fetcher traits.

pub use query_plan::fetcher::{
    CommitsQueryOptions, DocFetcher, FetchByIdsResult, IndexScanResult,
};
pub use query_parse::{CollectionProvider, StaticCollectionProvider};
