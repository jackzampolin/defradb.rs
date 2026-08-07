use async_trait::async_trait;

use crate::corekv::{Iterator, KvPair, Result};

/// Iterator over a range that cannot contain any key.
pub(crate) struct EmptyIterator;

impl crate::corekv::private::Sealed for EmptyIterator {}

#[async_trait]
impl Iterator for EmptyIterator {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    async fn seek(&mut self, _key: &[u8]) -> Result<bool> {
        Ok(false)
    }

    async fn reset(&mut self) -> Result<()> {
        Ok(())
    }
}
