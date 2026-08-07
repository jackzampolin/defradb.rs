use async_trait::async_trait;

use crate::corekv::{Error, Iterator, KvPair, Result};

/// Iterator over a range that cannot contain any key.
pub(crate) struct EmptyIterator {
    closed: bool,
}

impl EmptyIterator {
    pub(crate) fn new() -> Self {
        Self { closed: false }
    }
}

impl crate::corekv::private::Sealed for EmptyIterator {}

#[async_trait]
impl Iterator for EmptyIterator {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }

    async fn seek(&mut self, _key: &[u8]) -> Result<bool> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        Ok(false)
    }

    async fn reset(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        Ok(())
    }

    fn is_valid(&self) -> bool {
        !self.closed
    }
}
