use std::sync::Arc;

use anyhow::{bail, Result};
use thiserror::Error;
use tokio::sync::Semaphore;

use crate::dense;
use crate::snapshot::{Manifest, Snapshot};

#[derive(Clone, Copy, Debug)]
pub struct PirServiceConfig {
    pub max_in_flight: usize,
    pub max_batch_size: usize,
    pub worker_threads: usize,
}

impl Default for PirServiceConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 1,
            max_batch_size: 128,
            worker_threads: 2,
        }
    }
}

#[derive(Debug, Error)]
pub enum EvaluationError {
    #[error("PIR evaluator is at capacity")]
    Overloaded,
    #[error("invalid PIR query: {0}")]
    Invalid(#[source] anyhow::Error),
    #[error("PIR evaluator worker failed: {0}")]
    Worker(String),
}

#[derive(Clone)]
pub struct PirService {
    snapshot: Arc<Snapshot>,
    permits: Arc<Semaphore>,
    evaluator: Arc<dense::ParallelEvaluator>,
    max_batch_size: usize,
}

impl PirService {
    pub fn new(snapshot: Arc<Snapshot>, config: PirServiceConfig) -> Result<Self> {
        if config.max_in_flight == 0 || config.max_batch_size == 0 || config.worker_threads == 0 {
            bail!("PIR service limits must be non-zero");
        }
        if snapshot.manifest.lookup_page_count > config.max_batch_size {
            bail!(
                "snapshot requires {} lookup pages, service limit is {}",
                snapshot.manifest.lookup_page_count,
                config.max_batch_size
            );
        }
        Ok(Self {
            snapshot,
            permits: Arc::new(Semaphore::new(config.max_in_flight)),
            evaluator: Arc::new(dense::ParallelEvaluator::new(config.worker_threads)?),
            max_batch_size: config.max_batch_size,
        })
    }

    pub fn manifest(&self) -> &Manifest {
        &self.snapshot.manifest
    }

    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    pub async fn evaluate_batch(
        &self,
        query_shares: Vec<Vec<u8>>,
    ) -> std::result::Result<Vec<Vec<u8>>, EvaluationError> {
        if query_shares.is_empty() || query_shares.len() > self.max_batch_size {
            return Err(EvaluationError::Invalid(anyhow::anyhow!(
                "batch size must be between 1 and {}",
                self.max_batch_size
            )));
        }
        let expected = dense::query_size(self.snapshot.manifest.bucket_count);
        if let Some(query) = query_shares.iter().find(|query| query.len() != expected) {
            return Err(EvaluationError::Invalid(anyhow::anyhow!(
                "query share has {} bytes, expected {expected}",
                query.len()
            )));
        }

        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| EvaluationError::Overloaded)?;
        let snapshot = Arc::clone(&self.snapshot);
        let evaluator = Arc::clone(&self.evaluator);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            evaluator.answer_batch(snapshot.view(), &query_shares)
        })
        .await
        .map_err(|error| EvaluationError::Worker(error.to_string()))?
        .map_err(EvaluationError::Invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::Snapshot;

    #[tokio::test]
    async fn evaluates_a_bounded_batch() {
        let snapshot = Arc::new(Snapshot::benchmark(64, 32, 1).unwrap());
        let service = PirService::new(
            Arc::clone(&snapshot),
            PirServiceConfig {
                max_in_flight: 1,
                max_batch_size: 2,
                worker_threads: 2,
            },
        )
        .unwrap();
        let queries = vec![vec![0u8; dense::query_size(64)]; 2];
        assert_eq!(service.evaluate_batch(queries).await.unwrap().len(), 2);
        let oversized = vec![vec![0u8; dense::query_size(64)]; 3];
        assert!(matches!(
            service.evaluate_batch(oversized).await,
            Err(EvaluationError::Invalid(_))
        ));

        let _permit = service.permits.clone().try_acquire_owned().unwrap();
        let query = vec![vec![0u8; dense::query_size(64)]];
        assert!(matches!(
            service.evaluate_batch(query).await,
            Err(EvaluationError::Overloaded)
        ));
    }
}
