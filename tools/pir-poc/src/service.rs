use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{bail, Result};
use thiserror::Error;
use tokio::sync::Semaphore;

use crate::dense;
use crate::snapshot::{CatalogManifest, Manifest, Snapshot, SnapshotCatalog};

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

#[derive(Debug)]
pub struct WindowEvaluation {
    pub window_id: String,
    pub snapshot_id: String,
    pub query_shares: Vec<Vec<u8>>,
}

#[derive(Debug)]
pub struct WindowEvaluationResult {
    pub window_id: String,
    pub snapshot_id: String,
    pub answer_shares: Vec<Vec<u8>>,
}

#[derive(Clone)]
pub struct PirService {
    catalog: Arc<SnapshotCatalog>,
    permits: Arc<Semaphore>,
    evaluator: Arc<dense::ParallelEvaluator>,
    max_batch_size: usize,
}

impl PirService {
    pub fn new(snapshot: Arc<Snapshot>, config: PirServiceConfig) -> Result<Self> {
        Self::from_catalog(Arc::new(SnapshotCatalog::global_only(snapshot)?), config)
    }

    pub fn from_catalog(catalog: Arc<SnapshotCatalog>, config: PirServiceConfig) -> Result<Self> {
        if config.max_in_flight == 0 || config.max_batch_size == 0 || config.worker_threads == 0 {
            bail!("PIR service limits must be non-zero");
        }
        for (name, snapshot) in std::iter::once(("global", catalog.global())).chain(
            catalog
                .windows()
                .iter()
                .map(|(window_id, snapshot)| (window_id.as_str(), snapshot)),
        ) {
            if snapshot.manifest.lookup_page_count > config.max_batch_size {
                bail!(
                    "snapshot {name} requires {} lookup pages, service limit is {}",
                    snapshot.manifest.lookup_page_count,
                    config.max_batch_size
                );
            }
        }
        Ok(Self {
            catalog,
            permits: Arc::new(Semaphore::new(config.max_in_flight)),
            evaluator: Arc::new(dense::ParallelEvaluator::new(config.worker_threads)?),
            max_batch_size: config.max_batch_size,
        })
    }

    pub fn manifest(&self) -> &Manifest {
        &self.catalog.global().manifest
    }

    pub fn catalog_manifest(&self) -> &CatalogManifest {
        self.catalog.manifest()
    }

    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    pub fn max_query_size(&self) -> usize {
        std::iter::once(self.catalog.global())
            .chain(self.catalog.windows().values())
            .map(|snapshot| dense::query_size(snapshot.manifest.bucket_count))
            .max()
            .unwrap_or(0)
    }

    pub async fn evaluate_batch(
        &self,
        query_shares: Vec<Vec<u8>>,
    ) -> std::result::Result<Vec<Vec<u8>>, EvaluationError> {
        self.validate_queries(self.catalog.global(), &query_shares, query_shares.len())?;

        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| EvaluationError::Overloaded)?;
        let snapshot = Arc::clone(self.catalog.global());
        let evaluator = Arc::clone(&self.evaluator);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            evaluator.answer_batch(snapshot.view(), &query_shares)
        })
        .await
        .map_err(|error| EvaluationError::Worker(error.to_string()))?
        .map_err(EvaluationError::Invalid)
    }

    pub async fn evaluate_windows(
        &self,
        requests: Vec<WindowEvaluation>,
    ) -> std::result::Result<Vec<WindowEvaluationResult>, EvaluationError> {
        let total_queries = requests
            .iter()
            .try_fold(0usize, |total, request| {
                total.checked_add(request.query_shares.len())
            })
            .ok_or_else(|| EvaluationError::Invalid(anyhow::anyhow!("batch size overflow")))?;
        if requests.is_empty() || total_queries == 0 || total_queries > self.max_batch_size {
            return Err(EvaluationError::Invalid(anyhow::anyhow!(
                "total window batch size must be between 1 and {}",
                self.max_batch_size
            )));
        }

        let mut seen = BTreeSet::new();
        let mut work = Vec::with_capacity(requests.len());
        for request in requests {
            if !seen.insert(request.window_id.clone()) {
                return Err(EvaluationError::Invalid(anyhow::anyhow!(
                    "duplicate public window {}",
                    request.window_id
                )));
            }
            let snapshot = self.catalog.window(&request.window_id).ok_or_else(|| {
                EvaluationError::Invalid(anyhow::anyhow!(
                    "unknown public window {}",
                    request.window_id
                ))
            })?;
            if request.snapshot_id != snapshot.manifest.snapshot_id {
                return Err(EvaluationError::Invalid(anyhow::anyhow!(
                    "snapshot ID mismatch for public window {}",
                    request.window_id
                )));
            }
            self.validate_queries(snapshot, &request.query_shares, total_queries)?;
            work.push((request, Arc::clone(snapshot)));
        }

        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| EvaluationError::Overloaded)?;
        let evaluator = Arc::clone(&self.evaluator);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work.into_iter()
                .map(|(request, snapshot)| {
                    let answer_shares =
                        evaluator.answer_batch(snapshot.view(), &request.query_shares)?;
                    Ok(WindowEvaluationResult {
                        window_id: request.window_id,
                        snapshot_id: request.snapshot_id,
                        answer_shares,
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .await
        .map_err(|error| EvaluationError::Worker(error.to_string()))?
        .map_err(EvaluationError::Invalid)
    }

    fn validate_queries(
        &self,
        snapshot: &Snapshot,
        query_shares: &[Vec<u8>],
        total_queries: usize,
    ) -> std::result::Result<(), EvaluationError> {
        if query_shares.is_empty() || total_queries > self.max_batch_size {
            return Err(EvaluationError::Invalid(anyhow::anyhow!(
                "batch size must be between 1 and {}",
                self.max_batch_size
            )));
        }
        let expected = dense::query_size(snapshot.manifest.bucket_count);
        if let Some(query) = query_shares.iter().find(|query| query.len() != expected) {
            return Err(EvaluationError::Invalid(anyhow::anyhow!(
                "query share has {} bytes, expected {expected}",
                query.len()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::Snapshot;
    use std::collections::BTreeMap;

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

    #[tokio::test]
    async fn window_batches_use_each_snapshot_shape_and_one_total_limit() {
        let global = Arc::new(Snapshot::benchmark(64, 32, 1).unwrap());
        let old = Arc::new(Snapshot::benchmark(32, 32, 2).unwrap());
        let new = Arc::new(Snapshot::benchmark(128, 32, 3).unwrap());
        let catalog = Arc::new(
            SnapshotCatalog::new(
                global,
                BTreeMap::from([
                    ("2026-W31".into(), Arc::clone(&old)),
                    ("2026-W32".into(), Arc::clone(&new)),
                ]),
            )
            .unwrap(),
        );
        let service = PirService::from_catalog(
            catalog,
            PirServiceConfig {
                max_in_flight: 1,
                max_batch_size: 2,
                worker_threads: 2,
            },
        )
        .unwrap();
        let results = service
            .evaluate_windows(vec![
                WindowEvaluation {
                    window_id: "2026-W31".into(),
                    snapshot_id: old.manifest.snapshot_id.clone(),
                    query_shares: vec![vec![0; dense::query_size(32)]],
                },
                WindowEvaluation {
                    window_id: "2026-W32".into(),
                    snapshot_id: new.manifest.snapshot_id.clone(),
                    query_shares: vec![vec![0; dense::query_size(128)]],
                },
            ])
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| result.answer_shares.len() == 1));

        let oversized = vec![WindowEvaluation {
            window_id: "2026-W31".into(),
            snapshot_id: old.manifest.snapshot_id.clone(),
            query_shares: vec![vec![0; dense::query_size(32)]; 3],
        }];
        assert!(matches!(
            service.evaluate_windows(oversized).await,
            Err(EvaluationError::Invalid(_))
        ));
    }
}
