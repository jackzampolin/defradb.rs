//! Query runner initialization helpers for Node startup.

use std::sync::Arc;

use tracing::info;

use super::node::Node;
use crate::config::{AcpDocumentType, Config};
use identity::Did;

pub(super) struct QueryRunnerSetup<S: storage::corekv::Store + 'static> {
    pub(super) runner: Arc<dyn query::executor::QueryExecutor>,
    pub(super) rest_ops: Arc<dyn query::rest::RestOperations>,
    pub(super) registry: Arc<db::DbTransactionRegistry<S>>,
    pub(super) collection_provider: Arc<dyn query::CollectionProvider>,
}

impl Node {
    pub(super) fn setup_query_runner<S>(
        database: Arc<db::DB<S>>,
        config: &Config,
        user_did: Option<&Did>,
        document_acp: Arc<dyn acp::DocumentACP>,
        nac_adapter: Option<Arc<crate::nac_adapter::NacAdapter>>,
        mutator: Arc<dyn query::mutator::DocMutator>,
        txn_broadcaster: Option<Arc<dyn db::event_emission::TxnBroadcaster>>,
    ) -> QueryRunnerSetup<S>
    where
        S: storage::corekv::Store + 'static,
    {
        let fetcher = db::LensedAutoCommitFetcher::new(database.clone());
        let registry = Arc::new(match txn_broadcaster {
            Some(b) => db::DbTransactionRegistry::with_broadcaster(database.clone(), b),
            None => db::DbTransactionRegistry::new(database.clone()),
        });
        let collection_provider: Arc<dyn query::CollectionProvider> =
            db::DbCollectionProvider::new_arc(database.clone());
        info!(
            "Collection provider configured ({} collection(s) available)",
            database.list_collections().map(|c| c.len()).unwrap_or(0)
        );

        let mut query_runner = query::QueryRunner::with_arc_registry_and_provider(
            fetcher,
            collection_provider.clone(),
            registry.clone(),
        )
        .with_mutator(mutator)
        .with_acp(document_acp)
        .with_lens_store(database.lens_store().clone())
        .with_query_timeout(config.api.query_timeout)
        .with_query_limits(query::QueryLimits {
            max_query_depth: config.api.query_max_depth,
            max_query_width: config.api.query_max_width,
            max_filter_depth: config.api.query_max_filter_depth,
        });

        if !config.datastore.no_encryption {
            info!("CRDT delta encryption enabled");
        }

        if let Some(did) = user_did {
            if config.acp.document_type != AcpDocumentType::SourceHub
                && config.acp.document_type != AcpDocumentType::HubRs
            {
                info!("Query runner configured with default identity for ACP");
                query_runner = query_runner.with_default_identity(did.clone());
            }
        }

        if let Some(adapter) = &nac_adapter {
            query_runner = query_runner.with_nac(adapter.clone() as Arc<dyn query::NacChecker>);
        }

        let runner = Arc::new(query_runner);
        let rest_ops: Arc<dyn query::rest::RestOperations> =
            Arc::new(query::rest::RestOperationsImpl::new(runner.clone()));
        let runner: Arc<dyn query::executor::QueryExecutor> = runner;

        QueryRunnerSetup {
            runner,
            rest_ops,
            registry,
            collection_provider,
        }
    }
}
