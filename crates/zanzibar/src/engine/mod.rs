mod cache;
mod evaluate;
mod trace;

use std::sync::Arc;

use crate::did::Did;

use crate::error::Result;
use crate::lookup::PolicyLookupTable;
use crate::store::ZanzibarStore;
use crate::types::Policy;

use cache::{CheckCache, NodeId, NodeTrail};

#[derive(Debug, Clone)]
pub struct PermissionCheckRequest<'a> {
    pub policy_id: &'a str,
    pub resource: &'a str,
    pub object_id: &'a str,
    pub relation: &'a str,
    pub subject: &'a Did,
}

impl<'a> PermissionCheckRequest<'a> {
    pub fn new(
        policy_id: &'a str,
        resource: &'a str,
        object_id: &'a str,
        relation: &'a str,
        subject: &'a Did,
    ) -> Self {
        Self {
            policy_id,
            resource,
            object_id,
            relation,
            subject,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PermissionExplanation {
    pub granted: bool,
    pub resource: String,
    pub object_id: String,
    pub relation: String,
    pub subject: String,
    pub trace: EvaluationTrace,
}

#[derive(Debug, Clone, Default)]
pub struct EvaluationTrace {
    pub steps: Vec<EvaluationStep>,
}

impl EvaluationTrace {
    pub(crate) fn new() -> Self {
        Self { steps: Vec::new() }
    }

    pub(crate) fn add_step(&mut self, step: EvaluationStep) {
        self.steps.push(step);
    }
}

#[derive(Debug, Clone)]
pub struct EvaluationStep {
    pub expression_type: String,
    pub resource: String,
    pub object_id: String,
    pub relation: String,
    pub result: StepResult,
    pub details: Option<String>,
}

#[derive(Debug, Clone)]
pub enum StepResult {
    Granted,
    Denied,
    Skipped,
    Continuing,
}

impl std::fmt::Display for StepResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StepResult::Granted => write!(f, "GRANTED"),
            StepResult::Denied => write!(f, "DENIED"),
            StepResult::Skipped => write!(f, "SKIPPED"),
            StepResult::Continuing => write!(f, "..."),
        }
    }
}

pub struct PermissionEngine<S: ZanzibarStore> {
    store: Arc<S>,
    pub lookup: PolicyLookupTable,
}

impl<S: ZanzibarStore> PermissionEngine<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            lookup: PolicyLookupTable::new(),
        }
    }

    pub fn add_policy(&mut self, policy: &Policy) {
        self.lookup.add_policy(policy);
    }

    pub fn remove_policy(&mut self, policy_id: &str) {
        self.lookup.remove_policy(policy_id);
    }

    pub fn update_policy(&mut self, policy: &Policy) {
        self.lookup.update_policy(policy);
    }

    pub async fn load_policy(&mut self, policy_id: &str) -> Result<()> {
        if let Some(policy) = self.store.get_policy(policy_id).await? {
            self.lookup.add_policy(&policy);
        }
        Ok(())
    }

    pub async fn reload_policy(&mut self, policy_id: &str) -> Result<()> {
        self.lookup.remove_policy(policy_id);
        self.load_policy(policy_id).await
    }

    pub fn clear_cache(&mut self) {
        self.lookup.clear();
    }

    pub async fn check(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Did,
    ) -> Result<bool> {
        let expression = self.lookup.get_expression(policy_id, resource, relation)?;

        let node_id = NodeId::new(resource, object_id, relation);
        let trail = NodeTrail::new().with_node(node_id);

        let cache = Arc::new(CheckCache::new());

        self.evaluate_expr_cached(
            policy_id, resource, object_id, relation, subject, expression, trail, cache,
        )
        .await
    }

    pub async fn check_many(&self, requests: &[PermissionCheckRequest<'_>]) -> Vec<Result<bool>> {
        let cache = Arc::new(CheckCache::new());

        let mut results = Vec::with_capacity(requests.len());

        for req in requests {
            let result = self
                .check_with_cache(
                    req.policy_id,
                    req.resource,
                    req.object_id,
                    req.relation,
                    req.subject,
                    cache.clone(),
                )
                .await;
            results.push(result);
        }

        results
    }

    async fn check_with_cache(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Did,
        cache: Arc<CheckCache>,
    ) -> Result<bool> {
        let expression = self.lookup.get_expression(policy_id, resource, relation)?;

        let node_id = NodeId::new(resource, object_id, relation);
        let trail = NodeTrail::new().with_node(node_id);

        self.evaluate_expr_cached(
            policy_id, resource, object_id, relation, subject, expression, trail, cache,
        )
        .await
    }

    pub async fn explain(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Did,
    ) -> Result<PermissionExplanation> {
        let expression = self.lookup.get_expression(policy_id, resource, relation)?;

        let node_id = NodeId::new(resource, object_id, relation);
        let trail = NodeTrail::new().with_node(node_id);

        let cache = Arc::new(CheckCache::new());
        let mut trace = EvaluationTrace::new();

        let granted = self
            .evaluate_expr_with_trace(
                policy_id, resource, object_id, relation, subject, expression, trail, cache,
                &mut trace,
            )
            .await?;

        Ok(PermissionExplanation {
            granted,
            resource: resource.to_string(),
            object_id: object_id.to_string(),
            relation: relation.to_string(),
            subject: subject.to_string(),
            trace,
        })
    }
}
