//! Permission evaluation engine.
//!
//! Implements goal-tree search with cycle detection for evaluating
//! Zanzibar permission expressions.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use defra_core::thread_bounds::MaybeBoxFuture;

use async_lock::RwLock;
use identity::Did;
use sha2::{Digest, Sha256};

use super::expression::RelationExpression;
use super::lookup::PolicyLookupTable;
use super::store::ZanzibarStore;
use super::types::{Policy, Subject};
use crate::error::Result;

/// A request for a permission check (used in batch operations).
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

/// Explanation of a permission decision, including the evaluation trace.
#[derive(Debug, Clone)]
pub struct PermissionExplanation {
    /// Whether permission was granted.
    pub granted: bool,
    /// The resource type checked.
    pub resource: String,
    /// The object ID checked.
    pub object_id: String,
    /// The relation checked.
    pub relation: String,
    /// The subject (DID) checked.
    pub subject: String,
    /// Detailed trace of the evaluation.
    pub trace: EvaluationTrace,
}

/// Trace of permission evaluation steps.
#[derive(Debug, Clone, Default)]
pub struct EvaluationTrace {
    /// Steps taken during evaluation, in order.
    pub steps: Vec<EvaluationStep>,
}

impl EvaluationTrace {
    fn new() -> Self {
        Self { steps: Vec::new() }
    }

    fn add_step(&mut self, step: EvaluationStep) {
        self.steps.push(step);
    }
}

/// A single step in permission evaluation.
#[derive(Debug, Clone)]
pub struct EvaluationStep {
    /// Type of expression evaluated.
    pub expression_type: String,
    /// Resource being evaluated.
    pub resource: String,
    /// Object ID being evaluated.
    pub object_id: String,
    /// Relation being evaluated.
    pub relation: String,
    /// Result of this step.
    pub result: StepResult,
    /// Additional context/details.
    pub details: Option<String>,
}

/// Result of an evaluation step.
#[derive(Debug, Clone)]
pub enum StepResult {
    /// Step succeeded (returned true).
    Granted,
    /// Step failed (returned false).
    Denied,
    /// Step was skipped (e.g., due to cycle detection).
    Skipped,
    /// Step is continuing to evaluate sub-expressions.
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

/// Node identifier for cycle detection.
///
/// Uniquely identifies a permission check node in the evaluation tree.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NodeId(String);

impl NodeId {
    /// Create a node ID from (resource, object_id, relation).
    fn new(resource: &str, object_id: &str, relation: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(resource.as_bytes());
        hasher.update(b"/");
        hasher.update(object_id.as_bytes());
        hasher.update(b"#");
        hasher.update(relation.as_bytes());
        let result = hasher.finalize();
        Self(format!("{:x}", result))
    }
}

/// Trail of visited nodes for cycle detection.
///
/// Tracks the path through the evaluation tree to detect cycles.
///
/// # Performance Note
/// The trail is cloned on each recursive call to maintain independent paths.
/// For very deep permission hierarchies, this could become expensive (O(n) per clone
/// where n is the depth). If this becomes a bottleneck, consider using the `im` crate
/// for persistent data structures which provide O(log n) cloning.
#[derive(Debug, Clone, Default)]
struct NodeTrail {
    visited: HashSet<NodeId>,
}

impl NodeTrail {
    fn new() -> Self {
        Self {
            visited: HashSet::new(),
        }
    }

    /// Check if a node has been visited (would create a cycle).
    fn contains(&self, node: &NodeId) -> bool {
        self.visited.contains(node)
    }

    /// Add a node to the trail.
    fn insert(&mut self, node: NodeId) {
        self.visited.insert(node);
    }

    /// Create a new trail with an additional node.
    fn with_node(&self, node: NodeId) -> Self {
        let mut new_trail = self.clone();
        new_trail.insert(node);
        new_trail
    }
}

/// Request-scoped cache for permission check results.
///
/// Caches the result of permission evaluations within a single top-level check
/// to avoid redundant computations when the same (resource, object_id, relation)
/// is checked multiple times during recursive evaluation.
///
/// The cache key includes the subject DID to ensure correct behavior when
/// checking permissions for different subjects.
#[derive(Debug, Default)]
struct CheckCache {
    /// Cache: (resource, object_id, relation, subject_hash) -> result
    results: RwLock<HashMap<String, bool>>,
}

impl CheckCache {
    fn new() -> Self {
        Self {
            results: RwLock::new(HashMap::new()),
        }
    }

    /// Generate a cache key for a permission check.
    fn cache_key(resource: &str, object_id: &str, relation: &str, subject: &Did) -> String {
        let mut hasher = Sha256::new();
        hasher.update(resource.as_bytes());
        hasher.update(b"/");
        hasher.update(object_id.as_bytes());
        hasher.update(b"#");
        hasher.update(relation.as_bytes());
        hasher.update(b"@");
        hasher.update(subject.to_string().as_bytes());
        let result = hasher.finalize();
        format!("{:x}", result)
    }

    /// Get a cached result if available.
    async fn get(
        &self,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Did,
    ) -> Option<bool> {
        let key = Self::cache_key(resource, object_id, relation, subject);
        self.results.read().await.get(&key).copied()
    }

    /// Store a result in the cache.
    async fn set(
        &self,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Did,
        result: bool,
    ) {
        let key = Self::cache_key(resource, object_id, relation, subject);
        self.results.write().await.insert(key, result);
    }
}

/// Permission evaluation engine.
///
/// Evaluates Zanzibar permission expressions using goal-tree search
/// with cycle detection. Supports all expression types:
/// - This: Direct tuple lookup
/// - ComputedUserset: Check different relation on same object
/// - TupleToUserset: Follow relation, then check computed relation
/// - Union: OR of expressions (short-circuit)
/// - Intersection: AND of expressions
/// - Difference: Left AND NOT right
pub struct PermissionEngine<S: ZanzibarStore> {
    store: Arc<S>,
    pub lookup: PolicyLookupTable,
}

impl<S: ZanzibarStore> PermissionEngine<S> {
    /// Create a new permission engine with the given store.
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            lookup: PolicyLookupTable::new(),
        }
    }

    /// Add a policy to the engine's lookup table.
    pub fn add_policy(&mut self, policy: &Policy) {
        self.lookup.add_policy(policy);
    }

    /// Remove a policy from the engine's lookup table.
    pub fn remove_policy(&mut self, policy_id: &str) {
        self.lookup.remove_policy(policy_id);
    }

    /// Update (reload) a policy in the engine's lookup table.
    pub fn update_policy(&mut self, policy: &Policy) {
        self.lookup.update_policy(policy);
    }

    /// Load policies from the store into the lookup table.
    pub async fn load_policy(&mut self, policy_id: &str) -> Result<()> {
        if let Some(policy) = self.store.get_policy(policy_id).await? {
            self.lookup.add_policy(&policy);
        }
        Ok(())
    }

    /// Reload a policy from the store (invalidates cache and reloads).
    pub async fn reload_policy(&mut self, policy_id: &str) -> Result<()> {
        self.lookup.remove_policy(policy_id);
        self.load_policy(policy_id).await
    }

    /// Clear all cached policies.
    pub fn clear_cache(&mut self) {
        self.lookup.clear();
    }

    /// Check if subject has permission on object.
    ///
    /// This is the main entry point for permission evaluation.
    /// Creates a request-scoped cache to avoid redundant evaluations
    /// during recursive permission checks.
    pub async fn check(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Did,
    ) -> Result<bool> {
        // Get the expression for this relation
        let expression = self.lookup.get_expression(policy_id, resource, relation)?;

        // Start evaluation with empty trail, but add initial node
        let node_id = NodeId::new(resource, object_id, relation);
        let trail = NodeTrail::new().with_node(node_id);

        // Create request-scoped cache for this check
        let cache = Arc::new(CheckCache::new());

        self.evaluate_expr_cached(
            policy_id, resource, object_id, relation, subject, expression, trail, cache,
        )
        .await
    }

    /// Check multiple permissions in a single batch operation.
    ///
    /// This is more efficient than calling `check` multiple times when checking
    /// the same subject against multiple resources/relations, as it shares the
    /// request-scoped cache across all checks.
    ///
    /// Returns a vector of results in the same order as the input requests.
    pub async fn check_many(&self, requests: &[PermissionCheckRequest<'_>]) -> Vec<Result<bool>> {
        // Create a shared cache for all checks in this batch
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

    /// Check permission with an existing cache (for batch operations).
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

    /// Check permission and return an explanation of the decision.
    ///
    /// This is useful for debugging and auditing. Returns a `PermissionExplanation`
    /// that describes why access was granted or denied, including the evaluation path.
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

    /// Evaluate an expression with caching support.
    #[allow(clippy::too_many_arguments)]
    fn evaluate_expr_cached<'a>(
        &'a self,
        policy_id: &'a str,
        resource: &'a str,
        object_id: &'a str,
        relation: &'a str,
        subject: &'a Did,
        expression: &'a RelationExpression,
        trail: NodeTrail,
        cache: Arc<CheckCache>,
    ) -> MaybeBoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            // Check cache first (for ComputedUserset which may re-evaluate same relation)
            if let Some(cached) = cache.get(resource, object_id, relation, subject).await {
                return Ok(cached);
            }

            // Evaluate the expression
            let result = self
                .evaluate_expr_inner(
                    policy_id,
                    resource,
                    object_id,
                    relation,
                    subject,
                    expression,
                    trail,
                    cache.clone(),
                )
                .await?;

            // Cache the result
            cache
                .set(resource, object_id, relation, subject, result)
                .await;

            Ok(result)
        })
    }

    /// Inner expression evaluation with caching support.
    #[allow(clippy::too_many_arguments)]
    fn evaluate_expr_inner<'a>(
        &'a self,
        policy_id: &'a str,
        resource: &'a str,
        object_id: &'a str,
        relation: &'a str,
        subject: &'a Did,
        expression: &'a RelationExpression,
        trail: NodeTrail,
        cache: Arc<CheckCache>,
    ) -> MaybeBoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            match expression {
                RelationExpression::This => {
                    // Direct lookup: check if tuple exists
                    self.store
                        .check_permission_direct(policy_id, resource, object_id, relation, subject)
                        .await
                }

                RelationExpression::ComputedUserset {
                    relation: computed_rel,
                } => {
                    // Check for cycles when transitioning to a new relation
                    // Per Go zanzi behavior: cycles return false (unauthorized), not error
                    let node_id = NodeId::new(resource, object_id, computed_rel);
                    if trail.contains(&node_id) {
                        return Ok(false);
                    }
                    let new_trail = trail.with_node(node_id);

                    // Check a different relation on the same object
                    let computed_expr =
                        self.lookup
                            .get_expression(policy_id, resource, computed_rel)?;

                    self.evaluate_expr_cached(
                        policy_id,
                        resource,
                        object_id,
                        computed_rel,
                        subject,
                        computed_expr,
                        new_trail,
                        cache,
                    )
                    .await
                }

                RelationExpression::TupleToUserset {
                    tuple_relation,
                    computed_relation,
                } => {
                    // Find objects that have tuple_relation to this object
                    let targets = self
                        .store
                        .get_relation_targets(policy_id, resource, object_id, tuple_relation)
                        .await?;

                    for target in targets {
                        // Check for cycles when transitioning to new object/relation
                        let node_id =
                            NodeId::new(&target.resource, &target.object_id, computed_relation);
                        if trail.contains(&node_id) {
                            continue; // Skip cyclic paths
                        }
                        let new_trail = trail.with_node(node_id);

                        let target_expr = self.lookup.get_expression(
                            policy_id,
                            &target.resource,
                            computed_relation,
                        )?;

                        if self
                            .evaluate_expr_cached(
                                policy_id,
                                &target.resource,
                                &target.object_id,
                                computed_relation,
                                subject,
                                target_expr,
                                new_trail,
                                cache.clone(),
                            )
                            .await?
                        {
                            return Ok(true);
                        }
                    }

                    // Also check direct tuples with entity set subjects
                    let subjects = self
                        .store
                        .get_relation_subjects(policy_id, resource, object_id, tuple_relation)
                        .await?;

                    for subj in subjects {
                        match subj {
                            Subject::EntitySet {
                                resource: target_resource,
                                object_id: target_object_id,
                                relation: _, // Ignore EntitySet's relation, use computed_relation
                            } => {
                                // Check for cycles using computed_relation (not EntitySet's relation)
                                let node_id = NodeId::new(
                                    &target_resource,
                                    &target_object_id,
                                    computed_relation,
                                );
                                if trail.contains(&node_id) {
                                    continue;
                                }
                                let new_trail = trail.with_node(node_id);

                                let target_expr = self.lookup.get_expression(
                                    policy_id,
                                    &target_resource,
                                    computed_relation,
                                )?;

                                if self
                                    .evaluate_expr_cached(
                                        policy_id,
                                        &target_resource,
                                        &target_object_id,
                                        computed_relation,
                                        subject,
                                        target_expr,
                                        new_trail,
                                        cache.clone(),
                                    )
                                    .await?
                                {
                                    return Ok(true);
                                }
                            }
                            Subject::Wildcard | Subject::TypedWildcard { .. } => {
                                // Wildcard on tuple_relation means any entity is a valid target.
                                // This grants access because the TTU chain succeeds for everyone.
                                return Ok(true);
                            }
                            Subject::Entity(_) => {
                                // Direct entity subjects are not targets for TTU traversal
                                continue;
                            }
                        }
                    }

                    Ok(false)
                }

                RelationExpression::Union(exprs) => {
                    // OR with short-circuit: return true if any matches
                    for expr in exprs {
                        if self
                            .evaluate_expr_inner(
                                policy_id,
                                resource,
                                object_id,
                                relation,
                                subject,
                                expr,
                                trail.clone(),
                                cache.clone(),
                            )
                            .await?
                        {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }

                RelationExpression::Intersection(exprs) => {
                    // AND: return true only if all match
                    for expr in exprs {
                        if !self
                            .evaluate_expr_inner(
                                policy_id,
                                resource,
                                object_id,
                                relation,
                                subject,
                                expr,
                                trail.clone(),
                                cache.clone(),
                            )
                            .await?
                        {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }

                RelationExpression::Difference { base, subtract } => {
                    // Base AND NOT subtract
                    let base_result = self
                        .evaluate_expr_inner(
                            policy_id,
                            resource,
                            object_id,
                            relation,
                            subject,
                            base,
                            trail.clone(),
                            cache.clone(),
                        )
                        .await?;

                    if !base_result {
                        return Ok(false);
                    }

                    let subtract_result = self
                        .evaluate_expr_inner(
                            policy_id, resource, object_id, relation, subject, subtract, trail,
                            cache,
                        )
                        .await?;

                    Ok(!subtract_result)
                }
            }
        })
    }

    /// Evaluate expression with trace collection for explain API.
    #[allow(clippy::too_many_arguments)]
    fn evaluate_expr_with_trace<'a>(
        &'a self,
        policy_id: &'a str,
        resource: &'a str,
        object_id: &'a str,
        relation: &'a str,
        subject: &'a Did,
        expression: &'a RelationExpression,
        trail: NodeTrail,
        cache: Arc<CheckCache>,
        trace: &'a mut EvaluationTrace,
    ) -> MaybeBoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            match expression {
                RelationExpression::This => {
                    let result = self
                        .store
                        .check_permission_direct(policy_id, resource, object_id, relation, subject)
                        .await?;

                    trace.add_step(EvaluationStep {
                        expression_type: "This (direct lookup)".to_string(),
                        resource: resource.to_string(),
                        object_id: object_id.to_string(),
                        relation: relation.to_string(),
                        result: if result {
                            StepResult::Granted
                        } else {
                            StepResult::Denied
                        },
                        details: Some(format!("Direct tuple check for subject {}", subject)),
                    });

                    Ok(result)
                }

                RelationExpression::ComputedUserset {
                    relation: computed_rel,
                } => {
                    let node_id = NodeId::new(resource, object_id, computed_rel);
                    if trail.contains(&node_id) {
                        trace.add_step(EvaluationStep {
                            expression_type: "ComputedUserset".to_string(),
                            resource: resource.to_string(),
                            object_id: object_id.to_string(),
                            relation: computed_rel.to_string(),
                            result: StepResult::Skipped,
                            details: Some("Cycle detected, returning false".to_string()),
                        });
                        return Ok(false);
                    }
                    let new_trail = trail.with_node(node_id);

                    trace.add_step(EvaluationStep {
                        expression_type: "ComputedUserset".to_string(),
                        resource: resource.to_string(),
                        object_id: object_id.to_string(),
                        relation: computed_rel.to_string(),
                        result: StepResult::Continuing,
                        details: Some(format!(
                            "Checking relation '{}' on same object",
                            computed_rel
                        )),
                    });

                    let computed_expr =
                        self.lookup
                            .get_expression(policy_id, resource, computed_rel)?;

                    let result = self
                        .evaluate_expr_with_trace(
                            policy_id,
                            resource,
                            object_id,
                            computed_rel,
                            subject,
                            computed_expr,
                            new_trail,
                            cache,
                            trace,
                        )
                        .await?;

                    trace.add_step(EvaluationStep {
                        expression_type: "ComputedUserset (result)".to_string(),
                        resource: resource.to_string(),
                        object_id: object_id.to_string(),
                        relation: computed_rel.to_string(),
                        result: if result {
                            StepResult::Granted
                        } else {
                            StepResult::Denied
                        },
                        details: None,
                    });

                    Ok(result)
                }

                RelationExpression::TupleToUserset {
                    tuple_relation,
                    computed_relation,
                } => {
                    trace.add_step(EvaluationStep {
                        expression_type: "TupleToUserset".to_string(),
                        resource: resource.to_string(),
                        object_id: object_id.to_string(),
                        relation: relation.to_string(),
                        result: StepResult::Continuing,
                        details: Some(format!(
                            "Following '{}' to check '{}'",
                            tuple_relation, computed_relation
                        )),
                    });

                    let targets = self
                        .store
                        .get_relation_targets(policy_id, resource, object_id, tuple_relation)
                        .await?;

                    for target in targets {
                        let node_id =
                            NodeId::new(&target.resource, &target.object_id, computed_relation);
                        if trail.contains(&node_id) {
                            trace.add_step(EvaluationStep {
                                expression_type: "TTU target".to_string(),
                                resource: target.resource.clone(),
                                object_id: target.object_id.clone(),
                                relation: computed_relation.to_string(),
                                result: StepResult::Skipped,
                                details: Some("Cycle detected".to_string()),
                            });
                            continue;
                        }
                        let new_trail = trail.with_node(node_id);

                        let target_expr = self.lookup.get_expression(
                            policy_id,
                            &target.resource,
                            computed_relation,
                        )?;

                        if self
                            .evaluate_expr_cached(
                                policy_id,
                                &target.resource,
                                &target.object_id,
                                computed_relation,
                                subject,
                                target_expr,
                                new_trail,
                                cache.clone(),
                            )
                            .await?
                        {
                            trace.add_step(EvaluationStep {
                                expression_type: "TTU target match".to_string(),
                                resource: target.resource.clone(),
                                object_id: target.object_id.clone(),
                                relation: computed_relation.to_string(),
                                result: StepResult::Granted,
                                details: Some(format!(
                                    "Found permission via {}:{}#{}",
                                    target.resource, target.object_id, computed_relation
                                )),
                            });
                            return Ok(true);
                        }
                    }

                    // Check entity set subjects
                    let subjects = self
                        .store
                        .get_relation_subjects(policy_id, resource, object_id, tuple_relation)
                        .await?;

                    for subj in subjects {
                        match subj {
                            Subject::EntitySet {
                                resource: target_resource,
                                object_id: target_object_id,
                                relation: _,
                            } => {
                                let node_id = NodeId::new(
                                    &target_resource,
                                    &target_object_id,
                                    computed_relation,
                                );
                                if trail.contains(&node_id) {
                                    continue;
                                }
                                let new_trail = trail.with_node(node_id);

                                let target_expr = self.lookup.get_expression(
                                    policy_id,
                                    &target_resource,
                                    computed_relation,
                                )?;

                                if self
                                    .evaluate_expr_cached(
                                        policy_id,
                                        &target_resource,
                                        &target_object_id,
                                        computed_relation,
                                        subject,
                                        target_expr,
                                        new_trail,
                                        cache.clone(),
                                    )
                                    .await?
                                {
                                    trace.add_step(EvaluationStep {
                                        expression_type: "TTU EntitySet match".to_string(),
                                        resource: target_resource.clone(),
                                        object_id: target_object_id.clone(),
                                        relation: computed_relation.to_string(),
                                        result: StepResult::Granted,
                                        details: Some(format!(
                                            "Found permission via EntitySet {}:{}#{}",
                                            target_resource, target_object_id, computed_relation
                                        )),
                                    });
                                    return Ok(true);
                                }
                            }
                            Subject::Wildcard | Subject::TypedWildcard { .. } => {
                                trace.add_step(EvaluationStep {
                                    expression_type: "TTU Wildcard".to_string(),
                                    resource: resource.to_string(),
                                    object_id: object_id.to_string(),
                                    relation: tuple_relation.to_string(),
                                    result: StepResult::Granted,
                                    details: Some(
                                        "Wildcard on tuple relation grants access".to_string(),
                                    ),
                                });
                                return Ok(true);
                            }
                            Subject::Entity(_) => continue,
                        }
                    }

                    trace.add_step(EvaluationStep {
                        expression_type: "TupleToUserset (result)".to_string(),
                        resource: resource.to_string(),
                        object_id: object_id.to_string(),
                        relation: relation.to_string(),
                        result: StepResult::Denied,
                        details: Some("No matching TTU path found".to_string()),
                    });

                    Ok(false)
                }

                RelationExpression::Union(exprs) => {
                    trace.add_step(EvaluationStep {
                        expression_type: "Union".to_string(),
                        resource: resource.to_string(),
                        object_id: object_id.to_string(),
                        relation: relation.to_string(),
                        result: StepResult::Continuing,
                        details: Some(format!("Evaluating {} branches (OR)", exprs.len())),
                    });

                    for (i, expr) in exprs.iter().enumerate() {
                        if self
                            .evaluate_expr_inner(
                                policy_id,
                                resource,
                                object_id,
                                relation,
                                subject,
                                expr,
                                trail.clone(),
                                cache.clone(),
                            )
                            .await?
                        {
                            trace.add_step(EvaluationStep {
                                expression_type: format!("Union branch {}", i + 1),
                                resource: resource.to_string(),
                                object_id: object_id.to_string(),
                                relation: relation.to_string(),
                                result: StepResult::Granted,
                                details: Some("Branch succeeded, short-circuiting".to_string()),
                            });
                            return Ok(true);
                        }
                    }

                    trace.add_step(EvaluationStep {
                        expression_type: "Union (result)".to_string(),
                        resource: resource.to_string(),
                        object_id: object_id.to_string(),
                        relation: relation.to_string(),
                        result: StepResult::Denied,
                        details: Some("All branches failed".to_string()),
                    });

                    Ok(false)
                }

                RelationExpression::Intersection(exprs) => {
                    trace.add_step(EvaluationStep {
                        expression_type: "Intersection".to_string(),
                        resource: resource.to_string(),
                        object_id: object_id.to_string(),
                        relation: relation.to_string(),
                        result: StepResult::Continuing,
                        details: Some(format!("Evaluating {} branches (AND)", exprs.len())),
                    });

                    for (i, expr) in exprs.iter().enumerate() {
                        if !self
                            .evaluate_expr_inner(
                                policy_id,
                                resource,
                                object_id,
                                relation,
                                subject,
                                expr,
                                trail.clone(),
                                cache.clone(),
                            )
                            .await?
                        {
                            trace.add_step(EvaluationStep {
                                expression_type: format!("Intersection branch {}", i + 1),
                                resource: resource.to_string(),
                                object_id: object_id.to_string(),
                                relation: relation.to_string(),
                                result: StepResult::Denied,
                                details: Some("Branch failed, short-circuiting".to_string()),
                            });
                            return Ok(false);
                        }
                    }

                    trace.add_step(EvaluationStep {
                        expression_type: "Intersection (result)".to_string(),
                        resource: resource.to_string(),
                        object_id: object_id.to_string(),
                        relation: relation.to_string(),
                        result: StepResult::Granted,
                        details: Some("All branches succeeded".to_string()),
                    });

                    Ok(true)
                }

                RelationExpression::Difference { base, subtract } => {
                    trace.add_step(EvaluationStep {
                        expression_type: "Difference".to_string(),
                        resource: resource.to_string(),
                        object_id: object_id.to_string(),
                        relation: relation.to_string(),
                        result: StepResult::Continuing,
                        details: Some("Evaluating base AND NOT subtract".to_string()),
                    });

                    let base_result = self
                        .evaluate_expr_inner(
                            policy_id,
                            resource,
                            object_id,
                            relation,
                            subject,
                            base,
                            trail.clone(),
                            cache.clone(),
                        )
                        .await?;

                    if !base_result {
                        trace.add_step(EvaluationStep {
                            expression_type: "Difference (base)".to_string(),
                            resource: resource.to_string(),
                            object_id: object_id.to_string(),
                            relation: relation.to_string(),
                            result: StepResult::Denied,
                            details: Some("Base expression failed".to_string()),
                        });
                        return Ok(false);
                    }

                    let subtract_result = self
                        .evaluate_expr_inner(
                            policy_id, resource, object_id, relation, subject, subtract, trail,
                            cache,
                        )
                        .await?;

                    let final_result = !subtract_result;
                    trace.add_step(EvaluationStep {
                        expression_type: "Difference (result)".to_string(),
                        resource: resource.to_string(),
                        object_id: object_id.to_string(),
                        relation: relation.to_string(),
                        result: if final_result {
                            StepResult::Granted
                        } else {
                            StepResult::Denied
                        },
                        details: Some(format!(
                            "Base=true, Subtract={}, Result={}",
                            subtract_result, final_result
                        )),
                    });

                    Ok(final_result)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::zanzibar::store::MemoryZanzibarStore;
    use crate::zanzibar::types::{Relation, Relationship, Resource};

    fn test_did() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    fn test_did2() -> Did {
        Did::new("did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH").unwrap()
    }

    #[tokio::test]
    async fn test_this_expression() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let mut engine = PermissionEngine::new(store.clone());

        // Create policy with direct owner relation
        let policy = Policy::new("policy1", "Test")
            .with_resource(Resource::new("document").with_relation(Relation::direct("owner")));

        engine.add_policy(&policy);

        let did = test_did();

        // Store a relationship
        let rel = Relationship::with_entity("document", "doc1", "owner", did.clone());
        store.store_relationship("policy1", &rel).await.unwrap();

        // Check permission
        let result = engine
            .check("policy1", "document", "doc1", "owner", &did)
            .await
            .unwrap();
        assert!(result);

        // Check non-existent permission
        let did2 = test_did2();
        let result = engine
            .check("policy1", "document", "doc1", "owner", &did2)
            .await
            .unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn test_computed_userset() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let mut engine = PermissionEngine::new(store.clone());

        // Create policy: owner implies reader
        // reader = _this + owner
        let policy = Policy::new("policy1", "Test").with_resource(
            Resource::new("document")
                .with_relation(Relation::direct("owner"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::computed_userset("owner"),
                    ]),
                )),
        );

        engine.add_policy(&policy);

        let owner_did = test_did();
        let reader_did = test_did2();

        // Store owner relationship
        let rel = Relationship::with_entity("document", "doc1", "owner", owner_did.clone());
        store.store_relationship("policy1", &rel).await.unwrap();

        // Store direct reader relationship
        let rel = Relationship::with_entity("document", "doc1", "reader", reader_did.clone());
        store.store_relationship("policy1", &rel).await.unwrap();

        // Owner should be able to read (via computed userset)
        let result = engine
            .check("policy1", "document", "doc1", "reader", &owner_did)
            .await
            .unwrap();
        assert!(result);

        // Direct reader should also be able to read
        let result = engine
            .check("policy1", "document", "doc1", "reader", &reader_did)
            .await
            .unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn test_tuple_to_userset() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let mut engine = PermissionEngine::new(store.clone());

        // Create policy: file.reader = parent->owner
        // If user is owner of parent folder, they can read the file
        let policy = Policy::new("policy1", "Test")
            .with_resource(
                Resource::new("file")
                    .with_relation(Relation::direct("parent"))
                    .with_relation(Relation::computed(
                        "reader",
                        RelationExpression::tuple_to_userset("parent", "owner"),
                    )),
            )
            .with_resource(Resource::new("folder").with_relation(Relation::direct("owner")));

        engine.add_policy(&policy);

        let folder_owner = test_did();

        // Folder owner relationship
        let rel = Relationship::with_entity("folder", "folder1", "owner", folder_owner.clone());
        store.store_relationship("policy1", &rel).await.unwrap();

        // File has parent relation to folder (entity set)
        let rel = Relationship::new(
            "file",
            "file1",
            "parent",
            Subject::entity_set("folder", "folder1", "owner"),
        );
        store.store_relationship("policy1", &rel).await.unwrap();

        // Folder owner should be able to read file via TTU
        let result = engine
            .check("policy1", "file", "file1", "reader", &folder_owner)
            .await
            .unwrap();
        assert!(result);

        // Non-owner should not be able to read
        let non_owner = test_did2();
        let result = engine
            .check("policy1", "file", "file1", "reader", &non_owner)
            .await
            .unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn test_intersection() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let mut engine = PermissionEngine::new(store.clone());

        // Create policy: editor = member & approved
        let policy = Policy::new("policy1", "Test").with_resource(
            Resource::new("document")
                .with_relation(Relation::direct("member"))
                .with_relation(Relation::direct("approved"))
                .with_relation(Relation::computed(
                    "editor",
                    RelationExpression::intersection(vec![
                        RelationExpression::computed_userset("member"),
                        RelationExpression::computed_userset("approved"),
                    ]),
                )),
        );

        engine.add_policy(&policy);

        let user = test_did();

        // User is member
        let rel = Relationship::with_entity("document", "doc1", "member", user.clone());
        store.store_relationship("policy1", &rel).await.unwrap();

        // User not approved yet - should not be editor
        let result = engine
            .check("policy1", "document", "doc1", "editor", &user)
            .await
            .unwrap();
        assert!(!result);

        // Add approval
        let rel = Relationship::with_entity("document", "doc1", "approved", user.clone());
        store.store_relationship("policy1", &rel).await.unwrap();

        // Now user should be editor
        let result = engine
            .check("policy1", "document", "doc1", "editor", &user)
            .await
            .unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn test_difference() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let mut engine = PermissionEngine::new(store.clone());

        // Create policy: viewer = member - banned
        let policy = Policy::new("policy1", "Test").with_resource(
            Resource::new("document")
                .with_relation(Relation::direct("member"))
                .with_relation(Relation::direct("banned"))
                .with_relation(Relation::computed(
                    "viewer",
                    RelationExpression::difference(
                        RelationExpression::computed_userset("member"),
                        RelationExpression::computed_userset("banned"),
                    ),
                )),
        );

        engine.add_policy(&policy);

        let user = test_did();

        // User is member
        let rel = Relationship::with_entity("document", "doc1", "member", user.clone());
        store.store_relationship("policy1", &rel).await.unwrap();

        // User should be viewer
        let result = engine
            .check("policy1", "document", "doc1", "viewer", &user)
            .await
            .unwrap();
        assert!(result);

        // Ban the user
        let rel = Relationship::with_entity("document", "doc1", "banned", user.clone());
        store.store_relationship("policy1", &rel).await.unwrap();

        // User should no longer be viewer
        let result = engine
            .check("policy1", "document", "doc1", "viewer", &user)
            .await
            .unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn test_wildcard() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let mut engine = PermissionEngine::new(store.clone());

        let policy = Policy::new("policy1", "Test")
            .with_resource(Resource::new("document").with_relation(Relation::direct("viewer")));

        engine.add_policy(&policy);

        // Store wildcard relationship (public access)
        let rel = Relationship::new("document", "doc1", "viewer", Subject::Wildcard);
        store.store_relationship("policy1", &rel).await.unwrap();

        // Any user should have permission
        let user = test_did();
        let result = engine
            .check("policy1", "document", "doc1", "viewer", &user)
            .await
            .unwrap();
        assert!(result);

        let user2 = test_did2();
        let result = engine
            .check("policy1", "document", "doc1", "viewer", &user2)
            .await
            .unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn test_policy_not_found() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let engine = PermissionEngine::new(store);

        let did = test_did();
        let result = engine
            .check("nonexistent", "document", "doc1", "owner", &did)
            .await;

        assert!(matches!(result, Err(Error::PolicyNotFound(_))));
    }

    #[tokio::test]
    async fn test_relation_not_found() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let mut engine = PermissionEngine::new(store);

        let policy = Policy::new("policy1", "Test")
            .with_resource(Resource::new("document").with_relation(Relation::direct("owner")));

        engine.add_policy(&policy);

        let did = test_did();
        let result = engine
            .check("policy1", "document", "doc1", "nonexistent", &did)
            .await;

        assert!(matches!(result, Err(Error::RelationNotFound { .. })));
    }

    #[tokio::test]
    async fn test_cycle_detection_returns_false() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let mut engine = PermissionEngine::new(store.clone());

        // Create a policy with a cyclic relation:
        // reader = viewer, viewer = reader (mutual recursion)
        let policy = Policy::new("policy1", "Test").with_resource(
            Resource::new("document")
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::computed_userset("viewer"),
                ))
                .with_relation(Relation::computed(
                    "viewer",
                    RelationExpression::computed_userset("reader"),
                )),
        );

        engine.add_policy(&policy);

        let did = test_did();

        // Cycle detection should return false (not authorized), not an error
        // This matches Go zanzi behavior
        let result = engine
            .check("policy1", "document", "doc1", "reader", &did)
            .await;

        // Should succeed with false, not error
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_typed_wildcard_permission() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let mut engine = PermissionEngine::new(store.clone());

        let policy = Policy::new("policy1", "Test")
            .with_resource(Resource::new("document").with_relation(Relation::direct("viewer")));

        engine.add_policy(&policy);

        // Store typed wildcard relationship (user:*)
        let rel = Relationship::new(
            "document",
            "doc1",
            "viewer",
            Subject::typed_wildcard("user"),
        );
        store.store_relationship("policy1", &rel).await.unwrap();

        // Any user should have permission via typed wildcard
        let did = test_did();
        let result = engine
            .check("policy1", "document", "doc1", "viewer", &did)
            .await
            .unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn test_check_many_batch() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let mut engine = PermissionEngine::new(store.clone());

        // Create policy with owner and reader
        let policy = Policy::new("policy1", "Test").with_resource(
            Resource::new("document")
                .with_relation(Relation::direct("owner"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::computed_userset("owner"),
                    ]),
                )),
        );

        engine.add_policy(&policy);

        let owner = test_did();
        let reader = test_did2();

        // Store owner relationship
        let rel = Relationship::with_entity("document", "doc1", "owner", owner.clone());
        store.store_relationship("policy1", &rel).await.unwrap();

        // Store reader relationship
        let rel = Relationship::with_entity("document", "doc2", "reader", reader.clone());
        store.store_relationship("policy1", &rel).await.unwrap();

        // Batch check multiple permissions
        let requests = vec![
            PermissionCheckRequest::new("policy1", "document", "doc1", "owner", &owner),
            PermissionCheckRequest::new("policy1", "document", "doc1", "reader", &owner),
            PermissionCheckRequest::new("policy1", "document", "doc2", "reader", &reader),
            PermissionCheckRequest::new("policy1", "document", "doc2", "reader", &owner), // Should be false
        ];

        let results = engine.check_many(&requests).await;

        assert_eq!(results.len(), 4);
        assert!(results[0].as_ref().unwrap()); // owner has owner
        assert!(results[1].as_ref().unwrap()); // owner has reader (via owner)
        assert!(results[2].as_ref().unwrap()); // reader has reader
        assert!(!results[3].as_ref().unwrap()); // owner doesn't have reader on doc2
    }

    #[tokio::test]
    async fn test_explain_granted() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let mut engine = PermissionEngine::new(store.clone());

        let policy = Policy::new("policy1", "Test").with_resource(
            Resource::new("document")
                .with_relation(Relation::direct("owner"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::computed_userset("owner"),
                    ]),
                )),
        );

        engine.add_policy(&policy);

        let owner = test_did();
        let rel = Relationship::with_entity("document", "doc1", "owner", owner.clone());
        store.store_relationship("policy1", &rel).await.unwrap();

        // Explain why owner has reader permission
        let explanation = engine
            .explain("policy1", "document", "doc1", "reader", &owner)
            .await
            .unwrap();

        assert!(explanation.granted);
        assert_eq!(explanation.resource, "document");
        assert_eq!(explanation.object_id, "doc1");
        assert_eq!(explanation.relation, "reader");
        assert!(!explanation.trace.steps.is_empty());

        // The trace should show the evaluation path
        let granted_steps: Vec<_> = explanation
            .trace
            .steps
            .iter()
            .filter(|s| matches!(s.result, StepResult::Granted))
            .collect();
        assert!(!granted_steps.is_empty());
    }

    #[tokio::test]
    async fn test_explain_denied() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let mut engine = PermissionEngine::new(store.clone());

        let policy = Policy::new("policy1", "Test")
            .with_resource(Resource::new("document").with_relation(Relation::direct("owner")));

        engine.add_policy(&policy);

        let user = test_did();

        // Don't store any relationships - user shouldn't have permission
        let explanation = engine
            .explain("policy1", "document", "doc1", "owner", &user)
            .await
            .unwrap();

        assert!(!explanation.granted);
        assert!(!explanation.trace.steps.is_empty());

        // Should have a denied step
        let denied_steps: Vec<_> = explanation
            .trace
            .steps
            .iter()
            .filter(|s| matches!(s.result, StepResult::Denied))
            .collect();
        assert!(!denied_steps.is_empty());
    }
}
