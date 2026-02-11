//! Core permission expression evaluation.

use std::sync::Arc;

use defra_core::thread_bounds::MaybeBoxFuture;
use identity::Did;

use super::cache::{CheckCache, NodeId, NodeTrail};
use super::PermissionEngine;
use crate::error::Result;
use crate::zanzibar::expression::RelationExpression;
use crate::zanzibar::store::ZanzibarStore;
use crate::zanzibar::types::Subject;

impl<S: ZanzibarStore> PermissionEngine<S> {
    /// Evaluate an expression with caching support.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn evaluate_expr_cached<'a>(
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
    pub(crate) fn evaluate_expr_inner<'a>(
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
}
