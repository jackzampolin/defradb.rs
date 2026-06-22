use std::sync::Arc;

use crate::did::Did;
use crate::thread_bounds::MaybeBoxFuture;

use super::cache::{CheckCache, NodeId, NodeTrail};
use super::PermissionEngine;
use crate::error::Result;
use crate::expression::RelationExpression;
use crate::store::ZanzibarStore;
use crate::types::Subject;

/// Evaluation result paired with a `tainted` flag.
///
/// A result is *tainted* when it depended on a cycle truncation (a node revisited
/// on the current trail, evaluated as `false`). Such a result is only valid for
/// the trail that produced it — the same node reached via a different trail can
/// resolve differently — so a tainted result must NOT be memoized in the
/// trail-independent [`CheckCache`].
type Eval = (bool, bool);

impl<S: ZanzibarStore + ?Sized> PermissionEngine<S> {
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
    ) -> MaybeBoxFuture<'a, Result<Eval>> {
        Box::pin(async move {
            if let Some(cached) = cache.get(resource, object_id, relation, subject).await {
                // Only untainted results are ever stored, so a hit is trail-independent.
                return Ok((cached, false));
            }

            let (value, tainted) = self
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

            if !tainted {
                cache
                    .set(resource, object_id, relation, subject, value)
                    .await;
            }

            Ok((value, tainted))
        })
    }

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
    ) -> MaybeBoxFuture<'a, Result<Eval>> {
        Box::pin(async move {
            match expression {
                RelationExpression::This => {
                    let granted = self
                        .store
                        .check_permission_direct(policy_id, resource, object_id, relation, subject)
                        .await?;
                    Ok((granted, false))
                }

                RelationExpression::ComputedUserset {
                    relation: computed_rel,
                } => {
                    let node_id = NodeId::new(resource, object_id, computed_rel);
                    if trail.contains(&node_id) {
                        // Cycle truncation: trail-dependent, must not be cached.
                        return Ok((false, true));
                    }
                    let new_trail = trail.with_node(node_id);

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
                    let mut tainted = false;

                    let targets = self
                        .store
                        .get_relation_targets(policy_id, resource, object_id, tuple_relation)
                        .await?;

                    for target in targets {
                        let node_id =
                            NodeId::new(&target.resource, &target.object_id, computed_relation);
                        if trail.contains(&node_id) {
                            // Skipping a cycled target taints the eventual `false`.
                            tainted = true;
                            continue;
                        }
                        let new_trail = trail.with_node(node_id);

                        let target_expr = self.lookup.get_expression(
                            policy_id,
                            &target.resource,
                            computed_relation,
                        )?;

                        let (granted, t) = self
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
                            .await?;
                        if granted {
                            return Ok((true, t));
                        }
                        tainted |= t;
                    }

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
                                    tainted = true;
                                    continue;
                                }
                                let new_trail = trail.with_node(node_id);

                                let target_expr = self.lookup.get_expression(
                                    policy_id,
                                    &target_resource,
                                    computed_relation,
                                )?;

                                let (granted, t) = self
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
                                    .await?;
                                if granted {
                                    return Ok((true, t));
                                }
                                tainted |= t;
                            }
                            Subject::Wildcard | Subject::TypedWildcard { .. } => {
                                return Ok((true, false));
                            }
                            Subject::Entity(_) => {
                                continue;
                            }
                        }
                    }

                    Ok((false, tainted))
                }

                RelationExpression::Union(exprs) => {
                    let mut tainted = false;
                    for expr in exprs {
                        let (granted, t) = self
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
                            .await?;
                        if granted {
                            // A true short-circuits on this branch alone.
                            return Ok((true, t));
                        }
                        tainted |= t;
                    }
                    Ok((false, tainted))
                }

                RelationExpression::Intersection(exprs) => {
                    let mut tainted = false;
                    for expr in exprs {
                        let (granted, t) = self
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
                            .await?;
                        if !granted {
                            // A false short-circuits on this branch alone.
                            return Ok((false, t));
                        }
                        tainted |= t;
                    }
                    Ok((true, tainted))
                }

                RelationExpression::Difference { base, subtract } => {
                    let (base_granted, base_tainted) = self
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

                    if !base_granted {
                        return Ok((false, base_tainted));
                    }

                    let (subtract_granted, subtract_tainted) = self
                        .evaluate_expr_inner(
                            policy_id, resource, object_id, relation, subject, subtract, trail,
                            cache,
                        )
                        .await?;

                    Ok((!subtract_granted, base_tainted || subtract_tainted))
                }
            }
        })
    }
}
