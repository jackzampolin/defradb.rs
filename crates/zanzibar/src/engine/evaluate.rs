use std::sync::Arc;

use crate::did::Did;
use crate::thread_bounds::MaybeBoxFuture;

use super::cache::{CheckCache, NodeId, NodeTrail};
use super::PermissionEngine;
use crate::error::Result;
use crate::expression::RelationExpression;
use crate::store::ZanzibarStore;
use crate::types::Subject;

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
    ) -> MaybeBoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            if let Some(cached) = cache.get(resource, object_id, relation, subject).await {
                return Ok(cached);
            }

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

            cache
                .set(resource, object_id, relation, subject, result)
                .await;

            Ok(result)
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
    ) -> MaybeBoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            match expression {
                RelationExpression::This => {
                    self.store
                        .check_permission_direct(policy_id, resource, object_id, relation, subject)
                        .await
                }

                RelationExpression::ComputedUserset {
                    relation: computed_rel,
                } => {
                    let node_id = NodeId::new(resource, object_id, computed_rel);
                    if trail.contains(&node_id) {
                        return Ok(false);
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
                    let targets = self
                        .store
                        .get_relation_targets(policy_id, resource, object_id, tuple_relation)
                        .await?;

                    for target in targets {
                        let node_id =
                            NodeId::new(&target.resource, &target.object_id, computed_relation);
                        if trail.contains(&node_id) {
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
                            return Ok(true);
                        }
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
                                return Ok(true);
                            }
                            Subject::Entity(_) => {
                                continue;
                            }
                        }
                    }

                    Ok(false)
                }

                RelationExpression::Union(exprs) => {
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
