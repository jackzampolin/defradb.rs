use std::sync::Arc;

use crate::did::Did;
use crate::thread_bounds::MaybeBoxFuture;

use super::cache::{CheckCache, NodeId, NodeTrail};
use super::{EvaluationStep, EvaluationTrace, PermissionEngine, StepResult};
use crate::error::Result;
use crate::expression::RelationExpression;
use crate::store::ZanzibarStore;
use crate::types::Subject;

impl<S: ZanzibarStore + ?Sized> PermissionEngine<S> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn evaluate_expr_with_trace<'a>(
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
