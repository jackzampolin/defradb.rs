//! Aggregate and similarity node building utilities
//!
//! Contains methods for adding aggregate nodes (count, sum, avg, min, max)
//! and similarity nodes to query plans.

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{AggregateType, Requestable, Select};
use crate::plan::{
    AverageNode, AvgSourceMeta, BM25Node, CountNode, CountSourceMeta, MaxNode, MaxSourceMeta,
    MinNode, MinSourceMeta, SimilarityNode, SumNode, SumSourceMeta,
};
use crate::planner::PlanNode;

use super::builder::Planner;

impl Planner {
    /// Add SimilarityNode(s) to the plan for each _similarity field in the select.
    ///
    /// Each _similarity computes a dot product between the document's vector field
    /// and the query vector, storing the result at the designated index.
    pub(super) fn add_similarity_nodes(
        &self,
        mut plan: Box<dyn PlanNode>,
        select: &Select,
        mapping: &DocumentMapping,
    ) -> Result<Box<dyn PlanNode>> {
        for field in &select.fields {
            if let Requestable::Similarity(sim) = field {
                let field_index =
                    mapping
                        .first_index_of_name(&sim.target_field)
                        .ok_or_else(|| {
                            QueryError::internal(format!(
                                "similarity target field '{}' not found in mapping",
                                sim.target_field
                            ))
                        })?;

                let similarity_index = mapping
                    .try_find_index_from_render_key(sim.output_name())
                    .ok_or_else(|| {
                        QueryError::internal(format!(
                            "similarity output '{}' not found in mapping render keys",
                            sim.output_name()
                        ))
                    })?;

                plan = Box::new(SimilarityNode::new(
                    plan,
                    mapping.clone(),
                    field_index,
                    similarity_index,
                    sim.vector.clone(),
                ));
            }
        }
        Ok(plan)
    }

    /// Add BM25Node(s) to the plan for each _bm25 field in the select.
    ///
    /// Looks up the collection's fulltext index configuration to get k1, b,
    /// and language parameters for proper BM25 scoring.
    pub(super) fn add_bm25_nodes(
        &self,
        mut plan: Box<dyn PlanNode>,
        select: &Select,
        mapping: &DocumentMapping,
    ) -> Result<Box<dyn PlanNode>> {
        for field in &select.fields {
            if let Requestable::FullTextSearch(fts) = field {
                let field_indices: Vec<usize> = fts
                    .target_fields
                    .iter()
                    .filter_map(|f| mapping.first_index_of_name(f))
                    .collect();

                if field_indices.is_empty() {
                    return Err(QueryError::internal(format!(
                        "BM25 target fields {:?} not found in mapping",
                        fts.target_fields
                    )));
                }

                let score_index = mapping
                    .try_find_index_from_render_key(fts.output_name())
                    .ok_or_else(|| {
                        QueryError::internal(format!(
                            "BM25 score output '{}' not found in mapping render keys",
                            fts.output_name()
                        ))
                    })?;

                // Look up k1/b/language from the collection's fulltext index config
                let (k1, b, language) = self
                    .collections
                    .get(&select.collection_name)
                    .and_then(|col| {
                        fts.target_fields.first().and_then(|field_name| {
                            col.fulltext_indexes
                                .iter()
                                .find(|ft| ft.field_name == *field_name)
                        })
                    })
                    .map(|ft| (ft.k1, ft.b, ft.language.as_str()))
                    .unwrap_or((1.2, 0.75, "english"));

                plan = Box::new(BM25Node::new(
                    plan,
                    mapping.clone(),
                    field_indices,
                    score_index,
                    fts.query.clone(),
                    k1,
                    b,
                    language,
                ));
            }
        }
        Ok(plan)
    }

    /// Add aggregate nodes to the plan based on the select's aggregate fields.
    ///
    /// Handles three types of aggregates:
    /// - Simple field aggregates (e.g., _sum(field: age))
    /// - Group aggregates (e.g., _sum(_group: {field: age}))
    /// - Relation aggregates (e.g., _sum(articles: {field: pages}))
    ///
    /// Relation and inline-array aggregates are handled by iterating through
    /// the JSON array stored in the relation/array field.
    pub(super) fn add_aggregate_nodes(
        &self,
        mut plan: Box<dyn PlanNode>,
        select: &Select,
        mapping: &DocumentMapping,
    ) -> Result<Box<dyn PlanNode>> {
        for field in &select.fields {
            if let Requestable::Aggregate(agg) = field {
                let agg_index = mapping
                    .try_find_index_from_render_key(agg.output_name())
                    .ok_or_else(|| {
                        QueryError::internal(format!(
                            "aggregate '{}' not found in document mapping render keys - this is a bug",
                            agg.output_name()
                        ))
                    })?;

                let mut is_array_aggregate = false;
                let mut array_field_index = 0usize;
                let mut target_field_name = String::new();
                let mut field_index = 0usize;

                if !agg.targets.is_empty() {
                    let target = &agg.targets[0];
                    let host_name = &target.host_name;

                    if host_name == "GROUP" {
                        if let Some(ref fname) = target.field_name {
                            let is_aggregate_name =
                                matches!(fname.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX");
                            if is_aggregate_name || mapping.first_index_of_name(fname).is_none() {
                                is_array_aggregate = true;
                                array_field_index =
                                    mapping.first_index_of_name("GROUP").unwrap_or(0);
                                target_field_name = fname.clone();
                            } else {
                                field_index =
                                    mapping.first_index_of_name(fname).ok_or_else(|| {
                                        QueryError::execution(format!(
                                            "aggregate target field '{}' not found in mapping",
                                            fname
                                        ))
                                    })?;
                            }
                        }
                    } else if !host_name.is_empty() {
                        if let Some(idx) = mapping.first_index_of_name(host_name) {
                            is_array_aggregate = true;
                            array_field_index = idx;
                            target_field_name = target.field_name.clone().unwrap_or_default();
                        }
                    } else if let Some(ref fname) = target.field_name {
                        field_index = mapping.first_index_of_name(fname).ok_or_else(|| {
                            QueryError::execution(format!(
                                "aggregate target field '{}' not found in mapping",
                                fname
                            ))
                        })?;
                    }
                }

                let target_filter = if !agg.targets.is_empty() {
                    agg.targets[0].filter.clone()
                } else {
                    None
                };
                let target_limit = if !agg.targets.is_empty() {
                    agg.targets[0].limit.clone()
                } else {
                    None
                };

                match agg.aggregate_type {
                    AggregateType::Count => {
                        let mut node = CountNode::new(plan, mapping.clone(), agg_index);
                        if is_array_aggregate {
                            node = node.with_child_aggregate_source(
                                array_field_index,
                                target_field_name.clone(),
                            );
                        }
                        if let Some(ref filter) = target_filter {
                            node = node.with_filter(filter.clone());
                        }
                        if let Some(limit) = target_limit {
                            node = node.with_limit(limit);
                        }
                        let sources: Vec<CountSourceMeta> = agg
                            .targets
                            .iter()
                            .map(|target| CountSourceMeta {
                                field_name: if !target.host_name.is_empty() {
                                    target.host_name.clone()
                                } else {
                                    select.collection_name.clone()
                                },
                                filter: target.filter.clone(),
                                is_inline_array: is_array_aggregate && target.field_name.is_none(),
                            })
                            .collect();
                        node = node.with_sources(sources);
                        plan = Box::new(node);
                    }
                    AggregateType::Sum => {
                        let mut node = SumNode::new(plan, mapping.clone(), field_index, agg_index);
                        if is_array_aggregate {
                            node = node.with_child_aggregate_source(
                                array_field_index,
                                target_field_name.clone(),
                            );
                        }
                        if let Some(ref filter) = target_filter {
                            node = node.with_filter(filter.clone());
                        }
                        if let Some(limit) = target_limit {
                            node = node.with_limit(limit);
                        }
                        let sources: Vec<SumSourceMeta> = agg
                            .targets
                            .iter()
                            .map(|target| SumSourceMeta {
                                field_name: if !target.host_name.is_empty() {
                                    target.host_name.clone()
                                } else {
                                    select.collection_name.clone()
                                },
                                child_field_name: target.field_name.clone(),
                                filter: target.filter.clone(),
                                is_inline_array: is_array_aggregate && target.field_name.is_none(),
                            })
                            .collect();
                        node = node.with_sources(sources);
                        plan = Box::new(node);
                    }
                    AggregateType::Average => {
                        let mut node =
                            AverageNode::new(plan, mapping.clone(), field_index, agg_index);
                        if is_array_aggregate {
                            node = node.with_child_aggregate_source(
                                array_field_index,
                                target_field_name.clone(),
                            );
                        }
                        if let Some(ref filter) = target_filter {
                            node = node.with_filter(filter.clone());
                        }
                        if let Some(limit) = target_limit {
                            node = node.with_limit(limit);
                        }
                        let sources: Vec<AvgSourceMeta> = agg
                            .targets
                            .iter()
                            .map(|target| AvgSourceMeta {
                                field_name: if !target.host_name.is_empty() {
                                    target.host_name.clone()
                                } else {
                                    select.collection_name.clone()
                                },
                                child_field_name: target.field_name.clone(),
                                filter: target.filter.clone(),
                                is_inline_array: is_array_aggregate && target.field_name.is_none(),
                            })
                            .collect();
                        node = node.with_sources(sources);
                        plan = Box::new(node);
                    }
                    AggregateType::Min => {
                        let mut node = MinNode::new(plan, mapping.clone(), field_index, agg_index);
                        if is_array_aggregate {
                            node = node.with_child_aggregate_source(
                                array_field_index,
                                target_field_name.clone(),
                            );
                        }
                        if let Some(ref filter) = target_filter {
                            node = node.with_filter(filter.clone());
                        }
                        if let Some(limit) = target_limit {
                            node = node.with_limit(limit);
                        }
                        let sources: Vec<MinSourceMeta> = agg
                            .targets
                            .iter()
                            .map(|target| MinSourceMeta {
                                field_name: if !target.host_name.is_empty() {
                                    target.host_name.clone()
                                } else {
                                    select.collection_name.clone()
                                },
                                child_field_name: target.field_name.clone(),
                                filter: target.filter.clone(),
                                is_inline_array: is_array_aggregate && target.field_name.is_none(),
                            })
                            .collect();
                        node = node.with_sources(sources);
                        plan = Box::new(node);
                    }
                    AggregateType::Max => {
                        let mut node = MaxNode::new(plan, mapping.clone(), field_index, agg_index);
                        if is_array_aggregate {
                            node = node.with_child_aggregate_source(
                                array_field_index,
                                target_field_name.clone(),
                            );
                        }
                        if let Some(ref filter) = target_filter {
                            node = node.with_filter(filter.clone());
                        }
                        if let Some(limit) = target_limit {
                            node = node.with_limit(limit);
                        }
                        let sources: Vec<MaxSourceMeta> = agg
                            .targets
                            .iter()
                            .map(|target| MaxSourceMeta {
                                field_name: if !target.host_name.is_empty() {
                                    target.host_name.clone()
                                } else {
                                    select.collection_name.clone()
                                },
                                child_field_name: target.field_name.clone(),
                                filter: target.filter.clone(),
                                is_inline_array: is_array_aggregate && target.field_name.is_none(),
                            })
                            .collect();
                        node = node.with_sources(sources);
                        plan = Box::new(node);
                    }
                }
            }
        }
        Ok(plan)
    }
}
