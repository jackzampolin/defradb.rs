use document::Document;
use serde_json::Value as JsonValue;

use crate::mapper::{Aggregate, AggregateType, Requestable, Select};
use crate::txn::TransactionRegistry;

use super::super::commits_numeric::{
    max_commit_numeric_values, min_commit_numeric_values, sum_commit_numeric_values,
    CommitNumericValue,
};
use super::super::{DocFetcher, QueryRunner};

pub(super) fn is_commit_aggregate_only_selection(select: &Select) -> bool {
    !select.fields.is_empty()
        && select
            .fields
            .iter()
            .all(|field| matches!(field, Requestable::Aggregate(_)))
}

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    pub(super) fn commit_aggregate_target_field<'a>(&self, agg: &'a Aggregate) -> Option<&'a str> {
        agg.targets.first().and_then(|target| {
            target
                .field_name
                .as_deref()
                .filter(|field_name| !field_name.is_empty())
                .or_else(|| {
                    Some(target.host_name.as_str()).filter(|host_name| !host_name.is_empty())
                })
        })
    }

    pub(super) fn normal_value_to_commit_numeric(
        &self,
        value: &document::NormalValue,
    ) -> Option<CommitNumericValue> {
        value.as_int().map(CommitNumericValue::Int).or_else(|| {
            value
                .as_float64()
                .map(CommitNumericValue::Float)
                .or_else(|| {
                    value
                        .as_float32()
                        .map(|value| CommitNumericValue::Float(value as f64))
                })
        })
    }

    pub(super) fn decode_commit_delta_numeric(
        &self,
        commit: &Document,
    ) -> Option<CommitNumericValue> {
        use base64::Engine;

        let delta_base64 = commit.get("delta")?.as_str()?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(delta_base64)
            .ok()?;
        let value = ciborium::from_reader::<document::NormalValue, _>(&bytes[..]).ok()?;
        self.normal_value_to_commit_numeric(&value)
    }

    pub(super) fn commit_numeric_value(
        &self,
        commit: &Document,
        field_name: &str,
    ) -> Option<CommitNumericValue> {
        if field_name == "delta" {
            self.decode_commit_delta_numeric(commit)
        } else {
            commit
                .get(field_name)
                .and_then(|value| self.normal_value_to_commit_numeric(value))
        }
    }

    pub(super) fn collect_commit_numeric_values(
        &self,
        commit: &Document,
        group_docs: Option<&[Document]>,
        field_name: &str,
    ) -> Vec<CommitNumericValue> {
        let mut values = Vec::new();

        if let Some(group_docs) = group_docs {
            for doc in group_docs {
                if let Some(value) = self.commit_numeric_value(doc, field_name) {
                    values.push(value);
                }
            }
        } else if let Some(value) = self.commit_numeric_value(commit, field_name) {
            values.push(value);
        }

        values
    }

    pub(super) fn count_commit_aggregate_values(
        &self,
        commit: &Document,
        group_docs: Option<&[Document]>,
        field_name: Option<&str>,
    ) -> i64 {
        match field_name {
            None => group_docs.map(|docs| docs.len()).unwrap_or(1) as i64,
            Some(field_name) => {
                let mut count = 0i64;

                let mut count_doc = |doc: &Document| {
                    if field_name == "delta" {
                        if self.decode_commit_delta_numeric(doc).is_some() {
                            count += 1;
                        }
                        return;
                    }

                    if let Some(value) = doc.get(field_name) {
                        if let Ok(json_value) = crate::json_convert::normal_value_to_json(value) {
                            if let Some(array) = json_value.as_array() {
                                count += array.len() as i64;
                            } else if !json_value.is_null() {
                                count += 1;
                            }
                        }
                    }
                };

                if let Some(group_docs) = group_docs {
                    for doc in group_docs {
                        count_doc(doc);
                    }
                } else {
                    count_doc(commit);
                }

                count
            }
        }
    }

    pub(super) fn compute_commit_aggregate(
        &self,
        agg: &Aggregate,
        commit: &Document,
        group_docs: Option<&[Document]>,
    ) -> JsonValue {
        match agg.aggregate_type {
            AggregateType::Count => {
                let count = self.count_commit_aggregate_values(
                    commit,
                    group_docs,
                    self.commit_aggregate_target_field(agg),
                );
                JsonValue::Number(count.into())
            }
            AggregateType::Sum => {
                let Some(field_name) = self.commit_aggregate_target_field(agg) else {
                    return JsonValue::Number(0.into());
                };
                let values = self.collect_commit_numeric_values(commit, group_docs, field_name);
                sum_commit_numeric_values(&values)
            }
            AggregateType::Average => {
                let Some(field_name) = self.commit_aggregate_target_field(agg) else {
                    return JsonValue::Number(0.into());
                };
                let values = self.collect_commit_numeric_values(commit, group_docs, field_name);
                let avg = if values.is_empty() {
                    0.0
                } else {
                    values.iter().map(|value| value.as_f64()).sum::<f64>() / values.len() as f64
                };
                serde_json::Number::from_f64(avg)
                    .map(JsonValue::Number)
                    .unwrap_or_else(|| JsonValue::Number(0.into()))
            }
            AggregateType::Min => {
                let Some(field_name) = self.commit_aggregate_target_field(agg) else {
                    return JsonValue::Null;
                };
                let values = self.collect_commit_numeric_values(commit, group_docs, field_name);
                min_commit_numeric_values(&values)
            }
            AggregateType::Max => {
                let Some(field_name) = self.commit_aggregate_target_field(agg) else {
                    return JsonValue::Null;
                };
                let values = self.collect_commit_numeric_values(commit, group_docs, field_name);
                max_commit_numeric_values(&values)
            }
        }
    }
}
