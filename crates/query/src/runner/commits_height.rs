//! Height range extraction for commits queries.

use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CommitsHeightRange {
    pub start: u64,
    pub end: Option<u64>,
}

impl CommitsHeightRange {
    pub(crate) fn merge(self, other: Self) -> HeightRangeExtraction {
        let start = self.start.max(other.start);
        let end = match (self.end, other.end) {
            (Some(lhs), Some(rhs)) => Some(lhs.min(rhs)),
            (Some(lhs), None) => Some(lhs),
            (None, Some(rhs)) => Some(rhs),
            (None, None) => None,
        };

        if end.is_some_and(|end| start >= end) {
            HeightRangeExtraction::Empty
        } else {
            HeightRangeExtraction::Range(Self { start, end })
        }
    }

    fn with_lower_bound(mut self, start: u64) -> HeightRangeExtraction {
        self.start = self.start.max(start);
        if self.end.is_some_and(|end| self.start >= end) {
            HeightRangeExtraction::Empty
        } else {
            HeightRangeExtraction::Range(self)
        }
    }

    fn with_upper_bound(mut self, end: Option<u64>) -> HeightRangeExtraction {
        self.end = match (self.end, end) {
            (Some(lhs), Some(rhs)) => Some(lhs.min(rhs)),
            (Some(lhs), None) => Some(lhs),
            (None, Some(rhs)) => Some(rhs),
            (None, None) => None,
        };

        if self.end.is_some_and(|end| self.start >= end) {
            HeightRangeExtraction::Empty
        } else {
            HeightRangeExtraction::Range(self)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HeightRangeExtraction {
    None,
    Range(CommitsHeightRange),
    Empty,
    Unsupported,
}

impl HeightRangeExtraction {
    pub(crate) fn merge_and(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unsupported, _) | (_, Self::Unsupported) => Self::Unsupported,
            (Self::Empty, _) | (_, Self::Empty) => Self::Empty,
            (Self::None, rhs) => rhs,
            (lhs, Self::None) => lhs,
            (Self::Range(lhs), Self::Range(rhs)) => lhs.merge(rhs),
        }
    }
}

pub(crate) fn extract_commits_height_range(
    filter: &crate::mapper::Filter,
) -> HeightRangeExtraction {
    extract_commits_height_range_from_conditions(filter.conditions())
}

fn extract_commits_height_range_from_conditions(
    conditions: &serde_json::Map<String, JsonValue>,
) -> HeightRangeExtraction {
    let mut extracted = HeightRangeExtraction::None;

    for (field_name, value) in conditions {
        if field_name == "height" {
            extracted = extracted.merge_and(parse_height_condition(value));
            continue;
        }

        match crate::mapper::FilterOp::parse(field_name) {
            Some(crate::mapper::FilterOp::And) => {
                if value.is_null() {
                    continue;
                }
                let Some(items) = value.as_array() else {
                    return HeightRangeExtraction::Unsupported;
                };
                for item in items {
                    let Ok(sub_conditions) =
                        serde_json::from_value::<serde_json::Map<String, JsonValue>>(item.clone())
                    else {
                        return HeightRangeExtraction::Unsupported;
                    };
                    extracted = extracted.merge_and(extract_commits_height_range_from_conditions(
                        &sub_conditions,
                    ));
                }
            }
            Some(crate::mapper::FilterOp::Or | crate::mapper::FilterOp::Not) => {
                if logical_value_contains_top_level_height(value) {
                    return HeightRangeExtraction::Unsupported;
                }
            }
            _ => {}
        }
    }

    extracted
}

fn logical_value_contains_top_level_height(value: &JsonValue) -> bool {
    match value {
        JsonValue::Array(items) => items.iter().any(logical_value_contains_top_level_height),
        JsonValue::Object(obj) => logical_conditions_contain_top_level_height(obj),
        _ => false,
    }
}

fn logical_conditions_contain_top_level_height(
    conditions: &serde_json::Map<String, JsonValue>,
) -> bool {
    conditions.iter().any(|(field_name, value)| {
        field_name == "height"
            || matches!(
                crate::mapper::FilterOp::parse(field_name),
                Some(
                    crate::mapper::FilterOp::And
                        | crate::mapper::FilterOp::Or
                        | crate::mapper::FilterOp::Not
                )
            ) && logical_value_contains_top_level_height(value)
    })
}

pub(crate) fn parse_height_condition(value: &JsonValue) -> HeightRangeExtraction {
    if value.is_null() {
        return HeightRangeExtraction::Empty;
    }

    if let Some(height) = json_value_to_non_negative_u64(value) {
        return HeightRangeExtraction::Range(CommitsHeightRange {
            start: height,
            end: height.checked_add(1),
        });
    }

    let Some(ops) = value.as_object() else {
        return HeightRangeExtraction::Unsupported;
    };

    let mut range = HeightRangeExtraction::Range(CommitsHeightRange::default());

    for (op_str, expected) in ops {
        let Some(op) = crate::mapper::FilterOp::parse(op_str) else {
            return HeightRangeExtraction::Unsupported;
        };
        let Some(height) = json_value_to_non_negative_u64(expected) else {
            return HeightRangeExtraction::Unsupported;
        };

        range = match (range, op) {
            (HeightRangeExtraction::Range(range), crate::mapper::FilterOp::Eq) => {
                range.with_lower_bound(height).merge_and(
                    CommitsHeightRange {
                        start: height,
                        end: height.checked_add(1),
                    }
                    .into(),
                )
            }
            (HeightRangeExtraction::Range(range), crate::mapper::FilterOp::Gt) => {
                if let Some(start) = height.checked_add(1) {
                    range.with_lower_bound(start)
                } else {
                    HeightRangeExtraction::Empty
                }
            }
            (HeightRangeExtraction::Range(range), crate::mapper::FilterOp::Gte) => {
                range.with_lower_bound(height)
            }
            (HeightRangeExtraction::Range(range), crate::mapper::FilterOp::Lt) => {
                range.with_upper_bound(Some(height))
            }
            (HeightRangeExtraction::Range(range), crate::mapper::FilterOp::Lte) => {
                range.with_upper_bound(height.checked_add(1))
            }
            (_, _) => HeightRangeExtraction::Unsupported,
        };

        match range {
            HeightRangeExtraction::Range(_) => {}
            other => return other,
        }
    }

    range
}

impl From<CommitsHeightRange> for HeightRangeExtraction {
    fn from(range: CommitsHeightRange) -> Self {
        Self::Range(range)
    }
}

pub(crate) fn json_value_to_non_negative_u64(value: &JsonValue) -> Option<u64> {
    value.as_i64().and_then(|height| u64::try_from(height).ok())
}
