//! Index matchers for filter condition evaluation
//!
//! Provides type-specific matchers for filter operators that can be used
//! to evaluate whether index entries match filter conditions.

use document::NormalValue;

use crate::corekv::MaybeSendSync;

#[cfg(test)]
mod tests;

/// Trait for matching values against filter conditions.
pub trait IndexMatcher: MaybeSendSync {
    /// Check if the given value matches this condition.
    fn matches(&self, value: &NormalValue) -> bool;
}

/// Matcher for equality (_eq) conditions.
pub struct EqMatcher {
    value: NormalValue,
}

impl EqMatcher {
    pub fn new(value: NormalValue) -> Self {
        Self { value }
    }
}

impl IndexMatcher for EqMatcher {
    fn matches(&self, value: &NormalValue) -> bool {
        values_equal(value, &self.value)
    }
}

/// Matcher for not-equal (_ne) conditions.
pub struct NeMatcher {
    value: NormalValue,
}

impl NeMatcher {
    pub fn new(value: NormalValue) -> Self {
        Self { value }
    }
}

impl IndexMatcher for NeMatcher {
    fn matches(&self, value: &NormalValue) -> bool {
        !values_equal(value, &self.value)
    }
}

/// Matcher for greater-than (_gt) and greater-than-or-equal (_gte) conditions.
pub struct GtMatcher {
    value: NormalValue,
    inclusive: bool,
}

impl GtMatcher {
    /// Create a greater-than matcher (>).
    pub fn new(value: NormalValue) -> Self {
        Self {
            value,
            inclusive: false,
        }
    }

    /// Create a greater-than-or-equal matcher (>=).
    pub fn new_inclusive(value: NormalValue) -> Self {
        Self {
            value,
            inclusive: true,
        }
    }
}

impl IndexMatcher for GtMatcher {
    fn matches(&self, value: &NormalValue) -> bool {
        match compare_values(value, &self.value) {
            Some(std::cmp::Ordering::Greater) => true,
            Some(std::cmp::Ordering::Equal) => self.inclusive,
            _ => false,
        }
    }
}

/// Matcher for less-than (_lt) and less-than-or-equal (_lte) conditions.
pub struct LtMatcher {
    value: NormalValue,
    inclusive: bool,
}

impl LtMatcher {
    /// Create a less-than matcher (<).
    pub fn new(value: NormalValue) -> Self {
        Self {
            value,
            inclusive: false,
        }
    }

    /// Create a less-than-or-equal matcher (<=).
    pub fn new_inclusive(value: NormalValue) -> Self {
        Self {
            value,
            inclusive: true,
        }
    }
}

impl IndexMatcher for LtMatcher {
    fn matches(&self, value: &NormalValue) -> bool {
        match compare_values(value, &self.value) {
            Some(std::cmp::Ordering::Less) => true,
            Some(std::cmp::Ordering::Equal) => self.inclusive,
            _ => false,
        }
    }
}

/// Matcher for _in conditions (value in set).
pub struct InMatcher {
    values: Vec<NormalValue>,
}

impl InMatcher {
    pub fn new(values: Vec<NormalValue>) -> Self {
        Self { values }
    }
}

impl IndexMatcher for InMatcher {
    fn matches(&self, value: &NormalValue) -> bool {
        self.values.iter().any(|v| values_equal(value, v))
    }
}

/// Matcher for _nin conditions (value not in set).
pub struct NinMatcher {
    values: Vec<NormalValue>,
}

impl NinMatcher {
    pub fn new(values: Vec<NormalValue>) -> Self {
        Self { values }
    }
}

impl IndexMatcher for NinMatcher {
    fn matches(&self, value: &NormalValue) -> bool {
        !self.values.iter().any(|v| values_equal(value, v))
    }
}

/// Matcher for _like conditions (SQL LIKE pattern matching).
///
/// `%` matches zero or more characters. `_` is treated as literal
/// (matches Go DefraDB behavior). Supports arbitrary patterns.
pub struct LikeMatcher {
    pattern: String,
}

impl LikeMatcher {
    pub fn new(pattern: &str) -> Option<Self> {
        Some(Self {
            pattern: pattern.to_string(),
        })
    }
}

impl IndexMatcher for LikeMatcher {
    fn matches(&self, value: &NormalValue) -> bool {
        let s = match value.as_str() {
            Some(s) => s,
            None => return false,
        };
        like_match(s, &self.pattern)
    }
}

/// Matcher for negated _like conditions (_nlike).
pub struct NlikeMatcher {
    inner: LikeMatcher,
}

impl NlikeMatcher {
    pub fn new(pattern: &str) -> Option<Self> {
        LikeMatcher::new(pattern).map(|inner| Self { inner })
    }
}

impl IndexMatcher for NlikeMatcher {
    fn matches(&self, value: &NormalValue) -> bool {
        // Non-string values don't participate in LIKE matching at all (Go behavior).
        // Both _like and _nlike return false for non-strings.
        if value.as_str().is_none() {
            return false;
        }
        !self.inner.matches(value)
    }
}

/// SQL LIKE pattern matching with `%` as wildcard for zero or more characters.
/// `_` is treated as a literal character (matches Go DefraDB behavior).
fn like_match(text: &str, pattern: &str) -> bool {
    let text_bytes = text.as_bytes();
    let pattern_bytes = pattern.as_bytes();
    let p_len = pattern_bytes.len();

    let mut dp = vec![false; p_len + 1];
    dp[0] = true;

    for j in 0..p_len {
        if pattern_bytes[j] == b'%' {
            dp[j + 1] = dp[j];
        } else {
            break;
        }
    }

    for &text_byte in text_bytes {
        let mut prev = dp[0];
        dp[0] = false;
        for j in 0..p_len {
            let temp = dp[j + 1];
            if pattern_bytes[j] == b'%' {
                dp[j + 1] = prev || dp[j + 1];
            } else if text_byte == pattern_bytes[j] {
                dp[j + 1] = prev;
            } else {
                dp[j + 1] = false;
            }
            prev = temp;
        }
    }

    // Propagate through trailing % wildcards (each matches empty string)
    for j in 0..p_len {
        if pattern_bytes[j] == b'%' && dp[j] {
            dp[j + 1] = true;
        }
    }

    dp[p_len]
}

/// Compare two NormalValue instances for equality.
fn values_equal(a: &NormalValue, b: &NormalValue) -> bool {
    match (a, b) {
        (NormalValue::Null, NormalValue::Null) => true,
        (NormalValue::Bool(a), NormalValue::Bool(b)) => a == b,
        (NormalValue::Int(a), NormalValue::Int(b)) => a == b,
        (NormalValue::Float64(a), NormalValue::Float64(b)) => (a - b).abs() < f64::EPSILON,
        (NormalValue::Float32(a), NormalValue::Float32(b)) => (a - b).abs() < f32::EPSILON,
        (NormalValue::String(a), NormalValue::String(b)) => a == b,
        (NormalValue::Bytes(a), NormalValue::Bytes(b)) => a == b,
        (NormalValue::Time(a), NormalValue::Time(b)) => a == b,
        // Handle nillable variants with inner values
        (NormalValue::NillableBool(Some(a)), NormalValue::Bool(b)) => a == b,
        (NormalValue::Bool(a), NormalValue::NillableBool(Some(b))) => a == b,
        (NormalValue::NillableInt(Some(a)), NormalValue::Int(b)) => a == b,
        (NormalValue::Int(a), NormalValue::NillableInt(Some(b))) => a == b,
        (NormalValue::NillableFloat64(Some(a)), NormalValue::Float64(b)) => {
            (a - b).abs() < f64::EPSILON
        }
        (NormalValue::Float64(a), NormalValue::NillableFloat64(Some(b))) => {
            (a - b).abs() < f64::EPSILON
        }
        (NormalValue::NillableString(Some(a)), NormalValue::String(b)) => a == b,
        (NormalValue::String(a), NormalValue::NillableString(Some(b))) => a == b,
        // Nillable None values are considered null
        (NormalValue::NillableBool(None), NormalValue::Null) => true,
        (NormalValue::Null, NormalValue::NillableBool(None)) => true,
        (NormalValue::NillableInt(None), NormalValue::Null) => true,
        (NormalValue::Null, NormalValue::NillableInt(None)) => true,
        (NormalValue::NillableFloat64(None), NormalValue::Null) => true,
        (NormalValue::Null, NormalValue::NillableFloat64(None)) => true,
        (NormalValue::NillableString(None), NormalValue::Null) => true,
        (NormalValue::Null, NormalValue::NillableString(None)) => true,
        _ => false,
    }
}

/// Compare two NormalValue instances for ordering.
fn compare_values(a: &NormalValue, b: &NormalValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        // Integer comparisons
        (NormalValue::Int(a), NormalValue::Int(b)) => Some(a.cmp(b)),
        (NormalValue::NillableInt(Some(a)), NormalValue::Int(b)) => Some(a.cmp(b)),
        (NormalValue::Int(a), NormalValue::NillableInt(Some(b))) => Some(a.cmp(b)),

        // Float64 comparisons
        (NormalValue::Float64(a), NormalValue::Float64(b)) => a.partial_cmp(b),
        (NormalValue::NillableFloat64(Some(a)), NormalValue::Float64(b)) => a.partial_cmp(b),
        (NormalValue::Float64(a), NormalValue::NillableFloat64(Some(b))) => a.partial_cmp(b),

        // Float32 comparisons
        (NormalValue::Float32(a), NormalValue::Float32(b)) => a.partial_cmp(b),
        (NormalValue::NillableFloat32(Some(a)), NormalValue::Float32(b)) => a.partial_cmp(b),
        (NormalValue::Float32(a), NormalValue::NillableFloat32(Some(b))) => a.partial_cmp(b),

        // String comparisons
        (NormalValue::String(a), NormalValue::String(b)) => Some(a.cmp(b)),
        (NormalValue::NillableString(Some(a)), NormalValue::String(b)) => Some(a.cmp(b)),
        (NormalValue::String(a), NormalValue::NillableString(Some(b))) => Some(a.cmp(b)),

        // Time comparisons
        (NormalValue::Time(a), NormalValue::Time(b)) => Some(a.cmp(b)),
        (NormalValue::NillableTime(Some(a)), NormalValue::Time(b)) => Some(a.cmp(b)),
        (NormalValue::Time(a), NormalValue::NillableTime(Some(b))) => Some(a.cmp(b)),

        // Cross-type numeric comparisons (Int to Float)
        (NormalValue::Int(a), NormalValue::Float64(b)) => (*a as f64).partial_cmp(b),
        (NormalValue::Float64(a), NormalValue::Int(b)) => a.partial_cmp(&(*b as f64)),

        _ => None,
    }
}
