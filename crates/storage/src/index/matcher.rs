//! Index matchers for filter condition evaluation
//!
//! Provides type-specific matchers for filter operators that can be used
//! to evaluate whether index entries match filter conditions.

use document::NormalValue;

use crate::corekv::MaybeSendSync;

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

/// Matcher for _like conditions (pattern matching).
///
/// Supports simple patterns:
/// - `prefix%` (starts with)
/// - `%suffix` (ends with)
/// - `%contains%` (contains)
/// - `exact` (exact match)
pub struct LikeMatcher {
    pattern: LikePattern,
}

enum LikePattern {
    StartsWith(String),
    EndsWith(String),
    Contains(String),
    Exact(String),
}

impl LikeMatcher {
    /// Create a new LIKE matcher from a pattern string.
    ///
    /// Returns None if the pattern is invalid or unsupported.
    pub fn new(pattern: &str) -> Option<Self> {
        // Check for unsupported patterns
        if pattern.contains('_') {
            return None;
        }

        let percent_count = pattern.matches('%').count();
        if percent_count > 2 {
            return None;
        }
        if percent_count == 2 && !(pattern.starts_with('%') && pattern.ends_with('%')) {
            return None;
        }

        let pattern =
            if let Some(inner) = pattern.strip_prefix('%').and_then(|s| s.strip_suffix('%')) {
                LikePattern::Contains(inner.to_string())
            } else if let Some(suffix) = pattern.strip_prefix('%') {
                LikePattern::EndsWith(suffix.to_string())
            } else if let Some(prefix) = pattern.strip_suffix('%') {
                LikePattern::StartsWith(prefix.to_string())
            } else {
                LikePattern::Exact(pattern.to_string())
            };

        Some(Self { pattern })
    }
}

impl IndexMatcher for LikeMatcher {
    fn matches(&self, value: &NormalValue) -> bool {
        let s = match value.as_str() {
            Some(s) => s,
            None => return false,
        };

        match &self.pattern {
            LikePattern::StartsWith(prefix) => s.starts_with(prefix),
            LikePattern::EndsWith(suffix) => s.ends_with(suffix),
            LikePattern::Contains(inner) => s.contains(inner),
            LikePattern::Exact(exact) => s == exact,
        }
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
        !self.inner.matches(value)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eq_matcher() {
        let matcher = EqMatcher::new(NormalValue::Int(42));
        assert!(matcher.matches(&NormalValue::Int(42)));
        assert!(!matcher.matches(&NormalValue::Int(41)));
        assert!(!matcher.matches(&NormalValue::String("42".to_string())));
    }

    #[test]
    fn test_eq_matcher_string() {
        let matcher = EqMatcher::new(NormalValue::String("alice".to_string()));
        assert!(matcher.matches(&NormalValue::String("alice".to_string())));
        assert!(!matcher.matches(&NormalValue::String("bob".to_string())));
    }

    #[test]
    fn test_eq_matcher_null() {
        let matcher = EqMatcher::new(NormalValue::Null);
        assert!(matcher.matches(&NormalValue::Null));
        assert!(!matcher.matches(&NormalValue::Int(0)));
    }

    #[test]
    fn test_ne_matcher() {
        let matcher = NeMatcher::new(NormalValue::Int(42));
        assert!(!matcher.matches(&NormalValue::Int(42)));
        assert!(matcher.matches(&NormalValue::Int(41)));
    }

    #[test]
    fn test_gt_matcher() {
        let matcher = GtMatcher::new(NormalValue::Int(10));
        assert!(matcher.matches(&NormalValue::Int(11)));
        assert!(!matcher.matches(&NormalValue::Int(10)));
        assert!(!matcher.matches(&NormalValue::Int(9)));
    }

    #[test]
    fn test_gt_matcher_inclusive() {
        let matcher = GtMatcher::new_inclusive(NormalValue::Int(10));
        assert!(matcher.matches(&NormalValue::Int(11)));
        assert!(matcher.matches(&NormalValue::Int(10)));
        assert!(!matcher.matches(&NormalValue::Int(9)));
    }

    #[test]
    fn test_gt_matcher_string() {
        let matcher = GtMatcher::new(NormalValue::String("bob".to_string()));
        assert!(matcher.matches(&NormalValue::String("charlie".to_string())));
        assert!(!matcher.matches(&NormalValue::String("bob".to_string())));
        assert!(!matcher.matches(&NormalValue::String("alice".to_string())));
    }

    #[test]
    fn test_lt_matcher() {
        let matcher = LtMatcher::new(NormalValue::Int(10));
        assert!(matcher.matches(&NormalValue::Int(9)));
        assert!(!matcher.matches(&NormalValue::Int(10)));
        assert!(!matcher.matches(&NormalValue::Int(11)));
    }

    #[test]
    fn test_lt_matcher_inclusive() {
        let matcher = LtMatcher::new_inclusive(NormalValue::Int(10));
        assert!(matcher.matches(&NormalValue::Int(9)));
        assert!(matcher.matches(&NormalValue::Int(10)));
        assert!(!matcher.matches(&NormalValue::Int(11)));
    }

    #[test]
    fn test_in_matcher() {
        let matcher = InMatcher::new(vec![
            NormalValue::Int(1),
            NormalValue::Int(2),
            NormalValue::Int(3),
        ]);
        assert!(matcher.matches(&NormalValue::Int(1)));
        assert!(matcher.matches(&NormalValue::Int(2)));
        assert!(matcher.matches(&NormalValue::Int(3)));
        assert!(!matcher.matches(&NormalValue::Int(4)));
    }

    #[test]
    fn test_in_matcher_string() {
        let matcher = InMatcher::new(vec![
            NormalValue::String("alice".to_string()),
            NormalValue::String("bob".to_string()),
        ]);
        assert!(matcher.matches(&NormalValue::String("alice".to_string())));
        assert!(matcher.matches(&NormalValue::String("bob".to_string())));
        assert!(!matcher.matches(&NormalValue::String("charlie".to_string())));
    }

    #[test]
    fn test_nin_matcher() {
        let matcher = NinMatcher::new(vec![NormalValue::Int(1), NormalValue::Int(2)]);
        assert!(!matcher.matches(&NormalValue::Int(1)));
        assert!(!matcher.matches(&NormalValue::Int(2)));
        assert!(matcher.matches(&NormalValue::Int(3)));
    }

    #[test]
    fn test_like_matcher_starts_with() {
        let matcher = LikeMatcher::new("Ali%").unwrap();
        assert!(matcher.matches(&NormalValue::String("Alice".to_string())));
        assert!(matcher.matches(&NormalValue::String("Alicia".to_string())));
        assert!(!matcher.matches(&NormalValue::String("Bob".to_string())));
    }

    #[test]
    fn test_like_matcher_ends_with() {
        let matcher = LikeMatcher::new("%ice").unwrap();
        assert!(matcher.matches(&NormalValue::String("Alice".to_string())));
        assert!(matcher.matches(&NormalValue::String("ice".to_string())));
        assert!(!matcher.matches(&NormalValue::String("Bob".to_string())));
    }

    #[test]
    fn test_like_matcher_contains() {
        let matcher = LikeMatcher::new("%lic%").unwrap();
        assert!(matcher.matches(&NormalValue::String("Alice".to_string())));
        assert!(matcher.matches(&NormalValue::String("delicate".to_string())));
        assert!(!matcher.matches(&NormalValue::String("Bob".to_string())));
    }

    #[test]
    fn test_like_matcher_exact() {
        let matcher = LikeMatcher::new("Alice").unwrap();
        assert!(matcher.matches(&NormalValue::String("Alice".to_string())));
        assert!(!matcher.matches(&NormalValue::String("Alice!".to_string())));
    }

    #[test]
    fn test_like_matcher_invalid_patterns() {
        assert!(LikeMatcher::new("Al_ce").is_none()); // underscore not supported
        assert!(LikeMatcher::new("%a%b%").is_none()); // too many percent signs
    }

    #[test]
    fn test_nlike_matcher() {
        let matcher = NlikeMatcher::new("Ali%").unwrap();
        assert!(!matcher.matches(&NormalValue::String("Alice".to_string())));
        assert!(matcher.matches(&NormalValue::String("Bob".to_string())));
    }

    #[test]
    fn test_like_matcher_non_string_returns_false() {
        let matcher = LikeMatcher::new("test%").unwrap();
        assert!(!matcher.matches(&NormalValue::Int(42)));
        assert!(!matcher.matches(&NormalValue::Null));
    }

    #[test]
    fn test_cross_type_numeric_comparison() {
        let matcher = GtMatcher::new(NormalValue::Float64(10.5));
        assert!(matcher.matches(&NormalValue::Int(11)));
        assert!(!matcher.matches(&NormalValue::Int(10)));
    }

    #[test]
    fn test_nillable_equality() {
        let matcher = EqMatcher::new(NormalValue::Int(42));
        assert!(matcher.matches(&NormalValue::NillableInt(Some(42))));
        assert!(!matcher.matches(&NormalValue::NillableInt(None)));
    }

    #[test]
    fn test_nillable_null_equality() {
        let matcher = EqMatcher::new(NormalValue::Null);
        assert!(matcher.matches(&NormalValue::NillableInt(None)));
        assert!(matcher.matches(&NormalValue::NillableString(None)));
    }

    #[test]
    fn test_float_equality_epsilon() {
        let matcher = EqMatcher::new(NormalValue::Float64(0.1 + 0.2));
        assert!(matcher.matches(&NormalValue::Float64(0.3)));
    }
}
