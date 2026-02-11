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
fn test_like_matcher_multi_wildcard() {
    let matcher = LikeMatcher::new("%a%b%").unwrap();
    assert!(matcher.matches(&NormalValue::String("a_b".to_string())));
    assert!(matcher.matches(&NormalValue::String("xaxbx".to_string())));
    assert!(!matcher.matches(&NormalValue::String("xyz".to_string())));
}

#[test]
fn test_like_matcher_underscore_literal() {
    // '_' is treated as literal character (Go behavior)
    let matcher = LikeMatcher::new("Al_ce").unwrap();
    assert!(matcher.matches(&NormalValue::String("Al_ce".to_string())));
    assert!(!matcher.matches(&NormalValue::String("Alice".to_string())));
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
