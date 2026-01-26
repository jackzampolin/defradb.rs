//! Schema version history graph traversal.
//!
//! Matches Go's internal/lens/history.go types and functions.

use std::collections::HashMap;

/// Link in the collection schema history.
///
/// Represents a node in the version DAG, linking to previous and next versions.
/// Matches Go's collectionHistoryLink.
#[derive(Debug, Clone)]
pub struct CollectionHistoryLink {
    /// The schema version ID at this point in history.
    pub version_id: String,

    /// Collection ID (schema root).
    pub collection_id: String,

    /// Optional transform ID to apply when migrating from previous version.
    pub transform: Option<String>,

    /// Previous version IDs (empty for initial version).
    pub previous: Vec<String>,

    /// Next version IDs (empty for most recent version).
    pub next: Vec<String>,
}

impl CollectionHistoryLink {
    /// Create a new history link.
    pub fn new(version_id: impl Into<String>, collection_id: impl Into<String>) -> Self {
        Self {
            version_id: version_id.into(),
            collection_id: collection_id.into(),
            transform: None,
            previous: Vec::new(),
            next: Vec::new(),
        }
    }

    /// Set the transform for this version.
    pub fn with_transform(mut self, transform: impl Into<String>) -> Self {
        self.transform = Some(transform.into());
        self
    }

    /// Add a previous version link.
    pub fn with_previous(mut self, version_id: impl Into<String>) -> Self {
        self.previous.push(version_id.into());
        self
    }

    /// Add a next version link.
    pub fn with_next(mut self, version_id: impl Into<String>) -> Self {
        self.next.push(version_id.into());
        self
    }
}

/// Link in the targeted collection schema history.
///
/// Represents a path through the version DAG relative to a target version.
/// Each link points to at most one previous and one next version (the path to target).
/// Matches Go's targetedCollectionHistoryLink.
#[derive(Debug, Clone)]
pub struct TargetedHistoryLink {
    /// The schema version ID at this point in history.
    pub version_id: String,

    /// Collection ID (schema root).
    pub collection_id: String,

    /// Optional transform ID to apply when migrating from previous version.
    pub transform: Option<String>,

    /// Previous version ID on the path to target (None for initial version).
    pub previous: Option<String>,

    /// Next version ID on the path to target (None for target version).
    pub next: Option<String>,
}

impl TargetedHistoryLink {
    /// Create a new targeted history link.
    pub fn new(version_id: impl Into<String>, collection_id: impl Into<String>) -> Self {
        Self {
            version_id: version_id.into(),
            collection_id: collection_id.into(),
            transform: None,
            previous: None,
            next: None,
        }
    }

    /// Set the transform for this version.
    pub fn with_transform(mut self, transform: Option<String>) -> Self {
        self.transform = transform;
        self
    }

    /// Set the previous version on the path to target.
    pub fn with_previous(mut self, version_id: impl Into<String>) -> Self {
        self.previous = Some(version_id.into());
        self
    }

    /// Set the next version on the path to target.
    pub fn with_next(mut self, version_id: impl Into<String>) -> Self {
        self.next = Some(version_id.into());
        self
    }
}

/// Build targeted history from a full history graph.
///
/// Returns a map of version IDs to their targeted links, representing paths to the target version.
/// Matches Go's getTargetedCollectionHistory.
pub fn build_targeted_history(
    history: &HashMap<String, CollectionHistoryLink>,
    target_version_id: &str,
) -> Option<HashMap<String, TargetedHistoryLink>> {
    let target_item = history.get(target_version_id)?;

    let mut result = HashMap::new();

    // Create the target link
    let target_link = TargetedHistoryLink::new(&target_item.version_id, &target_item.collection_id)
        .with_transform(target_item.transform.clone());
    result.insert(target_version_id.to_string(), target_link);

    // Link forwards from target (to newer versions)
    link_forwards(history, target_item, &mut result);

    // Link backwards from target (to older versions)
    link_backwards(history, target_item, &mut result);

    Some(result)
}

/// Traverse and link history forwards from the current item.
fn link_forwards(
    history: &HashMap<String, CollectionHistoryLink>,
    current_item: &CollectionHistoryLink,
    result: &mut HashMap<String, TargetedHistoryLink>,
) {
    for next_version_id in &current_item.next {
        if result.contains_key(next_version_id) {
            // Already visited (DAG traversal)
            continue;
        }

        let next_item = match history.get(next_version_id) {
            Some(item) => item,
            None => continue,
        };

        let next_link = TargetedHistoryLink::new(&next_item.version_id, &next_item.collection_id)
            .with_transform(next_item.transform.clone())
            .with_previous(&current_item.version_id);

        result.insert(next_version_id.clone(), next_link);

        // Update current link to point to next
        if let Some(current_link) = result.get_mut(&current_item.version_id) {
            if current_link.next.is_none() {
                current_link.next = Some(next_version_id.clone());
            }
        }

        // Continue traversal
        link_forwards(history, next_item, result);
        link_backwards(history, next_item, result);
    }
}

/// Traverse and link history backwards from the current item.
fn link_backwards(
    history: &HashMap<String, CollectionHistoryLink>,
    current_item: &CollectionHistoryLink,
    result: &mut HashMap<String, TargetedHistoryLink>,
) {
    for prev_version_id in &current_item.previous {
        if result.contains_key(prev_version_id) {
            // Already visited (DAG traversal)
            continue;
        }

        let prev_item = match history.get(prev_version_id) {
            Some(item) => item,
            None => continue,
        };

        let prev_link = TargetedHistoryLink::new(&prev_item.version_id, &prev_item.collection_id)
            .with_transform(prev_item.transform.clone())
            .with_next(&current_item.version_id);

        result.insert(prev_version_id.clone(), prev_link);

        // Update current link to point to previous
        if let Some(current_link) = result.get_mut(&current_item.version_id) {
            if current_link.previous.is_none() {
                current_link.previous = Some(prev_version_id.clone());
            }
        }

        // Continue traversal
        link_forwards(history, prev_item, result);
        link_backwards(history, prev_item, result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_linear_history() -> HashMap<String, CollectionHistoryLink> {
        // v1 -> v2 -> v3 (linear history)
        let mut history = HashMap::new();

        let v1 = CollectionHistoryLink::new("v1", "collection_1").with_next("v2");
        let v2 = CollectionHistoryLink::new("v2", "collection_1")
            .with_transform("transform_v1_v2")
            .with_previous("v1")
            .with_next("v3");
        let v3 = CollectionHistoryLink::new("v3", "collection_1")
            .with_transform("transform_v2_v3")
            .with_previous("v2");

        history.insert("v1".to_string(), v1);
        history.insert("v2".to_string(), v2);
        history.insert("v3".to_string(), v3);

        history
    }

    #[test]
    fn test_build_targeted_history_at_target() {
        let history = create_linear_history();
        let targeted = build_targeted_history(&history, "v3").unwrap();

        assert_eq!(targeted.len(), 3);

        // v3 is the target
        let v3_link = targeted.get("v3").unwrap();
        assert!(v3_link.next.is_none());
        assert_eq!(v3_link.previous, Some("v2".to_string()));

        // v2 links both ways
        let v2_link = targeted.get("v2").unwrap();
        assert_eq!(v2_link.next, Some("v3".to_string()));
        assert_eq!(v2_link.previous, Some("v1".to_string()));

        // v1 is the oldest
        let v1_link = targeted.get("v1").unwrap();
        assert_eq!(v1_link.next, Some("v2".to_string()));
        assert!(v1_link.previous.is_none());
    }

    #[test]
    fn test_build_targeted_history_middle_target() {
        let history = create_linear_history();
        let targeted = build_targeted_history(&history, "v2").unwrap();

        assert_eq!(targeted.len(), 3);

        // v2 is the target
        let v2_link = targeted.get("v2").unwrap();
        assert_eq!(v2_link.transform, Some("transform_v1_v2".to_string()));
    }

    #[test]
    fn test_build_targeted_history_unknown_target() {
        let history = create_linear_history();
        let targeted = build_targeted_history(&history, "v_unknown");

        assert!(targeted.is_none());
    }
}
