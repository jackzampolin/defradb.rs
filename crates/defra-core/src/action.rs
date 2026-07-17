use serde::{Deserialize, Serialize};

/// A long-running database operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Action(u16);

impl Action {
    pub const NONE: Self = Self(0);
    pub const TRUNCATE: Self = Self(1);
    pub const REFRESH_DATASTORE: Self = Self(2);
    pub const BACKFILL_INDEX: Self = Self(3);
    pub const DROP_INDEX: Self = Self(4);

    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u16 {
        self.0
    }
}

/// The current state of an action execution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ActionStatus(u16);

impl ActionStatus {
    pub const NONE: Self = Self(0);
    pub const IN_PROGRESS: Self = Self(1);
    pub const ERRORED: Self = Self(2);
    pub const COMPLETED: Self = Self(3);

    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u16 {
        self.0
    }
}

/// Observable state for one action execution.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionExecution {
    #[serde(rename = "CollectionID")]
    pub collection_id: String,
    #[serde(rename = "Action")]
    pub action: Action,
    #[serde(rename = "Subject")]
    pub subject: String,
    #[serde(rename = "Status")]
    pub status: ActionStatus,
    #[serde(rename = "Reason")]
    pub reason: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_execution_matches_go_wire_shape() {
        let execution = ActionExecution {
            collection_id: "bafycollection".to_string(),
            action: Action::TRUNCATE,
            status: ActionStatus::ERRORED,
            reason: "failed".to_string(),
            ..Default::default()
        };

        assert_eq!(
            serde_json::to_value(execution).unwrap(),
            serde_json::json!({
                "CollectionID": "bafycollection",
                "Action": 1,
                "Subject": "",
                "Status": 2,
                "Reason": "failed",
            })
        );
    }
}
