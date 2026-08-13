//! One search hit, shared by every index kind.

use crate::vector::store::NodeId;

/// A node and how far it is from the query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Neighbor {
    pub id: NodeId,
    pub distance: f64,
}
