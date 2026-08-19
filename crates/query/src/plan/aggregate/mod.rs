//! Aggregate nodes for COUNT, SUM, MAX, MIN, AVG
//!
//! Count, sum, max, and min share common structure via `AggregateNode<Op>`.
//! Average has a custom implementation due to its unique explain output format.

mod average;
mod count;
mod max;
mod min;
mod node;
mod sum;

pub use average::{AverageNode, AvgOp, AvgSourceMeta};
pub use count::{CountOp, CountSourceMeta};
pub use max::{MaxOp, MaxSourceMeta};
pub use min::{MinOp, MinSourceMeta};
pub use node::{AggregateNode, AggregateOp, NumericSourceMeta};
pub use sum::{SumOp, SumSourceMeta};

use crate::document::DocumentMapping;
use crate::planner::PlanNode;

/// Type aliases for aggregate nodes
pub type CountNode = AggregateNode<CountOp>;
pub type SumNode = AggregateNode<SumOp>;
pub type MaxNode = AggregateNode<MaxOp>;
pub type MinNode = AggregateNode<MinOp>;

// Type-specific constructors to match original API

impl CountNode {
    /// Create a new CountNode (aggregate_index only, no field_index)
    pub fn new(
        source: Box<dyn PlanNode>,
        document_mapping: DocumentMapping,
        aggregate_index: usize,
    ) -> Self {
        AggregateNode::new_without_field(source, document_mapping, aggregate_index)
    }
}

impl SumNode {
    /// Create a new SumNode with field_index and aggregate_index
    pub fn new(
        source: Box<dyn PlanNode>,
        document_mapping: DocumentMapping,
        field_index: usize,
        aggregate_index: usize,
    ) -> Self {
        AggregateNode::new_with_field(source, document_mapping, field_index, aggregate_index)
    }
}

impl MaxNode {
    /// Create a new MaxNode with field_index and aggregate_index
    pub fn new(
        source: Box<dyn PlanNode>,
        document_mapping: DocumentMapping,
        field_index: usize,
        aggregate_index: usize,
    ) -> Self {
        AggregateNode::new_with_field(source, document_mapping, field_index, aggregate_index)
    }
}

impl MinNode {
    /// Create a new MinNode with field_index and aggregate_index
    pub fn new(
        source: Box<dyn PlanNode>,
        document_mapping: DocumentMapping,
        field_index: usize,
        aggregate_index: usize,
    ) -> Self {
        AggregateNode::new_with_field(source, document_mapping, field_index, aggregate_index)
    }
}
