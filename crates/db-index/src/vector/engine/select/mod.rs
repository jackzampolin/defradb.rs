//! Edge-selection strategies.

mod angular;
mod heuristic;

pub use angular::{Angular, DEFAULT_ANGLE_DEGREES};
pub use heuristic::Heuristic;
