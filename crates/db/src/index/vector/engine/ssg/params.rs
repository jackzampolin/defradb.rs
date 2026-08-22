//! SSG build and search parameters.

use crate::index::error::{Error, Result};

pub const DEFAULT_R: u32 = 50;
pub const DEFAULT_ANGLE: u32 = 60;
pub const DEFAULT_POOL: u32 = 100;

pub const MAX_R: u32 = 1_024;
pub const MAX_POOL: u32 = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SsgParams {
    /// Maximum degree after pruning.
    pub r: u32,
    /// Minimum angle between two kept edges, in degrees.
    pub angle: u32,
    /// Candidate pool size during search, the paper's `L`.
    pub pool: u32,
}

impl Default for SsgParams {
    fn default() -> Self {
        Self {
            r: DEFAULT_R,
            angle: DEFAULT_ANGLE,
            pool: DEFAULT_POOL,
        }
    }
}

impl SsgParams {
    pub fn validate(&self) -> Result<()> {
        if self.r == 0 || self.r > MAX_R {
            return Err(Error::Other(format!(
                "vector index R is {}, outside 1..={MAX_R}",
                self.r
            )));
        }
        if self.pool == 0 || self.pool > MAX_POOL {
            return Err(Error::Other(format!(
                "vector index pool is {}, outside 1..={MAX_POOL}",
                self.pool
            )));
        }
        // At or above 180 degrees no two edges can coexist and every node would
        // keep exactly one, which is a graph no walk can cross.
        if self.angle == 0 || self.angle >= 180 {
            return Err(Error::Other(format!(
                "vector index angle is {} degrees, outside 1..180",
                self.angle
            )));
        }
        Ok(())
    }
}
