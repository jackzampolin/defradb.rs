//! Opening options for the regolith backend.

use std::time::Duration;

use regolith::{DurabilityMode as EngineDurability, IsolationLevel, Options};

use crate::backends::shared::DurabilityMode;

/// How a [`super::RegolithStore`] opens and commits.
#[derive(Clone)]
pub struct RegolithStoreOptions {
    /// Engine options. Built from a regolith profile, so a target that
    /// needs a different memory budget picks it up by construction rather
    /// than by a caller remembering to tune it.
    pub engine: Options,
    /// Commit-time validation. Defaults to
    /// [`IsolationLevel::Serializable`], which validates the whole read
    /// set: what DefraDB needs, because a merge that reads a head and
    /// writes a block against it must not commit if the head moved.
    pub isolation: IsolationLevel,
    /// How long `close` waits for in-flight transactions to finish.
    pub close_timeout: Duration,
}

impl RegolithStoreOptions {
    /// Server defaults: serializable, fsync on every commit.
    pub fn new() -> Self {
        Self {
            engine: Options {
                durability: EngineDurability::Immediate,
                ..Options::default()
            },
            isolation: IsolationLevel::Serializable,
            close_timeout: Duration::from_secs(30),
        }
    }

    /// A 1-4 MiB working set, for a phone or an edge device.
    pub fn embedded() -> Self {
        Self {
            engine: Options {
                durability: EngineDurability::Immediate,
                ..Options::embedded()
            },
            ..Self::new()
        }
    }

    /// A browser or wasi module. The caller supplies the filesystem: on
    /// `wasm32-unknown-unknown` there is none, so mount OPFS and set
    /// [`RegolithStoreOptions::engine`]'s `env` before opening.
    pub fn wasm() -> Self {
        Self {
            engine: Options {
                durability: EngineDurability::Immediate,
                ..Options::wasm()
            },
            ..Self::new()
        }
    }

    /// Keep the database in memory. For tests and for a node that is
    /// explicitly ephemeral.
    pub fn memory() -> Self {
        // Take the environment constraints from the engine's own preset,
        // which is where the knowledge lives: `MemEnv` starts no threads, so
        // a compaction worker cannot exist, and nothing is being made durable,
        // so an fsync per commit would buy nothing.
        //
        // The size limits stay the server ones. `Options::memory()` is sized
        // from the embedded profile, whose 256 KiB value ceiling is a limit a
        // caller can hit rather than a memory-tuning knob: a document larger
        // than that is ordinary here, and an in-memory store that refuses what
        // the on-disk one accepts is a different database, not a faster one.
        let preset = Options::memory();
        let mut opts = Self::new();
        opts.engine.env = preset.env;
        opts.engine.max_background_compactions = preset.max_background_compactions;
        opts.engine.durability = preset.durability;
        opts
    }

    /// Relax commit validation. Serializable is the default because it is
    /// what the merge and index paths need; a caller that knows its unit
    /// of work is a blind write can ask for less.
    pub fn with_isolation(mut self, isolation: IsolationLevel) -> Self {
        self.isolation = isolation;
        self
    }

    /// Trade durability for throughput: `Eventual` survives a process
    /// crash but not a power cut.
    pub fn with_durability(mut self, durability: DurabilityMode) -> Self {
        self.engine.durability = match durability {
            DurabilityMode::Immediate => EngineDurability::Immediate,
            DurabilityMode::Eventual => EngineDurability::Eventual,
        };
        self
    }
}

impl Default for RegolithStoreOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for RegolithStoreOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegolithStoreOptions")
            .field("isolation", &self.isolation)
            .field("durability", &self.engine.durability)
            .field("close_timeout", &self.close_timeout)
            .finish_non_exhaustive()
    }
}
