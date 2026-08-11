//! Level generation: how tall a new node is.

/// Draws a node's top layer from the exponential-decay distribution of the HNSW
/// paper, `floor(-ln(u) * ml)` for `u` uniform in `(0, 1]`.
///
/// The decay is what makes the graph a hierarchy: almost every node lands on
/// layer 0 and each layer above holds exponentially fewer, so a search covers
/// ground cheaply at the sparse top before the fine-grained walk at the bottom.
///
/// SplitMix64 rather than a random-number crate. The seed is the caller's, so
/// a sequence of inserts is reproducible; the generator is fixed here forever,
/// so a recall figure measured today means the same thing next year; and
/// nothing needs an entropy source, which is what makes this work unchanged on
/// `wasm32-unknown-unknown`, where the usual ones need a JS shim.
#[derive(Debug, Clone)]
pub struct LevelSampler {
    state: u64,
}

/// Mantissa bits in an `f64`. The sampler draws exactly this many, which is
/// what makes the smallest `u` it can produce `2^-53` and therefore bounds the
/// tallest node it can ask for (see [`LevelSampler::max_level`]).
const MANTISSA_BITS: u32 = f64::MANTISSA_DIGITS - 1;

/// SplitMix64's golden-ratio increment and its two mixing multipliers, from
/// Steele, Lea & Flood, "Fast splittable pseudorandom number generators".
const GOLDEN_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;
const MIX_MULTIPLIER_1: u64 = 0xBF58_476D_1CE4_E5B9;
const MIX_MULTIPLIER_2: u64 = 0x94D0_49BB_1331_11EB;
const MIX_SHIFT_1: u32 = 30;
const MIX_SHIFT_2: u32 = 27;
const MIX_SHIFT_3: u32 = 31;

impl LevelSampler {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GOLDEN_GAMMA);
        let mut z = self.state;
        z = (z ^ (z >> MIX_SHIFT_1)).wrapping_mul(MIX_MULTIPLIER_1);
        z = (z ^ (z >> MIX_SHIFT_2)).wrapping_mul(MIX_MULTIPLIER_2);
        z ^ (z >> MIX_SHIFT_3)
    }

    /// Uniform in `(0, 1]`. Open at zero so `ln` is always defined, which is
    /// why the reference implementation has to redraw and this does not.
    fn next_unit(&mut self) -> f64 {
        let bits = self.next_u64() >> (u64::BITS - MANTISSA_BITS);
        ((bits + 1) as f64) / ((1u64 << MANTISSA_BITS) as f64)
    }

    /// A node's top layer.
    pub fn level(&mut self, ml: f64) -> usize {
        let level = -self.next_unit().ln() * ml;
        // `level` is finite and non-negative for every `u` in (0, 1] and every
        // finite `ml`, so this cannot saturate to a surprising value; the cast
        // is a floor.
        level as usize
    }

    /// The tallest node this sampler can ever ask for, given `ml`.
    ///
    /// The smallest `u` is `2^-53`, so the level is bounded by
    /// `53 * ln(2) * ml`. Nothing depends on this at runtime; it exists so the
    /// bound on a node's memory is a fact that can be asserted rather than
    /// assumed.
    pub fn max_level(ml: f64) -> usize {
        ((f64::from(MANTISSA_BITS)) * core::f64::consts::LN_2 * ml) as usize
    }
}
