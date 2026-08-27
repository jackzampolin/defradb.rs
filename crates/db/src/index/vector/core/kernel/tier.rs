//! Tier selection: which kernel implementation runs, and how it is chosen.
//!
//! Membership in [`ALL_TIERS`] is a build-time property; [`Tier::is_available`]
//! is the runtime one. On x86_64 every tier is compiled in and detection picks
//! between them, which is what lets one binary use AVX-512 on a server and SSE2
//! on an old laptop. NEON and `simd128` are settled at compile time, because
//! neither architecture has runtime detection to make.

use core::sync::atomic::{AtomicU8, Ordering};

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use super::neon;
#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
use super::wasm;
#[cfg(target_arch = "x86_64")]
use super::x86;
use super::{scalar, Element};

/// One kernel implementation, identified by the instruction set it needs.
///
/// Public so that tests, benchmarks and diagnostics can drive a specific tier
/// instead of only the one this machine happens to pick.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Tier {
    Scalar,
    #[cfg(target_arch = "x86_64")]
    Sse2,
    #[cfg(target_arch = "x86_64")]
    Avx2,
    #[cfg(target_arch = "x86_64")]
    Avx512,
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    Neon,
    #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
    Simd128,
}

/// Every tier compiled into this build, narrowest first.
#[cfg(target_arch = "x86_64")]
pub const ALL_TIERS: &[Tier] = &[Tier::Scalar, Tier::Sse2, Tier::Avx2, Tier::Avx512];

/// Every tier compiled into this build, narrowest first.
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub const ALL_TIERS: &[Tier] = &[Tier::Scalar, Tier::Neon];

/// Every tier compiled into this build, narrowest first.
#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
pub const ALL_TIERS: &[Tier] = &[Tier::Scalar, Tier::Simd128];

/// Every tier compiled into this build, narrowest first.
#[cfg(not(any(
    target_arch = "x86_64",
    all(target_arch = "aarch64", target_feature = "neon"),
    all(target_arch = "wasm32", target_feature = "simd128"),
)))]
pub const ALL_TIERS: &[Tier] = &[Tier::Scalar];

impl Tier {
    /// The instruction set this tier needs, taken from the tier module itself
    /// so it cannot drift from what the kernels compile against.
    pub fn name(self) -> &'static str {
        match self {
            Tier::Scalar => scalar::FEATURES,
            #[cfg(target_arch = "x86_64")]
            Tier::Sse2 => x86::sse2::FEATURES,
            #[cfg(target_arch = "x86_64")]
            Tier::Avx2 => x86::avx2::FEATURES,
            #[cfg(target_arch = "x86_64")]
            Tier::Avx512 => x86::avx512::FEATURES,
            #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
            Tier::Neon => neon::FEATURES,
            #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
            Tier::Simd128 => wasm::FEATURES,
        }
    }

    /// Accumulator lanes consumed per step.
    pub fn lanes(self) -> usize {
        match self {
            Tier::Scalar => scalar::LANES,
            #[cfg(target_arch = "x86_64")]
            Tier::Sse2 => x86::sse2::LANES,
            #[cfg(target_arch = "x86_64")]
            Tier::Avx2 => x86::avx2::LANES,
            #[cfg(target_arch = "x86_64")]
            Tier::Avx512 => x86::avx512::LANES,
            #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
            Tier::Neon => neon::LANES,
            #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
            Tier::Simd128 => wasm::LANES,
        }
    }

    /// Whether the running machine can execute this tier.
    ///
    /// Only the x86_64 tiers can answer `false`: the NEON and `simd128` modules
    /// are gated on the target feature being enabled for the whole build, so if
    /// one compiled it is present.
    pub fn is_available(self) -> bool {
        match self {
            Tier::Scalar => true,
            #[cfg(target_arch = "x86_64")]
            Tier::Sse2 => x86::sse2::is_available(),
            #[cfg(target_arch = "x86_64")]
            Tier::Avx2 => x86::avx2::is_available(),
            #[cfg(target_arch = "x86_64")]
            Tier::Avx512 => x86::avx512::is_available(),
            #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
            Tier::Neon => true,
            #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
            Tier::Simd128 => true,
        }
    }

    /// Dot product on this specific tier.
    ///
    /// `None` when the running machine cannot execute the tier. A tier is never
    /// silently swapped for another: a caller asking for AVX-512 where there is
    /// none has a wrong assumption, not a slow path.
    #[inline]
    pub fn dot<T: Element>(self, a: &[T], b: &[T]) -> Option<f64> {
        if !self.is_available() {
            return None;
        }
        // SAFETY: availability checked immediately above.
        Some(unsafe { T::dot_with(self, a, b) })
    }

    /// Squared euclidean distance on this specific tier.
    ///
    /// `None` when the running machine cannot execute the tier; see
    /// [`Tier::dot`].
    #[inline]
    pub fn squared_euclidean<T: Element>(self, a: &[T], b: &[T]) -> Option<f64> {
        if !self.is_available() {
            return None;
        }
        // SAFETY: availability checked immediately above.
        Some(unsafe { T::squared_euclidean_with(self, a, b) })
    }

    /// The widest tier this machine can run. Detection happens once and is
    /// cached; racing threads compute the same answer, so a relaxed store is
    /// sufficient.
    pub fn active() -> Tier {
        const UNSET: u8 = u8::MAX;
        static ACTIVE: AtomicU8 = AtomicU8::new(UNSET);

        let mut raw = ACTIVE.load(Ordering::Relaxed);
        if raw == UNSET {
            // Narrowest first, so the last one that fits wins.
            let mut best = 0;
            for (index, tier) in ALL_TIERS.iter().enumerate() {
                if tier.is_available() {
                    best = index;
                }
            }
            raw = best as u8;
            ACTIVE.store(raw, Ordering::Relaxed);
        }
        ALL_TIERS[raw as usize]
    }
}
