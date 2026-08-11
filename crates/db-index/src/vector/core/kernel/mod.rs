//! Distance kernels, dispatched to the widest SIMD the running machine has.
//!
//! Every kernel is total: any two slices, of any lengths, no panic and no
//! error. Iteration covers the shared prefix, whole lanes first and a scalar
//! tail for the remainder. Dimension agreement is enforced once, where
//! documents enter the index, not on every comparison inside a graph walk.
//!
//! Products accumulate in `f64` whatever the element width, matching the Go
//! implementation's `dot`, which is "accumulated in float64 to avoid float32
//! rounding/underflow". A 1536-dimension `f32` reduction otherwise loses
//! meaningful precision.
//!
//! Tiers are hand-written intrinsics because a plain loop inside a
//! `#[target_feature]` function stays scalar: LLVM will not auto-vectorize a
//! floating-point reduction without fast-math, since float addition is not
//! associative. Disassembling one produced zero `ymm`/`zmm` instructions.
//!
//! [`dot`] returns the true dot product, never negated. "Smaller is closer"
//! belongs in [`super::metric`], where a distance is defined.

/// Compiled only where a SIMD tier exists to use it.
#[cfg(any(
    target_arch = "x86_64",
    all(target_arch = "aarch64", target_feature = "neon"),
    all(target_arch = "wasm32", target_feature = "simd128"),
))]
mod ops;

#[cfg(target_arch = "x86_64")]
mod x86;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod neon;

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
mod wasm;

mod tier;

pub use tier::{Tier, ALL_TIERS};

/// A vector element width the kernels can consume.
///
/// Sealed: a tier's kernels exist per concrete width, so a third implementation
/// would have nothing to dispatch to. `f32` is what embedding models emit and
/// what Go stores; `f64` is what JSON and GraphQL carry.
pub trait Element: Copy + sealed::Sealed {
    /// Widen to the accumulator width.
    fn widen(self) -> f64;

    /// Narrow from the accumulator width.
    fn narrow(value: f64) -> Self;

    /// # Safety
    /// `tier.is_available()` must be true.
    #[doc(hidden)]
    unsafe fn dot_with(tier: Tier, a: &[Self], b: &[Self]) -> f64;

    /// # Safety
    /// `tier.is_available()` must be true.
    #[doc(hidden)]
    unsafe fn squared_euclidean_with(tier: Tier, a: &[Self], b: &[Self]) -> f64;
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for f32 {}
    impl Sealed for f64 {}
}

/// Dot product of two vectors, on the best tier this machine has.
#[inline]
pub fn dot<T: Element>(a: &[T], b: &[T]) -> f64 {
    // SAFETY: `Tier::active` only ever returns an available tier.
    unsafe { T::dot_with(Tier::active(), a, b) }
}

/// Squared euclidean distance, on the best tier this machine has.
///
/// Squared rather than rooted: monotonic in the true distance, so it orders
/// neighbours identically while avoiding a `sqrt` per comparison.
#[inline]
pub fn squared_euclidean<T: Element>(a: &[T], b: &[T]) -> f64 {
    // SAFETY: `Tier::active` only ever returns an available tier.
    unsafe { T::squared_euclidean_with(Tier::active(), a, b) }
}

/// The oracle every other tier is tested against, and the floor on
/// architectures with no SIMD module.
mod scalar {
    use super::Element;

    pub(super) const LANES: usize = 1;
    pub(super) const FEATURES: &str = "scalar";

    #[inline]
    pub(super) fn dot<T: Element>(a: &[T], b: &[T]) -> f64 {
        a.iter().zip(b).map(|(p, q)| p.widen() * q.widen()).sum()
    }

    #[inline]
    pub(super) fn squared_euclidean<T: Element>(a: &[T], b: &[T]) -> f64 {
        a.iter()
            .zip(b)
            .map(|(p, q)| {
                let d = p.widen() - q.widen();
                d * d
            })
            .sum()
    }
}

macro_rules! impl_element {
    ($ty:ty, dot = $dot:ident, squared_euclidean = $sqe:ident) => {
        impl Element for $ty {
            // `f64 as f64` is a no-op the compiler discards; the cast exists
            // because one macro body serves both widths.
            #[allow(clippy::unnecessary_cast)]
            #[inline(always)]
            fn widen(self) -> f64 {
                self as f64
            }

            #[allow(clippy::unnecessary_cast)]
            #[inline(always)]
            fn narrow(value: f64) -> Self {
                value as $ty
            }

            #[inline]
            unsafe fn dot_with(tier: Tier, a: &[Self], b: &[Self]) -> f64 {
                match tier {
                    Tier::Scalar => scalar::dot(a, b),
                    #[cfg(target_arch = "x86_64")]
                    Tier::Sse2 => x86::sse2::$dot(a, b),
                    #[cfg(target_arch = "x86_64")]
                    Tier::Avx2 => x86::avx2::$dot(a, b),
                    #[cfg(target_arch = "x86_64")]
                    Tier::Avx512 => x86::avx512::$dot(a, b),
                    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
                    Tier::Neon => neon::$dot(a, b),
                    #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
                    Tier::Simd128 => wasm::$dot(a, b),
                }
            }

            #[inline]
            unsafe fn squared_euclidean_with(tier: Tier, a: &[Self], b: &[Self]) -> f64 {
                match tier {
                    Tier::Scalar => scalar::squared_euclidean(a, b),
                    #[cfg(target_arch = "x86_64")]
                    Tier::Sse2 => x86::sse2::$sqe(a, b),
                    #[cfg(target_arch = "x86_64")]
                    Tier::Avx2 => x86::avx2::$sqe(a, b),
                    #[cfg(target_arch = "x86_64")]
                    Tier::Avx512 => x86::avx512::$sqe(a, b),
                    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
                    Tier::Neon => neon::$sqe(a, b),
                    #[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
                    Tier::Simd128 => wasm::$sqe(a, b),
                }
            }
        }
    };
}

impl_element!(
    f32,
    dot = dot_f32,
    squared_euclidean = squared_euclidean_f32
);
impl_element!(
    f64,
    dot = dot_f64,
    squared_euclidean = squared_euclidean_f64
);
