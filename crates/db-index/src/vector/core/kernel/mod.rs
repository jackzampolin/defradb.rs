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

use defra_core::thread_bounds::MaybeSendSync;

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
/// The four widths a vector can arrive in, matching what the reference accepts:
/// `f32` is what embedding models emit and what a node stores, `f64` is what
/// JSON and GraphQL carry, and `i32`/`i64` are what an integer vector field
/// holds.
///
/// Sealed, because a tier's kernels exist per concrete width and an outside
/// implementation would have nothing to dispatch to.
///
/// Shareable because a `&[Self]` is held across the awaits of an index walk.
pub trait Element: Copy + MaybeSendSync + sealed::Sealed {
    /// Whether this width can only hold whole numbers.
    ///
    /// An integral element cannot represent a scaled unit vector: normalizing
    /// `[3, 4]` would truncate to `[0, 0]` and destroy the direction it was
    /// meant to preserve. [`normalize`](crate::vector::core::normalize) checks
    /// this and refuses rather than silently doing that.
    const IS_INTEGRAL: bool;

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
    impl Sealed for i32 {}
    impl Sealed for i64 {}
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
/// neighbors identically while avoiding a `sqrt` per comparison.
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
            const IS_INTEGRAL: bool = false;

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

/// An integer width runs on the scalar tier whatever the machine has.
///
/// The SIMD tiers cover the widths a walk actually compares. An integer vector
/// is an input: the index converts it to its stored width once, at the
/// boundary, so these never appear inside a graph traversal. Widening `i32` is
/// a natural fit for the existing half-width load slot and `i64` is not (no
/// tier below AVX-512's `avx512dq` converts it), so the pair stays together on
/// the scalar path rather than splitting for a gain nothing measures. If an
/// integer hot path ever appears, the tier work is a local addition here.
macro_rules! impl_integral_element {
    ($ty:ty) => {
        impl Element for $ty {
            const IS_INTEGRAL: bool = true;

            #[inline(always)]
            fn widen(self) -> f64 {
                self as f64
            }

            /// Saturating, which is Rust's defined behaviour for this cast: a
            /// value outside the integer's range clamps rather than wrapping
            /// into an unrelated number.
            #[inline(always)]
            fn narrow(value: f64) -> Self {
                value as $ty
            }

            #[inline]
            unsafe fn dot_with(_tier: Tier, a: &[Self], b: &[Self]) -> f64 {
                scalar::dot(a, b)
            }

            #[inline]
            unsafe fn squared_euclidean_with(_tier: Tier, a: &[Self], b: &[Self]) -> f64 {
                scalar::squared_euclidean(a, b)
            }
        }
    };
}

impl_integral_element!(i32);
impl_integral_element!(i64);
