//! WebAssembly fixed-width SIMD (`simd128`) tier.
//!
//! WebAssembly has no runtime feature detection: an engine that cannot run
//! these instructions rejects the module at validation. So the tier is gated on
//! `cfg(target_feature = "simd128")`, set for the whole build by
//! `-C target-feature=+simd128`, and the browser client falls back to the
//! scalar tier when that flag is absent.
//!
//! Baseline `simd128` has no fused multiply-add (that is the separate
//! relaxed-SIMD proposal), so `fmadd` is a multiply then an add.

use super::ops::{simd_kernels, ACCUMULATOR_BITS};
use super::Element;
use core::arch::wasm32::*;

const V128_REGISTER_BITS: usize = 128;

pub(super) const FEATURES: &str = "simd128";
pub(super) const LANES: usize = V128_REGISTER_BITS / ACCUMULATOR_BITS;

/// # Safety
/// Trivially safe; `unsafe` matches the shape the kernel template calls.
#[inline(always)]
unsafe fn zero() -> v128 {
    f64x2_splat(0.0)
}

/// `v128_load` would take 16 bytes, eight past the end when only `LANES`
/// elements remain. `v128_load64_zero` takes exactly the eight that
/// `f64x2_promote_low_f32x4` then converts.
///
/// # Safety
/// `p` must be readable for `LANES` `f32`.
#[inline(always)]
unsafe fn load_widened(p: *const f32) -> v128 {
    f64x2_promote_low_f32x4(v128_load64_zero(p.cast::<u64>()))
}

/// # Safety
/// `p` must be readable for `LANES` `f64`.
#[inline(always)]
unsafe fn load_direct(p: *const f64) -> v128 {
    v128_load(p.cast::<v128>())
}

/// # Safety
/// Trivially safe; `unsafe` matches the shape the kernel template calls.
#[inline(always)]
unsafe fn sub(a: v128, b: v128) -> v128 {
    f64x2_sub(a, b)
}

/// # Safety
/// Trivially safe; `unsafe` matches the shape the kernel template calls.
#[inline(always)]
unsafe fn fmadd(a: v128, b: v128, acc: v128) -> v128 {
    f64x2_add(f64x2_mul(a, b), acc)
}

/// # Safety
/// Trivially safe; `unsafe` matches the shape the kernel template calls.
#[inline(always)]
unsafe fn reduce(acc: v128) -> f64 {
    f64x2_extract_lane::<0>(acc) + f64x2_extract_lane::<1>(acc)
}

simd_kernels!(vector = v128, attrs = {});
