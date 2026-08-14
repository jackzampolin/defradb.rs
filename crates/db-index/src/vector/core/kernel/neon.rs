//! aarch64 Advanced SIMD (NEON) tier.
//!
//! No runtime detection, unlike x86_64: Advanced SIMD is mandatory in the
//! AArch64 base architecture and every standard Rust aarch64 target enables it,
//! so this module is gated on `cfg(target_feature = "neon")`. The whole build
//! therefore already has NEON, and the kernels need no `#[target_feature]`.

use super::ops::{simd_kernels, ACCUMULATOR_BITS};
use super::Element;
use core::arch::aarch64::*;

const NEON_REGISTER_BITS: usize = 128;

pub(super) const FEATURES: &str = "neon";
pub(super) const LANES: usize = NEON_REGISTER_BITS / ACCUMULATOR_BITS;

/// # Safety
/// Trivially safe; `unsafe` matches the shape the kernel template calls.
#[inline(always)]
unsafe fn zero() -> float64x2_t {
    vdupq_n_f64(0.0)
}

/// `vld1_f32` is the half-width load, so nothing is read past the slice when
/// only `LANES` elements remain.
///
/// # Safety
/// `p` must be readable for `LANES` `f32`.
#[inline(always)]
unsafe fn load_widened(p: *const f32) -> float64x2_t {
    vcvt_f64_f32(vld1_f32(p))
}

/// # Safety
/// `p` must be readable for `LANES` `f64`.
#[inline(always)]
unsafe fn load_direct(p: *const f64) -> float64x2_t {
    vld1q_f64(p)
}

/// # Safety
/// Trivially safe; `unsafe` matches the shape the kernel template calls.
#[inline(always)]
unsafe fn sub(a: float64x2_t, b: float64x2_t) -> float64x2_t {
    vsubq_f64(a, b)
}

/// # Safety
/// Trivially safe; `unsafe` matches the shape the kernel template calls.
#[inline(always)]
unsafe fn fmadd(a: float64x2_t, b: float64x2_t, acc: float64x2_t) -> float64x2_t {
    // vfmaq_f64(addend, x, y) computes addend + x * y.
    vfmaq_f64(acc, a, b)
}

/// # Safety
/// Trivially safe; `unsafe` matches the shape the kernel template calls.
#[inline(always)]
unsafe fn reduce(acc: float64x2_t) -> f64 {
    vaddvq_f64(acc)
}

simd_kernels!(vector = float64x2_t, attrs = {});
