//! x86_64 kernel tiers: AVX-512, AVX2+FMA, and the SSE2 baseline.
//!
//! A tier's feature string is written once at its [`x86_tier!`] invocation and
//! reaches the three places that must agree: the `#[target_feature]` the
//! kernels compile under, the runtime detection that guards them, and the name
//! diagnostics report.
//!
//! SSE2 needs no detection, being part of the x86_64 baseline, which is why it
//! is the floor here rather than a scalar fallback. It has no FMA, so its
//! `fmadd` is a multiply then an add: one extra rounding step, still inside the
//! reduction's error bound.

use super::ops::simd_kernels;

macro_rules! x86_tier {
    (
        $module:ident,
        features = $features:tt,
        detect = [$($detect:tt),* $(,)?],
        register_bits = $register_bits:ident,
        vector = $vector:ty,
        setzero = $setzero:ident,
        loadu_pd = $loadu_pd:ident,
        loadu_ps_half = $loadu_ps_half:ident,
        cvtps_pd = $cvtps_pd:ident,
        sub_pd = $sub_pd:ident,
        storeu_pd = $storeu_pd:ident,
        $(fmadd_pd = $fmadd_pd:ident,)?
        $(mul_pd = $mul_pd:ident, add_pd = $add_pd:ident,)?
        $(extra = { $($extra:item)* },)?
    ) => {
        pub(crate) mod $module {
            use super::super::ops::ACCUMULATOR_BITS;
            use super::super::Element;
            use super::simd_kernels;
            use std::arch::x86_64::*;

            pub(crate) const FEATURES: &str = $features;
            pub(crate) const LANES: usize = super::$register_bits / ACCUMULATOR_BITS;

            /// An empty detect list means the tier is part of the x86_64
            /// baseline and is always present.
            #[inline]
            pub(crate) fn is_available() -> bool {
                true $(&& std::arch::is_x86_feature_detected!($detect))*
            }

            $($($extra)*)?

            /// # Safety
            /// Caller must have checked [`is_available`].
            #[target_feature(enable = $features)]
            unsafe fn zero() -> $vector {
                $setzero()
            }

            /// # Safety
            /// Caller must have checked [`is_available`]; `p` must be readable
            /// for `LANES` elements.
            #[target_feature(enable = $features)]
            unsafe fn load_widened(p: *const f32) -> $vector {
                $cvtps_pd($loadu_ps_half(p))
            }

            /// # Safety
            /// Caller must have checked [`is_available`]; `p` must be readable
            /// for `LANES` elements.
            #[target_feature(enable = $features)]
            unsafe fn load_direct(p: *const f64) -> $vector {
                $loadu_pd(p)
            }

            /// # Safety
            /// Caller must have checked [`is_available`].
            #[target_feature(enable = $features)]
            unsafe fn sub(a: $vector, b: $vector) -> $vector {
                $sub_pd(a, b)
            }

            $(
                /// # Safety
                /// Caller must have checked [`is_available`].
                #[target_feature(enable = $features)]
                unsafe fn fmadd(a: $vector, b: $vector, acc: $vector) -> $vector {
                    $fmadd_pd(a, b, acc)
                }
            )?

            $(
                /// # Safety
                /// Caller must have checked [`is_available`].
                #[target_feature(enable = $features)]
                unsafe fn fmadd(a: $vector, b: $vector, acc: $vector) -> $vector {
                    $add_pd($mul_pd(a, b), acc)
                }
            )?

            /// # Safety
            /// Caller must have checked [`is_available`].
            #[target_feature(enable = $features)]
            unsafe fn reduce(acc: $vector) -> f64 {
                let mut lanes = [0f64; LANES];
                $storeu_pd(lanes.as_mut_ptr(), acc);
                lanes.iter().sum()
            }

            simd_kernels!(
                vector = $vector,
                attrs = { #[target_feature(enable = $features)] }
            );
        }
    };
}

const SSE_REGISTER_BITS: usize = 128;
const AVX_REGISTER_BITS: usize = 256;
const AVX512_REGISTER_BITS: usize = 512;

x86_tier! {
    sse2,
    features = "sse2",
    detect = [],
    register_bits = SSE_REGISTER_BITS,
    vector = __m128d,
    setzero = _mm_setzero_pd,
    loadu_pd = _mm_loadu_pd,
    loadu_ps_half = load_two_f32,
    cvtps_pd = _mm_cvtps_pd,
    sub_pd = _mm_sub_pd,
    storeu_pd = _mm_storeu_pd,
    mul_pd = _mm_mul_pd, add_pd = _mm_add_pd,
    extra = {
        /// Every other tier's half-width `ps` load already matches its lane
        /// count. SSE2 is the one place it does not: `_mm_loadu_ps` reads four
        /// `f32`, eight bytes past the end when only two remain. A 64-bit
        /// `movsd` of the same bytes reads exactly the two lanes
        /// `_mm_cvtps_pd` will convert.
        ///
        /// # Safety
        /// Caller must have checked [`is_available`]; `p` must be readable for
        /// two `f32`.
        #[target_feature(enable = "sse2")]
        unsafe fn load_two_f32(p: *const f32) -> __m128 {
            _mm_castpd_ps(_mm_load_sd(p.cast::<f64>()))
        }
    },
}

x86_tier! {
    avx2,
    features = "avx2,fma",
    detect = ["avx2", "fma"],
    register_bits = AVX_REGISTER_BITS,
    vector = __m256d,
    setzero = _mm256_setzero_pd,
    loadu_pd = _mm256_loadu_pd,
    loadu_ps_half = _mm_loadu_ps,
    cvtps_pd = _mm256_cvtps_pd,
    sub_pd = _mm256_sub_pd,
    storeu_pd = _mm256_storeu_pd,
    fmadd_pd = _mm256_fmadd_pd,
}

// `avx512f` alone covers these kernels: they use only load, sub, fmadd and
// store, all in the foundation set.
x86_tier! {
    avx512,
    features = "avx512f",
    detect = ["avx512f"],
    register_bits = AVX512_REGISTER_BITS,
    vector = __m512d,
    setzero = _mm512_setzero_pd,
    loadu_pd = _mm512_loadu_pd,
    loadu_ps_half = _mm256_loadu_ps,
    cvtps_pd = _mm512_cvtps_pd,
    sub_pd = _mm512_sub_pd,
    storeu_pd = _mm512_storeu_pd,
    fmadd_pd = _mm512_fmadd_pd,
}
