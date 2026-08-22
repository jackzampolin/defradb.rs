//! The kernel body, written once and shared by every SIMD tier.
//!
//! A tier defines `LANES` plus `zero`, `load_widened`, `load_direct`, `sub`,
//! `fmadd` and `reduce` over its own register type, then invokes
//! [`simd_kernels!`]. Neither load may touch more than `LANES` elements: the
//! loop only guarantees that many remain, so a wider load would read past the
//! slice.
//!
//! `attrs` carries the tier's `#[target_feature]` when it has one. Tiers
//! selected at runtime need it; tiers gated on `cfg(target_feature = ...)` are
//! compiled with the feature enabled globally and pass an empty list.

pub(crate) const BITS_PER_BYTE: usize = 8;

/// A tier's lane count is its register width divided by this, never written out
/// by hand.
pub(crate) const ACCUMULATOR_BITS: usize = core::mem::size_of::<f64>() * BITS_PER_BYTE;

macro_rules! simd_kernels {
    (vector = $vector:ty, attrs = { $(#[$attr:meta])* }) => {
        simd_kernels!(@ops
            vector = $vector,
            attrs = { $(#[$attr])* },
            element = f32,
            load = load_widened,
            dot = dot_f32,
            squared_euclidean = squared_euclidean_f32,
        );
        simd_kernels!(@ops
            vector = $vector,
            attrs = { $(#[$attr])* },
            element = f64,
            load = load_direct,
            dot = dot_f64,
            squared_euclidean = squared_euclidean_f64,
        );
    };

    (@ops
        vector = $vector:ty,
        attrs = { $(#[$attr:meta])* },
        element = $ty:ty,
        load = $load:ident,
        dot = $dot:ident,
        squared_euclidean = $sqe:ident,
    ) => {
        /// Dot product over the shared prefix of `a` and `b`.
        ///
        /// # Safety
        /// The running machine must have the features named by `FEATURES`;
        /// check `is_available()` first.
        $(#[$attr])*
        pub(crate) unsafe fn $dot(a: &[$ty], b: &[$ty]) -> f64 {
            let n = a.len().min(b.len());
            let mut acc: $vector = zero();
            let mut i = 0;
            while i + LANES <= n {
                acc = fmadd($load(a.as_ptr().add(i)), $load(b.as_ptr().add(i)), acc);
                i += LANES;
            }
            let mut total = reduce(acc);
            while i < n {
                total += a[i].widen() * b[i].widen();
                i += 1;
            }
            total
        }

        /// Squared euclidean distance over the shared prefix of `a` and `b`.
        ///
        /// # Safety
        /// The running machine must have the features named by `FEATURES`;
        /// check `is_available()` first.
        $(#[$attr])*
        pub(crate) unsafe fn $sqe(a: &[$ty], b: &[$ty]) -> f64 {
            let n = a.len().min(b.len());
            let mut acc: $vector = zero();
            let mut i = 0;
            while i + LANES <= n {
                let d = sub($load(a.as_ptr().add(i)), $load(b.as_ptr().add(i)));
                acc = fmadd(d, d, acc);
                i += LANES;
            }
            let mut total = reduce(acc);
            while i < n {
                let d = a[i].widen() - b[i].widen();
                total += d * d;
                i += 1;
            }
            total
        }
    };
}

pub(crate) use simd_kernels;
