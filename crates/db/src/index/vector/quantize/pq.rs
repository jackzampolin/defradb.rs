//! Product quantization: a vector as `m` bytes.

use crate::index::error::{Error, Result};
use crate::index::vector::core::squared_euclidean;
use crate::index::vector::engine::ann::{Centroids, Clusterer, Quantizer};

/// Codebook entries per subquantizer. `nbits = 8`, so a code component is a
/// `u8` and a list scan is a byte lookup; that is the whole reason the scan is
/// cheap, and why other widths are out of scope.
pub const CODEBOOK_SIZE: usize = 256;

/// Splits a vector into `m` subvectors and quantizes each against its own
/// codebook, so `m` bytes stand in for `dimensions` floats.
///
/// At 768 dimensions and `m = 96` that is 96 bytes against 3,072: **32x**.
#[derive(Debug, Clone, PartialEq)]
pub struct ProductQuantizer {
    dimensions: usize,
    m: usize,
    dsub: usize,
    /// `m` codebooks, each `CODEBOOK_SIZE * dsub` values.
    books: Vec<Centroids>,
}

impl ProductQuantizer {
    /// Trains one codebook per subspace over `sample`, a flat
    /// `n * dimensions` buffer.
    ///
    /// `m` must divide `dimensions`: a ragged split would leave one subspace a
    /// different width and its codebook incomparable with the others.
    pub fn train<C: Clusterer>(
        clusterer: &C,
        sample: &[f32],
        dimensions: usize,
        m: usize,
    ) -> Result<Self> {
        if dimensions == 0 || m == 0 {
            return Err(Error::Other(
                "product quantizer needs a non-zero width and subquantizer count".into(),
            ));
        }
        if !dimensions.is_multiple_of(m) {
            return Err(Error::Other(format!(
                "product quantizer needs m to divide the width: {m} does not divide {dimensions}"
            )));
        }

        let dsub = dimensions / m;
        let n = sample.len() / dimensions;
        let mut books = Vec::with_capacity(m);

        let mut subspace = vec![0.0f32; n * dsub];
        for j in 0..m {
            for i in 0..n {
                let from = i * dimensions + j * dsub;
                subspace[i * dsub..(i + 1) * dsub].copy_from_slice(&sample[from..from + dsub]);
            }
            let (centroids, _) = clusterer.fit(&subspace, dsub, CODEBOOK_SIZE);
            books.push(centroids);
        }

        Ok(Self {
            dimensions,
            m,
            dsub,
            books,
        })
    }

    /// Rebuilds from stored codebooks.
    pub fn from_books(dimensions: usize, books: Vec<Centroids>) -> Result<Self> {
        let m = books.len();
        if m == 0 || !dimensions.is_multiple_of(m) {
            return Err(Error::Other(
                "product quantizer codebooks do not divide the width".into(),
            ));
        }
        let dsub = dimensions / m;
        if books.iter().any(|book| book.dimensions != dsub) {
            return Err(Error::Other(
                "a product quantizer codebook has the wrong subspace width".into(),
            ));
        }
        Ok(Self {
            dimensions,
            m,
            dsub,
            books,
        })
    }

    pub fn books(&self) -> &[Centroids] {
        &self.books
    }

    pub fn m(&self) -> usize {
        self.m
    }

    fn subspace<'v>(&self, vector: &'v [f32], j: usize) -> &'v [f32] {
        &vector[j * self.dsub..(j + 1) * self.dsub]
    }
}

impl Quantizer for ProductQuantizer {
    fn code_len(&self) -> usize {
        self.m
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn encode(&self, vector: &[f32], code: &mut [u8]) {
        for j in 0..self.m {
            let book = &self.books[j];
            if book.k == 0 || vector.len() < (j + 1) * self.dsub || code.len() <= j {
                continue;
            }
            let (nearest, _) = book.nearest(self.subspace(vector, j));
            code[j] = nearest as u8;
        }
    }

    fn decode(&self, code: &[u8], out: &mut [f32]) {
        for j in 0..self.m {
            let book = &self.books[j];
            if book.k == 0 || code.len() <= j || out.len() < (j + 1) * self.dsub {
                continue;
            }
            let index = (code[j] as usize).min(book.k - 1);
            out[j * self.dsub..(j + 1) * self.dsub].copy_from_slice(book.get(index));
        }
    }

    /// `m * CODEBOOK_SIZE` partial squared distances, computed once per query.
    fn distance_table(&self, query: &[f32]) -> Vec<f32> {
        let mut table = vec![0.0f32; self.m * CODEBOOK_SIZE];
        for j in 0..self.m {
            let book = &self.books[j];
            if query.len() < (j + 1) * self.dsub {
                continue;
            }
            let part = self.subspace(query, j);
            for c in 0..CODEBOOK_SIZE {
                table[j * CODEBOOK_SIZE + c] = if c < book.k {
                    squared_euclidean(part, book.get(c)) as f32
                } else {
                    f32::INFINITY
                };
            }
        }
        table
    }

    fn distance(&self, table: &[f32], code: &[u8]) -> f64 {
        let mut total = 0.0f64;
        for (j, part) in code.iter().enumerate().take(self.m) {
            let at = j * CODEBOOK_SIZE + *part as usize;
            if at < table.len() {
                total += table[at] as f64;
            }
        }
        total
    }
}
