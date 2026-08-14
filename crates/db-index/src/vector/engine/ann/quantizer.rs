//! Turning a vector into a compact code, and ranking from codes.

/// [`distance_table`](Quantizer::distance_table) is computed once per query and
/// [`distance`](Quantizer::distance) is then a handful of lookups per
/// candidate, which is what makes a list scan cheap.
pub trait Quantizer {
    /// Bytes in one code.
    fn code_len(&self) -> usize;

    /// Vectors this quantizer was trained for.
    fn dimensions(&self) -> usize;

    fn encode(&self, vector: &[f32], code: &mut [u8]);

    /// Reconstructs approximately. Lossy by construction.
    fn decode(&self, code: &[u8], out: &mut [f32]);

    fn distance_table(&self, query: &[f32]) -> Vec<f32>;

    fn distance(&self, table: &[f32], code: &[u8]) -> f64;
}
