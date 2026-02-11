//! Variable-length integer encoding (varint/uvarint)

mod decode;
mod encode;

#[cfg(test)]
mod tests;

pub use decode::*;
pub use encode::*;
