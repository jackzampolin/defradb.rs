//! The coarse quantizer IVF-PQ and IVF_FLAT both partition through: centroid
//! training over a byte-bounded sample, the inverted-list key layout, and
//! ranking centroids for probing. Neither is PQ-specific, so a fix here
//! covers both engines instead of two copies that can drift apart.
//!
//! What stays out: the residual pass and codebook training are IVF-PQ's own
//! (`ivfpq::build`), and the trained-state marker is per-engine, because
//! IVF-PQ's carries `m` and IVF_FLAT's does not (see [`TrainedState`]).

mod codec;
mod probe;
mod train;

pub use codec::{
    decode_centroids, decode_state, decode_vector, encode_centroids, encode_state, encode_vector,
    list_key, list_prefix, load_centroids, node_from_list_key, TrainedState, CENTROID, LIST, STATE,
};
pub use probe::probe_lists;
pub use train::{
    fit_centroids, resolved_nlist, resolved_train_threshold, MAX_NLIST, TRAIN_PER_LIST,
};
