//! Vector index engines, quantization and recall gates.

mod edge_selection;
mod ivfflat_engine;
mod ivfflat_exactness;
mod ivfflat_recall_gate;
mod ivfpq_engine;
mod ivfpq_recall_baseline;
mod ivfpq_recall_gate;
mod quantize_kmeans;
mod quantize_pq;
mod quantize_sample;
mod ssg_engine;
mod ssg_vs_hnsw;
mod support;
mod vector_aux_store;
mod vector_collection_index;
mod vector_dimensions;
mod vector_distance_metrics;
mod vector_engine;
mod vector_filtered_search;
mod vector_flat_algorithm;
mod vector_kv_built_state;
mod vector_kv_cost;
mod vector_kv_store;
mod vector_recall_baseline;
mod vector_recall_gate;
mod vector_train_trigger;
