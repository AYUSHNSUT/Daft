//! Native Vortex scans through Daft's configured positional I/O.
//!
//! The reader supports flat primitive columns, exports Vortex arrays
//! through Arrow, and applies complete Daft predicates before projection and
//! limits. It pushes required columns and a conservative predicate subset into
//! Vortex, while retaining the complete Daft residual and every limit. The
//! stream uses the caller's Tokio runtime and does not collect the entire file.

use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;

mod diagnostics;
mod io;
mod plan;
mod read;
mod snapshot;

pub use read::{
    read_vortex_metadata, read_vortex_metadata_bulk, read_vortex_metadata_bulk_cached,
    read_vortex_schema, stream_vortex,
};

#[cfg(feature = "python")]
mod python;
#[cfg(feature = "python")]
pub use python::register_python;

#[derive(Clone)]
pub struct VortexReadOptions {
    /// Maximum rows per decoded batch; this is not a byte-memory bound.
    pub batch_size: usize,
    /// Read only output columns and complete residual dependencies.
    pub enable_projection_pushdown: bool,
    /// Evaluate safe conjuncts in Vortex, then reevaluate the complete Daft filter.
    pub enable_filter_pushdown: bool,
    /// Maximum active async positional requests per opened file. Cancellation
    /// releases permits even if a dispatched blocking filesystem read finishes later.
    pub max_concurrent_requests: usize,
    /// Maximum requested bytes in flight per opened file. A larger individual
    /// read fails; returned buffers, caches, decoded output and blocking reads
    /// finishing after cancellation are not charged.
    pub max_in_flight_bytes: usize,
    /// Merge nearby remote segment reads. Local reads retain their existing policy.
    pub enable_read_coalescing: bool,
    /// Maximum gap between remote reads eligible for merging.
    pub read_coalesce_gap_bytes: usize,
    /// Maximum actual merged request size, also capped by the request byte budget.
    pub read_coalesce_max_bytes: usize,
    /// Private query-scoped opening bytes, validated against fresh S3 identity.
    pub metadata_snapshot: Option<std::sync::Arc<[u8]>>,
}

impl std::fmt::Debug for VortexReadOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VortexReadOptions")
            .field("batch_size", &self.batch_size)
            .field(
                "enable_projection_pushdown",
                &self.enable_projection_pushdown,
            )
            .field("enable_filter_pushdown", &self.enable_filter_pushdown)
            .field("max_concurrent_requests", &self.max_concurrent_requests)
            .field("max_in_flight_bytes", &self.max_in_flight_bytes)
            .field("enable_read_coalescing", &self.enable_read_coalescing)
            .field("read_coalesce_gap_bytes", &self.read_coalesce_gap_bytes)
            .field("read_coalesce_max_bytes", &self.read_coalesce_max_bytes)
            .field(
                "metadata_snapshot_bytes",
                &self
                    .metadata_snapshot
                    .as_ref()
                    .map_or(0, |bytes| bytes.len()),
            )
            .finish_non_exhaustive()
    }
}

impl Default for VortexReadOptions {
    fn default() -> Self {
        Self {
            batch_size: 128 * 1024,
            enable_projection_pushdown: true,
            enable_filter_pushdown: true,
            max_concurrent_requests: 8,
            max_in_flight_bytes: 64 * 1024 * 1024,
            enable_read_coalescing: true,
            read_coalesce_gap_bytes: 64 * 1024,
            read_coalesce_max_bytes: 4 * 1024 * 1024,
            metadata_snapshot: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct VortexConvertOptions {
    /// Full first-file schema returned by [`read_vortex_schema`], including its
    /// private source-nullability metadata. Validation precedes projection.
    pub schema: SchemaRef,
    /// Complete Daft residual; filtering precedes projection and limit.
    pub predicate: Option<ExprRef>,
    pub include_columns: Option<Vec<String>>,
    pub limit: Option<usize>,
}

#[cfg(test)]
mod tests;
