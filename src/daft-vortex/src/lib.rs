//! Native Vortex scans through Daft's configured positional I/O.
//!
//! The reader supports flat primitive columns, exports Vortex arrays
//! through Arrow, and applies complete Daft predicates before projection and
//! limits. It pushes required columns and a conservative predicate subset into
//! Vortex, while retaining the complete Daft residual and every limit. The
//! stream uses the caller's Tokio runtime and does not collect the entire file.

use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;

mod io;
mod plan;
mod read;

pub use read::{
    read_vortex_metadata, read_vortex_metadata_bulk, read_vortex_schema, stream_vortex,
};

#[cfg(feature = "python")]
mod python;
#[cfg(feature = "python")]
pub use python::register_python;

#[derive(Clone, Debug)]
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
}

impl Default for VortexReadOptions {
    fn default() -> Self {
        Self {
            batch_size: 128 * 1024,
            enable_projection_pushdown: true,
            enable_filter_pushdown: true,
            max_concurrent_requests: 8,
            max_in_flight_bytes: 64 * 1024 * 1024,
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
