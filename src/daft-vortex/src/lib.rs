//! Native local-file Vortex scans.
//!
//! The reader supports flat primitive columns, exports Vortex arrays
//! through Arrow, and applies complete Daft predicates before projection and
//! limits. It pushes required columns and a conservative predicate subset into
//! Vortex, while retaining the complete Daft residual and every limit. The
//! stream uses the caller's Tokio runtime and does not collect the entire file.

use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;

mod plan;
mod read;

pub use read::{read_vortex_schema, stream_vortex};

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
}

impl Default for VortexReadOptions {
    fn default() -> Self {
        Self {
            batch_size: 128 * 1024,
            enable_projection_pushdown: true,
            enable_filter_pushdown: true,
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
