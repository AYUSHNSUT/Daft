//! Native local-file Vortex scans.
//!
//! The initial reader supports flat primitive columns, exports Vortex arrays
//! through Arrow, and applies complete Daft predicates before projection and
//! limits. It does not push predicates or limits into Vortex. The stream uses
//! the caller's Tokio runtime and does not collect the entire file.

use daft_core::prelude::SchemaRef;
use daft_dsl::ExprRef;

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
}

impl Default for VortexReadOptions {
    fn default() -> Self {
        Self {
            batch_size: 128 * 1024,
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
