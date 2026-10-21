//! Bounded serialized opening ranges. No live readers or decoded arrays survive planning.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use common_error::{DaftError, DaftResult};
use daft_io::{ObjectReadMetadata, ObjectVersion};
use serde::{Deserialize, Serialize};

pub(crate) const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;

pub(crate) fn eligible(uri: &str) -> bool {
    url::Url::parse(uri).is_ok_and(|url| {
        url.scheme() == "s3"
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
    })
}

pub(crate) struct Budget(AtomicUsize);

impl Budget {
    pub fn new(limit: usize) -> Self {
        Self(AtomicUsize::new(limit.min(MAX_SNAPSHOT_BYTES)))
    }

    fn reserve(&self, bytes: usize) -> bool {
        self.0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                left.checked_sub(bytes)
            })
            .is_ok()
    }
}

pub(crate) struct Capture {
    budget: Arc<Budget>,
    ranges: Mutex<Option<Vec<OpeningRange>>>,
}

impl Capture {
    pub fn new(budget: Arc<Budget>, overhead: usize) -> Option<Self> {
        budget.reserve(overhead).then(|| Self {
            budget,
            ranges: Mutex::new(Some(Vec::new())),
        })
    }

    pub fn record(&self, offset: u64, bytes: &[u8]) {
        let mut state = self.ranges.lock().unwrap();
        let Some(ranges) = state.as_mut() else {
            return;
        };
        if !self.budget.reserve(bytes.len().saturating_add(32)) {
            *state = None;
        } else if !bytes.is_empty() {
            ranges.push(OpeningRange {
                offset,
                bytes: bytes.to_vec(),
            });
        }
    }

    pub fn take(&self) -> Option<Vec<OpeningRange>> {
        self.ranges.lock().unwrap().take()
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct OpeningRange {
    pub offset: u64,
    pub bytes: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Snapshot {
    format_version: u8,
    uri: String,
    size: u64,
    version: ObjectVersion,
    ranges: Vec<OpeningRange>,
}

fn invalid() -> DaftError {
    DaftError::ValueError("Invalid or unsupported Vortex metadata snapshot".into())
}

impl Snapshot {
    pub fn encode(
        uri: &str,
        size: u64,
        version: ObjectVersion,
        ranges: Vec<OpeningRange>,
        limit: usize,
    ) -> DaftResult<Option<Vec<u8>>> {
        if ranges.is_empty() {
            return Ok(None);
        }
        let value = Self {
            format_version: 1,
            uri: uri.into(),
            size,
            version,
            ranges,
        };
        let bytes = bincode::serde::encode_to_vec(&value, bincode::config::standard())
            .map_err(|_| invalid())?;
        Ok((bytes.len() <= limit.min(MAX_SNAPSHOT_BYTES)).then_some(bytes))
    }

    pub fn decode(bytes: &[u8], uri: &str) -> DaftResult<Self> {
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err(invalid());
        }
        let (value, consumed): (Self, _) = bincode::serde::decode_from_slice(
            bytes,
            bincode::config::standard().with_limit::<MAX_SNAPSHOT_BYTES>(),
        )
        .map_err(|_| invalid())?;
        if !eligible(uri)
            || consumed != bytes.len()
            || value.format_version != 1
            || value.uri != uri
            || !value.version.is_usable()
            || value.ranges.is_empty()
            || value.ranges.iter().any(|range| {
                range.bytes.is_empty()
                    || range
                        .offset
                        .checked_add(range.bytes.len() as u64)
                        .is_none_or(|end| end > value.size)
            })
        {
            return Err(invalid());
        }
        Ok(value)
    }

    pub fn validate(&self, metadata: &ObjectReadMetadata) -> DaftResult<()> {
        if metadata.size as u64 != self.size || metadata.version.as_ref() != Some(&self.version) {
            return Err(DaftError::ComputeError(
                "Vortex object changed since query planning".into(),
            ));
        }
        Ok(())
    }

    pub fn into_ranges(self) -> Vec<OpeningRange> {
        self.ranges
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version() -> ObjectVersion {
        ObjectVersion {
            e_tag: "\"stable\"".into(),
            version_id: Some("v1".into()),
        }
    }

    fn encoded() -> Vec<u8> {
        Snapshot::encode(
            "s3://bucket/object",
            32,
            version(),
            vec![OpeningRange {
                offset: 7,
                bytes: b"abcd".to_vec(),
            }],
            MAX_SNAPSHOT_BYTES,
        )
        .unwrap()
        .unwrap()
    }

    #[test]
    fn snapshots_bind_uri_size_etag_and_version_without_exporting_bad_values() {
        let bytes = encoded();
        let snapshot = Snapshot::decode(&bytes, "s3://bucket/object").unwrap();
        snapshot
            .validate(&ObjectReadMetadata {
                size: 32,
                version: Some(version()),
            })
            .unwrap();
        for metadata in [
            ObjectReadMetadata {
                size: 31,
                version: Some(version()),
            },
            ObjectReadMetadata {
                size: 32,
                version: None,
            },
            ObjectReadMetadata {
                size: 32,
                version: Some(ObjectVersion {
                    e_tag: "\"changed\"".into(),
                    ..version()
                }),
            },
            ObjectReadMetadata {
                size: 32,
                version: Some(ObjectVersion {
                    version_id: Some("v2".into()),
                    ..version()
                }),
            },
        ] {
            assert!(
                snapshot
                    .validate(&metadata)
                    .unwrap_err()
                    .to_string()
                    .contains("object changed")
            );
        }
        assert!(Snapshot::decode(&bytes, "s3://bucket/other").is_err());
        let mut corrupt = bytes;
        corrupt.push(0);
        assert!(Snapshot::decode(&corrupt, "s3://bucket/object").is_err());
        assert!(Snapshot::decode(&[255], "s3://bucket/object").is_err());
    }

    #[test]
    fn capture_budget_is_shared_and_overflow_bypasses_the_entire_file() {
        let budget = Arc::new(Budget::new(100));
        let first = Capture::new(budget.clone(), 10).unwrap();
        let second = Capture::new(budget, 10).unwrap();
        first.record(7, &[1; 30]); // 62 bytes including serialization allowance.
        second.record(9, &[2; 1]); // Insufficient remaining shared budget.
        assert_eq!(first.take().unwrap()[0].bytes.len(), 30);
        assert!(second.take().is_none());
        let tiny = Capture::new(Arc::new(Budget::new(40)), 0).unwrap();
        tiny.record(0, &[1; 4]);
        tiny.record(4, &[2; 4]);
        assert!(
            tiny.take().is_none(),
            "never publish a partial opening capture"
        );
    }

    #[test]
    fn serialized_snapshots_obey_the_limit_and_reject_out_of_file_ranges() {
        assert!(
            Snapshot::encode(
                "s3://bucket/object",
                32,
                version(),
                vec![OpeningRange {
                    offset: 7,
                    bytes: vec![1]
                }],
                1
            )
            .unwrap()
            .is_none()
        );
        let bytes = Snapshot::encode(
            "s3://bucket/object",
            3,
            version(),
            vec![OpeningRange {
                offset: 2,
                bytes: vec![1, 2],
            }],
            1024,
        )
        .unwrap()
        .unwrap();
        assert!(Snapshot::decode(&bytes, "s3://bucket/object").is_err());
        assert!(!eligible("https://bucket/object"));
        assert!(!eligible("s3://user:secret@bucket/object"));
        assert!(eligible("s3://bucket/object"));
    }
}
