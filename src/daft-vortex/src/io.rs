//! Positional Vortex reads through Daft's configured I/O client.
//!
//! Permits bound requests actively fetching/collecting bytes for one opened
//! file. Returned buffers, Vortex caches and decoded batches are not charged.
//! Cancellation releases these async-call permits; an already dispatched
//! blocking filesystem read can finish afterward outside these bounds.

use std::{
    ops::Range,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

use common_error::{DaftError, DaftResult};
use daft_io::{GetRange, GetResult, IOClient, IOStatsRef, ObjectReadMetadata, ObjectVersion};
use futures::{FutureExt, StreamExt, future::BoxFuture};
use tokio::sync::Semaphore;
use vortex::{
    array::buffer::BufferHandle,
    buffer::{Alignment, ByteBuffer, ByteBufferMut},
    error::{VortexError, VortexResult, vortex_err},
    io::{CoalesceConfig, ReadAtRequest, ReadAtStream, VortexReadAt},
};

use crate::{
    VortexReadOptions,
    diagnostics::{self, Phase},
    snapshot::{self, Budget, Capture, OpeningRange, Snapshot},
};

/// Never put URL credentials or signed query parameters into reader diagnostics.
pub(crate) fn display_uri(uri: &str) -> String {
    // Plain filenames, including absolute Windows paths, are not URL syntax.
    if Path::new(uri).is_absolute() {
        return uri.to_string();
    }
    if let Ok(mut url) = url::Url::parse(uri) {
        if url.scheme() == "file" {
            // Preserve Daft's literal percent semantics and the original host
            // spelling (Url normalizes localhost away), but never its query.
            return uri.split(['?', '#']).next().unwrap_or_default().to_string();
        }
        if url.cannot_be_a_base() {
            return format!("{}:<redacted>", url.scheme());
        }
        let _ = url.set_username("");
        let _ = url.set_password(None);
        url.set_query(None);
        url.set_fragment(None);
        return url.to_string();
    }
    if uri.contains("://") {
        // A failed parse cannot reliably identify userinfo/query boundaries.
        return "<invalid URL>".to_string();
    }
    uri.to_string()
}

pub(crate) fn validate_options(options: &VortexReadOptions) -> DaftResult<()> {
    if options.max_concurrent_requests == 0
        || options.max_concurrent_requests > Semaphore::MAX_PERMITS
    {
        return Err(DaftError::ValueError(format!(
            "Vortex max_concurrent_requests must be in 1..={}",
            Semaphore::MAX_PERMITS
        )));
    }
    if options.max_in_flight_bytes == 0
        || options.max_in_flight_bytes > u32::MAX as usize
        || options.max_in_flight_bytes > Semaphore::MAX_PERMITS
    {
        return Err(DaftError::ValueError(format!(
            "Vortex max_in_flight_bytes must be in 1..={}",
            (u32::MAX as usize).min(Semaphore::MAX_PERMITS)
        )));
    }
    if options.read_coalesce_gap_bytes > isize::MAX as usize
        || options.read_coalesce_max_bytes == 0
        || options.read_coalesce_max_bytes > isize::MAX as usize
    {
        return Err(DaftError::ValueError(
            "Vortex read_coalesce_gap_bytes must be in 0..=isize::MAX and read_coalesce_max_bytes in 1..=isize::MAX".into(),
        ));
    }
    if options
        .metadata_snapshot
        .as_ref()
        .is_some_and(|bytes| bytes.len() > snapshot::MAX_SNAPSHOT_BYTES)
    {
        return Err(DaftError::ValueError(
            "Vortex metadata snapshot exceeds its 64 MiB limit".into(),
        ));
    }
    Ok(())
}

fn remote_coalescing(options: &VortexReadOptions, remote: bool) -> Option<CoalesceConfig> {
    if !remote || !options.enable_read_coalescing {
        return None;
    }
    let actual_cap = options
        .read_coalesce_max_bytes
        .min(options.max_in_flight_bytes);
    Some(CoalesceConfig::new(
        options.read_coalesce_gap_bytes as u64,
        actual_cap as u64,
    ))
}

struct ReadGroup {
    requests: Vec<ReadAtRequest>,
    offset: u64,
    length: usize,
    alignment: Alignment,
}

fn group_reads(requests: &[ReadAtRequest], config: Option<CoalesceConfig>) -> Vec<ReadGroup> {
    let mut requests = requests.to_vec();
    if config.is_some() {
        requests.sort_by_key(|request| request.offset);
    }
    let mut groups: Vec<ReadGroup> = Vec::new();
    for request in requests {
        if let Some(config) = config
            && let Some(group) = groups.last_mut()
            && let Some(current_end) = group.offset.checked_add(group.length as u64)
            && let Some(request_end) = request.offset.checked_add(request.length as u64)
            && request.offset <= current_end.saturating_add(config.distance)
            && let Some(length) = request_end.max(current_end).checked_sub(group.offset)
            && length <= config.max_size
            && let Ok(length) = usize::try_from(length)
        {
            group.length = length;
            group.alignment = group.alignment.max(request.alignment);
            group.requests.push(request);
            continue;
        }
        // Existing individual segments retain their original interval, even if
        // larger than the coalescing cap. read_at still enforces its byte budget.
        groups.push(ReadGroup {
            requests: vec![request],
            offset: request.offset,
            length: request.length,
            alignment: request.alignment,
        });
    }
    groups
}

// Keep untrusted nested backend messages out of the Vortex error chain: HTTP
// and SDK errors can contain signed URLs, endpoint credentials or headers.
fn io_error(uri: &str, error: &daft_io::Error) -> VortexError {
    use daft_io::Error;
    let category = match error {
        Error::NotFound { .. } => "object not found",
        Error::ObjectChanged { .. } => "object changed since query planning",
        Error::Unauthorized { .. } => "unauthorized; check I/O credentials",
        Error::ConnectTimeout { .. } => "connection timed out",
        Error::ReadTimeout { .. } => "read timed out",
        Error::SocketError { .. } => "response body/socket error",
        Error::Throttled { .. } => "request throttled",
        Error::MiscTransient { .. } => "transient I/O error after retries",
        Error::UnableToLoadCredentials { .. } | Error::UnableToCreateClient { .. } => {
            "unable to load credentials or initialize I/O client"
        }
        Error::InvalidRangeRequest { .. }
        | Error::InvalidArgument { .. }
        | Error::RangeNotSupported => "invalid byte-range request or response",
        Error::NotAFile { .. } => "path is not a file",
        Error::UnableToDetermineSize { .. } => "unable to determine object size",
        Error::CachedError { source } => return io_error(uri, source),
        _ => "unable to open or read object",
    };
    vortex_err!("Daft I/O for {uri:?}: {category}")
}

trait RangeReader: Send + Sync + 'static {
    fn get(&self, range: Range<usize>) -> BoxFuture<'static, daft_io::Result<GetResult>>;
}

struct ClientReader {
    client: Arc<IOClient>,
    uri: Arc<str>,
    stats: IOStatsRef,
    version: Option<ObjectVersion>,
}

impl RangeReader for ClientReader {
    fn get(&self, range: Range<usize>) -> BoxFuture<'static, daft_io::Result<GetResult>> {
        let client = self.client.clone();
        let uri = self.uri.clone();
        let stats = self.stats.clone();
        let version = self.version.clone();
        async move {
            match version {
                Some(version) => {
                    client
                        .single_url_get_if_match(
                            uri.to_string(),
                            Some(GetRange::Bounded(range)),
                            Some(stats),
                            version,
                        )
                        .await
                }
                None => {
                    client
                        .single_url_get(
                            uri.to_string(),
                            Some(GetRange::Bounded(range)),
                            Some(stats),
                        )
                        .await
                }
            }
        }
        .boxed()
    }
}

pub(crate) struct DaftReadAt {
    backend: Arc<dyn RangeReader>,
    uri: Arc<str>,
    diagnostic_uri: Arc<str>,
    file_size: u64,
    stats: IOStatsRef,
    requests: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    max_requests: usize,
    max_bytes: usize,
    coalescing: Option<CoalesceConfig>,
    diagnostic_phase: AtomicU8,
    version: Option<ObjectVersion>,
    capture: Option<Arc<Capture>>,
    cached_ranges: Arc<[OpeningRange]>,
}

impl DaftReadAt {
    #[cfg(test)]
    pub(crate) async fn new(
        uri: String,
        client: Arc<IOClient>,
        stats: IOStatsRef,
        options: &VortexReadOptions,
    ) -> DaftResult<Self> {
        Self::new_with_phase(uri, client, stats, options, Phase::Data).await
    }

    pub(crate) async fn new_with_phase(
        uri: String,
        client: Arc<IOClient>,
        stats: IOStatsRef,
        options: &VortexReadOptions,
        phase: Phase,
    ) -> DaftResult<Self> {
        let snapshot = if matches!(phase, Phase::ExecutionOpen) {
            options
                .metadata_snapshot
                .as_ref()
                .map(|bytes| Snapshot::decode(bytes, &uri))
                .transpose()?
        } else {
            None
        };
        Self::new_internal(uri, client, stats, options, phase, None, snapshot).await
    }

    pub(crate) async fn new_for_snapshot(
        uri: String,
        client: Arc<IOClient>,
        stats: IOStatsRef,
        options: &VortexReadOptions,
        phase: Phase,
        budget: Arc<Budget>,
    ) -> DaftResult<Self> {
        let budget = snapshot::eligible(&uri).then_some(budget);
        Self::new_internal(uri, client, stats, options, phase, budget, None).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn new_internal(
        uri: String,
        client: Arc<IOClient>,
        stats: IOStatsRef,
        options: &VortexReadOptions,
        phase: Phase,
        capture_budget: Option<Arc<Budget>>,
        snapshot: Option<Snapshot>,
    ) -> DaftResult<Self> {
        validate_options(options)?;
        let redacted = display_uri(&uri);
        let remote = !Path::new(&uri).is_absolute()
            && url::Url::parse(&uri)
                .is_ok_and(|url| matches!(url.scheme(), "http" | "https" | "s3"));
        // Every open checks fresh metadata; saved opening bytes belong only to this task.
        let trace = diagnostics::begin(phase, "head", Some(&uri));
        let metadata = if capture_budget.is_some() || snapshot.is_some() {
            client
                .single_url_get_read_metadata(uri.clone(), Some(stats.clone()))
                .await
        } else {
            client
                .single_url_get_size(uri.clone(), Some(stats.clone()))
                .await
                .map(|size| ObjectReadMetadata {
                    size,
                    version: None,
                })
        };
        diagnostics::complete(trace, metadata.is_err());
        let metadata = metadata
            .map_err(|error| DaftError::ComputeError(io_error(&redacted, &error).to_string()))?;
        if let Some(snapshot) = &snapshot {
            snapshot.validate(&metadata)?;
        }
        let file_size = metadata.size as u64;
        let capture = capture_budget.and_then(|budget| {
            metadata.version.as_ref().and_then(|version| {
                let overhead = uri
                    .len()
                    .saturating_add(version.e_tag.len())
                    .saturating_add(version.version_id.as_ref().map_or(0, String::len))
                    .saturating_add(512);
                Capture::new(budget, overhead).map(Arc::new)
            })
        });
        let cached_ranges = snapshot
            .map(Snapshot::into_ranges)
            .unwrap_or_default()
            .into();
        // The transport and diagnostics share this URI allocation; disabled
        // diagnostics do not allocate an additional URL copy.
        let diagnostic_uri: Arc<str> = Arc::from(uri);
        Ok(Self {
            backend: Arc::new(ClientReader {
                client,
                uri: diagnostic_uri.clone(),
                stats: stats.clone(),
                version: metadata.version.clone(),
            }),
            uri: redacted.into(),
            diagnostic_uri,
            file_size,
            stats,
            requests: Arc::new(Semaphore::new(options.max_concurrent_requests)),
            bytes: Arc::new(Semaphore::new(options.max_in_flight_bytes)),
            max_requests: options.max_concurrent_requests,
            max_bytes: options.max_in_flight_bytes,
            coalescing: remote_coalescing(options, remote),
            diagnostic_phase: AtomicU8::new(phase as u8),
            version: metadata.version,
            capture,
            cached_ranges,
        })
    }

    pub(crate) fn take_snapshot(&self, limit: usize) -> DaftResult<Option<Vec<u8>>> {
        let Some(version) = self.version.clone() else {
            return Ok(None);
        };
        let Some(ranges) = self.capture.as_ref().and_then(|capture| capture.take()) else {
            return Ok(None);
        };
        Snapshot::encode(&self.diagnostic_uri, self.file_size, version, ranges, limit)
    }

    pub(crate) fn mark_data_phase(&self) {
        self.diagnostic_phase
            .store(Phase::Data as u8, Ordering::Relaxed);
    }
}

impl VortexReadAt for DaftReadAt {
    fn uri(&self) -> Option<&Arc<str>> {
        Some(&self.uri)
    }

    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        // The file driver expands even a single read to its global alignment.
        // Keep it off: read_ranges merges actual intervals without that padding.
        None
    }

    fn concurrency(&self) -> usize {
        self.max_requests
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        futures::future::ready(Ok(self.file_size)).boxed()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let backend = self.backend.clone();
        let uri = self.uri.clone();
        let diagnostic_uri = self.diagnostic_uri.clone();
        let stats = self.stats.clone();
        let requests = self.requests.clone();
        let bytes = self.bytes.clone();
        let file_size = self.file_size;
        let max_bytes = self.max_bytes;
        let cached_ranges = self.cached_ranges.clone();
        let capture = self.capture.clone();
        let phase = Phase::from_u8(self.diagnostic_phase.load(Ordering::Relaxed));
        // Includes waiting in read_ranges' buffer_unordered before first poll.
        // Semaphore wait alone cannot see this earlier driver-side queue.
        let queued = diagnostics::begin(phase, "adapter_poll_queue", Some(&diagnostic_uri));
        async move {
            diagnostics::complete(queued, false);
            let cached = cached_ranges.iter().find_map(|range| {
                let start = usize::try_from(offset.checked_sub(range.offset)?).ok()?;
                range.bytes.get(start..start.checked_add(length)?)
            });
            let operation = if cached.is_some() { "cached_range" } else { "range" };
            let mut trace = diagnostics::begin(phase, operation, Some(&diagnostic_uri));
            if let Some(trace) = &mut trace { trace.range(offset, length); }
            let result = async {
            let end = offset
                .checked_add(length as u64)
                .filter(|end| *end <= file_size)
                .ok_or_else(|| vortex_err!("Vortex range {offset}+{length} exceeds size {file_size} for {uri:?}"))?;
            if length > max_bytes {
                return Err(vortex_err!(
                    "Vortex read of {length} bytes exceeds max_in_flight_bytes={max_bytes} for {uri:?}; raise the budget for this segment"
                ));
            }
            let start = usize::try_from(offset)
                .map_err(|_| vortex_err!("Vortex offset does not fit usize for {uri:?}"))?;
            let end = usize::try_from(end)
                .map_err(|_| vortex_err!("Vortex range end does not fit usize for {uri:?}"))?;
            if length == 0 {
                if let Some(trace) = &mut trace { trace.returned(0); }
                return Ok(BufferHandle::new_host(ByteBuffer::empty_aligned(alignment)));
            }
            if let Some(cached) = cached {
                if let Some(trace) = &mut trace { trace.returned(length); }
                return Ok(BufferHandle::new_host(ByteBuffer::from(cached.to_vec()).aligned(alignment)));
            }
            let _request = requests.acquire_owned().await
                .map_err(|_| vortex_err!("Vortex request budget closed"))?;
            if let Some(trace) = &mut trace { trace.request_wait(); }
            let _bytes = bytes.acquire_many_owned(length as u32).await
                .map_err(|_| vortex_err!("Vortex byte budget closed"))?;
            if let Some(trace) = &mut trace { trace.byte_wait(); }
            let response = backend.get(start..end).await;
            if let Some(trace) = &mut trace { trace.headers(); }
            let result = response.map_err(|error| io_error(&uri, &error))?;
            let collected = collect_exact(result, length, alignment, &stats, &uri).await;
            if let Some(trace) = &mut trace { trace.body(); }
            let buffer = collected?;
            if let Some(capture) = &capture { capture.record(offset, buffer.as_slice()); }
            if let Some(trace) = &mut trace { trace.returned(length); }
            Ok(BufferHandle::new_host(buffer))
            }.await;
            diagnostics::complete(trace, result.is_err());
            result
        }
        .boxed()
    }

    fn read_ranges(&self, requests: Arc<[ReadAtRequest]>) -> ReadAtStream {
        let Some(config) = self.coalescing else {
            // Match the pinned Vortex default path for local and disabled reads.
            let reads = requests
                .iter()
                .copied()
                .map(|request| {
                    let read = self.read_at(request.offset, request.length, request.alignment);
                    async move { (request, read.await) }
                })
                .collect::<Vec<_>>();
            return futures::stream::iter(reads)
                .buffer_unordered(self.concurrency().max(1))
                .boxed();
        };
        let reads = group_reads(&requests, Some(config))
            .into_iter()
            .map(|group| {
                let read = self.read_at(group.offset, group.length, group.alignment);
                async move {
                    if group.requests.len() == 1 {
                        return vec![(group.requests[0], read.await)];
                    }
                    match read.await.and_then(|buffer| buffer.try_into_host_sync()) {
                        Ok(buffer) => group
                            .requests
                            .into_iter()
                            .map(|request| {
                                let start = usize::try_from(request.offset - group.offset).unwrap();
                                // Logical starts need not share the merged buffer's alignment.
                                let bytes = buffer
                                    .slice_unaligned(start..start + request.length)
                                    .aligned(request.alignment);
                                (request, Ok(BufferHandle::new_host(bytes)))
                            })
                            .collect::<Vec<_>>(),
                        Err(error) => {
                            let message = error.to_string();
                            group
                                .requests
                                .into_iter()
                                .map(|request| (request, Err(vortex_err!("{message}"))))
                                .collect()
                        }
                    }
                }
            })
            .collect::<Vec<_>>();
        futures::stream::iter(reads)
            .buffer_unordered(self.max_requests)
            .flat_map(futures::stream::iter)
            .boxed()
    }
}

async fn collect_exact(
    result: GetResult,
    length: usize,
    alignment: Alignment,
    stats: &IOStatsRef,
    uri: &str,
) -> VortexResult<ByteBuffer> {
    diagnostics::measure_async(Phase::Io, "vortex_body_collect", Some(uri), async {
    match result {
        GetResult::File(file) => {
            // LocalSource returns a file descriptor rather than a counted
            // stream. Count just this local branch; remote sources count their
            // transport streams, including retries, themselves.
            stats.mark_get_requests(1);
            let bytes = GetResult::File(file)
                .bytes()
                .await
                .map_err(|error| io_error(uri, &error))?;
            stats.mark_bytes_read(bytes.len());
            if bytes.len() != length {
                return Err(vortex_err!(
                    "Vortex short range read for {uri:?}: expected {length} bytes, got {}",
                    bytes.len()
                ));
            }
            Ok(ByteBuffer::from(bytes).aligned(alignment))
        }
        GetResult::Stream(mut stream, hint, _transport_permit, _retry) => {
            // IOClient::single_url_get already calls with_retry: this stream
            // contains ResumableByteStream, including its resumed body reads.
            // Its obsolete retry metadata has been consumed by that wrapper.
            if hint.is_some_and(|size| size != length) {
                return Err(vortex_err!(
                    "Vortex range response length mismatch for {uri:?}: expected {length}, got {hint:?}"
                ));
            }
            let mut buffer = ByteBufferMut::with_capacity_aligned(length, alignment);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|error| io_error(uri, &error))?;
                if chunk.len() > length - buffer.len() {
                    return Err(vortex_err!(
                        "Vortex oversized range response for {uri:?}: expected {length} bytes"
                    ));
                }
                buffer.extend_from_slice(&chunk);
            }
            if buffer.len() != length {
                return Err(vortex_err!(
                    "Vortex short range read for {uri:?}: expected {length} bytes, got {}",
                    buffer.len()
                ));
            }
            Ok(buffer.freeze())
        }
    }

    }).await
}

#[cfg(test)]
#[path = "io_tests.rs"]
mod tests;
