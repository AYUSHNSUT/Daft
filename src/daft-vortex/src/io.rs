//! Positional Vortex reads through Daft's configured I/O client.
//!
//! Permits bound requests actively fetching/collecting bytes for one opened
//! file. Returned buffers, Vortex caches and decoded batches are not charged.
//! Cancellation releases these async-call permits; an already dispatched
//! blocking filesystem read can finish afterward outside these bounds.

use std::{ops::Range, path::Path, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_io::{GetRange, GetResult, IOClient, IOStatsRef};
use futures::{FutureExt, StreamExt, future::BoxFuture};
use tokio::sync::Semaphore;
use vortex::{
    array::buffer::BufferHandle,
    buffer::{Alignment, ByteBuffer, ByteBufferMut},
    error::{VortexError, VortexResult, vortex_err},
    io::VortexReadAt,
};

use crate::VortexReadOptions;

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
    Ok(())
}

// Keep untrusted nested backend messages out of the Vortex error chain: HTTP
// and SDK errors can contain signed URLs, endpoint credentials or headers.
fn io_error(uri: &str, error: &daft_io::Error) -> VortexError {
    use daft_io::Error;
    let category = match error {
        Error::NotFound { .. } => "object not found",
        Error::Unauthorized { .. } => "unauthorized; check I/O credentials",
        Error::ConnectTimeout { .. } => "connection timed out",
        Error::ReadTimeout { .. } => "read timed out",
        Error::SocketError { .. } => "response body/socket error",
        Error::Throttled { .. } => "request throttled",
        Error::MiscTransient { .. } => "transient I/O error after retries",
        Error::UnableToLoadCredentials { .. } | Error::UnableToCreateClient { .. } => {
            "unable to load credentials or initialize I/O client"
        }
        Error::InvalidRangeRequest { .. } | Error::InvalidArgument { .. } => {
            "invalid byte-range request or response"
        }
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
    uri: String,
    stats: IOStatsRef,
}

impl RangeReader for ClientReader {
    fn get(&self, range: Range<usize>) -> BoxFuture<'static, daft_io::Result<GetResult>> {
        let client = self.client.clone();
        let uri = self.uri.clone();
        let stats = self.stats.clone();
        async move {
            client
                .single_url_get(uri, Some(GetRange::Bounded(range)), Some(stats))
                .await
        }
        .boxed()
    }
}

pub(crate) struct DaftReadAt {
    backend: Arc<dyn RangeReader>,
    uri: Arc<str>,
    file_size: u64,
    stats: IOStatsRef,
    requests: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    max_requests: usize,
    max_bytes: usize,
}

impl DaftReadAt {
    pub(crate) async fn new(
        uri: String,
        client: Arc<IOClient>,
        stats: IOStatsRef,
        options: &VortexReadOptions,
    ) -> DaftResult<Self> {
        validate_options(options)?;
        let redacted = display_uri(&uri);
        // Each open gets fresh metadata. There is no cross-file/footer cache.
        let file_size = client
            .single_url_get_size(uri.clone(), Some(stats.clone()))
            .await
            .map_err(|error| DaftError::ComputeError(io_error(&redacted, &error).to_string()))?
            as u64;
        Ok(Self {
            backend: Arc::new(ClientReader {
                client,
                uri,
                stats: stats.clone(),
            }),
            uri: redacted.into(),
            file_size,
            stats,
            requests: Arc::new(Semaphore::new(options.max_concurrent_requests)),
            bytes: Arc::new(Semaphore::new(options.max_in_flight_bytes)),
            max_requests: options.max_concurrent_requests,
            max_bytes: options.max_in_flight_bytes,
        })
    }
}

impl VortexReadAt for DaftReadAt {
    fn uri(&self) -> Option<&Arc<str>> {
        Some(&self.uri)
    }

    // Disable coalescing so unrelated gaps do not spend the configured budget.
    // The default VortexReadAt coalesce_config is None.
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
        let stats = self.stats.clone();
        let requests = self.requests.clone();
        let bytes = self.bytes.clone();
        let file_size = self.file_size;
        let max_bytes = self.max_bytes;
        async move {
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
                return Ok(BufferHandle::new_host(ByteBuffer::empty_aligned(alignment)));
            }
            let _request = requests.acquire_owned().await
                .map_err(|_| vortex_err!("Vortex request budget closed"))?;
            let _bytes = bytes.acquire_many_owned(length as u32).await
                .map_err(|_| vortex_err!("Vortex byte budget closed"))?;
            let result = backend.get(start..end).await.map_err(|error| io_error(&uri, &error))?;
            let buffer = collect_exact(result, length, alignment, &stats, &uri).await?;
            Ok(BufferHandle::new_host(buffer))
        }
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
}

#[cfg(test)]
#[path = "io_tests.rs"]
mod tests;
