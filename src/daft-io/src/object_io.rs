use std::{any::Any, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use common_error::DaftError;
use common_runtime::query_profile::{self as profile, Span};
use futures::{
    StreamExt,
    stream::{BoxStream, Stream},
};
use serde::{Deserialize, Serialize};
use tokio::sync::OwnedSemaphorePermit;

use crate::{
    FileFormat,
    local::{LocalFile, collect_file},
    stats::IOStatsRef,
};

pub struct StreamingRetryParams {
    source: Arc<dyn ObjectSource>,
    input: String,
    range: Option<GetRange>,
    io_stats: Option<IOStatsRef>,
    version: Option<ObjectVersion>,
}

/// A strong S3 response validator, including an optional immutable version ID.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectVersion {
    pub e_tag: String,
    pub version_id: Option<String>,
}

impl ObjectVersion {
    /// Require one quoted strong entity tag, never a wildcard or tag list.
    #[must_use]
    pub fn is_usable(&self) -> bool {
        self.e_tag.len() >= 3
            && self.e_tag.starts_with('"')
            && self.e_tag.ends_with('"')
            && self.e_tag.as_bytes()[1..self.e_tag.len() - 1]
                .iter()
                .all(|byte| byte.is_ascii_graphic() && *byte != b'"')
    }
}

/// Fresh metadata. Sources without conditional reads leave `version` empty.
#[derive(Clone, Debug)]
pub struct ObjectReadMetadata {
    pub size: usize,
    pub version: Option<ObjectVersion>,
}

impl StreamingRetryParams {
    pub(crate) fn new(
        source: Arc<dyn ObjectSource>,
        input: String,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> Self {
        Self {
            source,
            input,
            range,
            io_stats,
            version: None,
        }
    }

    pub(crate) fn with_version(mut self, version: Option<ObjectVersion>) -> Self {
        self.version = version;
        self
    }

    async fn get(&self, range: Option<GetRange>) -> super::Result<GetResult> {
        profile::measure_async(
            profile::Phase::Io,
            "resume_headers",
            Some(&self.input),
            async {
                match &self.version {
                    Some(version) => {
                        self.source
                            .get_if_match(&self.input, range, version, self.io_stats.clone())
                            .await
                    }
                    None => {
                        self.source
                            .get(&self.input, range, self.io_stats.clone())
                            .await
                    }
                }
            },
        )
        .await
    }
}

pub enum GetResult {
    File(LocalFile),
    Stream(
        BoxStream<'static, super::Result<Bytes>>,
        Option<usize>,
        Option<OwnedSemaphorePermit>,
        Option<Box<StreamingRetryParams>>,
    ),
}

async fn collect_bytes<S>(
    mut stream: S,
    size_hint: Option<usize>,
    _permit: Option<OwnedSemaphorePermit>,
) -> super::Result<Bytes>
where
    S: Stream<Item = super::Result<Bytes>> + Send + Unpin,
{
    profile::measure_async(profile::Phase::Io, "body_collect", None, async {
        let first = stream.next().await.transpose()?.unwrap_or_default();
        // Avoid copying if single response
        match stream.next().await.transpose()? {
            None => Ok(first),
            Some(second) => {
                let size_hint = size_hint.unwrap_or_else(|| first.len() + second.len());
                let mut buf = Vec::with_capacity(size_hint);
                buf.extend_from_slice(&first);
                buf.extend_from_slice(&second);

                while let Some(maybe_bytes) = stream.next().await {
                    buf.extend_from_slice(&maybe_bytes?);
                }

                Ok(buf.into())
            }
        }
    })
    .await
}

impl GetResult {
    /// Observe the response lifetime and bytes delivered at the shared I/O boundary.
    /// Elapsed time includes consumer/scheduler delay; poll time is not wire time.
    pub(crate) fn with_profile(self, span: Option<Span>) -> Self {
        match (self, span) {
            (Self::Stream(stream, size, permit, retry), Some(span)) => Self::Stream(
                Box::pin(ProfiledByteStream {
                    stream,
                    span: Some(span),
                    delivered: 0,
                }),
                size,
                permit,
                retry,
            ),
            (result, _) => result,
        }
    }

    pub async fn bytes(self) -> super::Result<Bytes> {
        use GetResult::{File, Stream};
        let mut get_result = self;
        match get_result {
            File(f) => collect_file(f).await,
            Stream(stream, size, permit, retry_params) => {
                use rand::Rng;
                const NUM_TRIES: u64 = 3;
                const JITTER_MS: u64 = 2_500;
                const MAX_BACKOFF_MS: u64 = 20_000;

                let mut result = collect_bytes(stream, size, permit).await; // drop permit to ensure quota
                for attempt in 1..NUM_TRIES {
                    match result {
                        Err(
                            super::Error::SocketError { .. }
                            | super::Error::UnableToReadBytes { .. }
                            | super::Error::UnableToOpenFile { .. }
                            | super::Error::MiscTransient { .. },
                        ) if let Some(rp) = &retry_params => {
                            let jitter =
                                rand::rng().random_range(0..((1 << (attempt - 1)) * JITTER_MS));
                            let jitter = jitter.min(MAX_BACKOFF_MS);

                            log::warn!(
                                "Received Socket Error when streaming bytes! Attempt {attempt} out of {NUM_TRIES} tries. Trying again in {jitter}ms\nDetails\n{}",
                                result.err().unwrap()
                            );
                            let trace =
                                profile::begin(profile::Phase::Io, "body_retry_backoff", None);
                            tokio::time::sleep(Duration::from_millis(jitter)).await;
                            profile::complete(trace, false);

                            get_result = rp.get(rp.range.clone()).await?;
                            if let Stream(stream, size, permit, _) = get_result {
                                result = collect_bytes(stream, size, permit).await;
                            } else {
                                unreachable!("Retrying a stream should always be a stream");
                            }
                        }
                        _ => break,
                    }
                }
                result
            }
        }
    }

    #[must_use]
    pub fn with_retry(self, params: StreamingRetryParams) -> Self {
        match self {
            Self::File(..) => self,
            Self::Stream(s, size, permit, _) => {
                let resumable = ResumableByteStream::new(s, params, permit).into_stream();
                Self::Stream(resumable, size, None, None)
            }
        }
    }
}

struct ProfiledByteStream {
    stream: BoxStream<'static, super::Result<Bytes>>,
    span: Option<Span>,
    delivered: usize,
}

impl Stream for ProfiledByteStream {
    type Item = super::Result<Bytes>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let started = self.span.as_ref().map(|_| std::time::Instant::now());
        let result = self.stream.as_mut().poll_next(cx);
        if let (Some(span), Some(started)) = (&mut self.span, started) {
            span.polled(started.elapsed());
        }
        match &result {
            std::task::Poll::Ready(Some(Ok(bytes))) => {
                self.delivered += bytes.len();
                let delivered = self.delivered;
                if let Some(span) = &mut self.span {
                    span.returned(delivered);
                }
            }
            std::task::Poll::Ready(end) => profile::complete(self.span.take(), end.is_some()),
            std::task::Poll::Pending => {}
        }
        result
    }
}

const JITTER_MS: u64 = 2_500;
const MAX_BACKOFF_MS: u64 = 20_000;
const MAX_NUM_TRIES: u64 = 3;
struct ResumableByteStream {
    inner: BoxStream<'static, super::Result<Bytes>>,
    delivered: usize,
    retry: StreamingRetryParams,
    attempt: u64,
    // A connection belongs to the current response body, not the whole retry
    // operation. Release it before reacquiring one for a resumed request.
    permit: Option<OwnedSemaphorePermit>,
}

impl ResumableByteStream {
    fn new(
        inner: BoxStream<'static, super::Result<Bytes>>,
        retry: StreamingRetryParams,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            inner,
            delivered: 0,
            retry,
            attempt: 0,
            permit,
        }
    }

    fn make_range(&self) -> Option<GetRange> {
        match self.retry.range.clone() {
            Some(GetRange::Bounded(r)) => {
                let start = r.start + self.delivered;
                let end = r.end;
                if start >= end {
                    Some(GetRange::Bounded(end..end))
                } else {
                    Some(GetRange::Bounded(start..end))
                }
            }
            Some(GetRange::Offset(o)) => Some(GetRange::Offset(o + self.delivered)),
            Some(GetRange::Suffix(n)) => {
                let rem = n.saturating_sub(self.delivered);
                Some(GetRange::Suffix(rem))
            }
            None => Some(GetRange::Offset(self.delivered)),
        }
    }
}
impl ResumableByteStream {
    fn into_stream(mut self) -> BoxStream<'static, super::Result<Bytes>> {
        stream! {
            loop {
                match self.inner.next().await {
                    Some(Ok(chunk)) => {
                        self.delivered += chunk.len();
                        yield Ok(chunk);
                    }
                    Some(Err(e)) => {
                        match e {
                            super::Error::SocketError { .. }
                            | super::Error::UnableToReadBytes { .. }
                            | super::Error::UnableToOpenFile { .. }
                            | super::Error::MiscTransient { .. } => {
                                // Drop the failed body and connection before the
                                // backoff; a one-connection pool must be able to
                                // issue the resumed request.
                                self.inner = futures::stream::empty().boxed();
                                self.permit = None;
                                self.attempt += 1;
                                if self.attempt > MAX_NUM_TRIES {
                                    yield Err(e);
                                    break;
                                }

                                let jitter = rand::rng()
                                    .random_range(0..((1 << (self.attempt - 1)) * JITTER_MS));
                                let backoff_ms = jitter.min(MAX_BACKOFF_MS);
                                let trace = profile::begin(profile::Phase::Io, "body_retry_backoff", None);
                                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                profile::complete(trace, false);

                                let range = self.make_range();
                                log::warn!("Received Socket Error when streaming bytes! Attempt {} out of {} tries. Trying again in {}ms with range {:?}",
                                self.attempt, MAX_NUM_TRIES, backoff_ms, range);

                                match self.retry.get(range).await {
                                    Ok(GetResult::Stream(s2, _size2, permit2, _)) => {
                                        // ObjectSource::get counts its own body
                                        // stream. Wrapping it again double-counts
                                        // every byte delivered after a retry.
                                        self.inner = s2;
                                        self.permit = permit2;
                                    }
                                    Ok(_) => {
                                        yield Err(e);
                                        break;
                                    }
                                    Err(err2) => {
                                        yield Err(err2);
                                        break;
                                    }
                                }
                            }
                            _ => {
                                yield Err(e);
                                break;
                            }
                        }
                    }
                    None => break,
                }
            }
        }.boxed()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileType {
    File,
    Directory,
}

impl TryFrom<std::fs::FileType> for FileType {
    type Error = DaftError;

    fn try_from(value: std::fs::FileType) -> Result<Self, Self::Error> {
        if value.is_dir() {
            Ok(Self::Directory)
        } else if value.is_file() {
            Ok(Self::File)
        } else if value.is_symlink() {
            Err(DaftError::InternalError(format!(
                "Symlinks should never be encountered when constructing FileMetadata, but got: {value:?}"
            )))
        } else {
            unreachable!(
                "Can only be a directory, file, or symlink, but got: {:?}",
                value
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMetadata {
    pub filepath: String,
    pub size: Option<u64>,
    pub filetype: FileType,
}
#[derive(Debug)]
pub struct LSResult {
    pub files: Vec<FileMetadata>,
    pub continuation_token: Option<String>,
    /// Whether an empty result across all pages means that the requested path was not found.
    pub not_found_if_empty: bool,
}

use async_stream::stream;
use rand::Rng;

use crate::{multipart::MultipartWriter, range::GetRange};

#[async_trait]
pub trait ObjectSource: Sync + Send {
    /// Check if the source supports range requests.
    /// Most object sources _should_ support range requests.
    /// Many object sources backed by http servers may not support range requests.
    /// So we need to check if the source supports range requests.
    async fn supports_range(&self, uri: &str) -> super::Result<bool>;

    /// Create a multipart writer to upload via multipart upload.
    /// Return None if the source does not support multipart upload.
    async fn create_multipart_writer(
        self: Arc<Self>,
        _uri: &str,
    ) -> super::Result<Option<Box<dyn MultipartWriter>>> {
        Ok(None)
    }

    /// Return the bytes with given range.
    /// Streaming implementations account for bytes and requests in `io_stats`;
    /// callers must not wrap the returned stream in a second statistics counter.
    /// Will return [`Error::InvalidRangeRequest`] if range start is greater than range end
    /// or range start is greater than object size.
    async fn get(
        &self,
        uri: &str,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult>;

    async fn put(
        &self,
        uri: &str,
        data: bytes::Bytes,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<()>;

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<usize>;

    async fn get_read_metadata(
        &self,
        uri: &str,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<ObjectReadMetadata> {
        Ok(ObjectReadMetadata {
            size: self.get_size(uri, io_stats).await?,
            version: None,
        })
    }

    /// Read exactly the recorded object. Unsupported sources must fail closed.
    async fn get_if_match(
        &self,
        _uri: &str,
        _range: Option<GetRange>,
        _version: &ObjectVersion,
        _io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        Err(super::Error::NotImplementedMethod {
            method: "Conditional reads are not supported by this source".to_string(),
        })
    }

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
        io_stats: Option<IOStatsRef>,
        file_format: Option<FileFormat>,
    ) -> super::Result<BoxStream<'static, super::Result<FileMetadata>>>;

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<LSResult>;

    /// Delete the object with the given uri.
    /// Return OK if the object is deleted successfully or the object does not exist.
    async fn delete(&self, _uri: &str, _io_stats: Option<IOStatsRef>) -> super::Result<()> {
        Err(super::Error::NotImplementedMethod {
            method: "Deletes is not yet supported! Please file an issue.".to_string(),
        })
    }

    async fn iter_dir(
        &self,
        uri: &str,
        posix: bool,
        page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<BoxStream<super::Result<FileMetadata>>> {
        let uri = uri.to_string();
        let s = stream! {
            let lsr = self.ls(&uri, posix, None, page_size, io_stats.clone()).await?;
            let mut found_any = !lsr.files.is_empty();
            let not_found_if_empty = lsr.not_found_if_empty;
            for fm in lsr.files {
                yield Ok(fm);
            }

            let mut continuation_token = lsr.continuation_token;
            while let Some(token) = continuation_token {
                let lsr = self
                    .ls(&uri, posix, Some(&token), page_size, io_stats.clone())
                    .await?;
                found_any |= !lsr.files.is_empty();
                continuation_token = lsr.continuation_token;
                for fm in lsr.files {
                    yield Ok(fm);
                }
            }

            if !found_any && not_found_if_empty {
                yield Err(super::Error::NotFound {
                    path: uri.clone(),
                    source: std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "listing returned no files",
                    )
                    .into(),
                });
            }
        };
        Ok(s.boxed())
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}

#[cfg(test)]
mod profile_tests {
    use super::*;

    #[tokio::test]
    async fn response_profiles_keep_bytes_errors_and_cancellation() {
        profile::start().unwrap();
        let make = |operation, stream: BoxStream<'static, crate::Result<Bytes>>| {
            GetResult::Stream(stream, None, None, None).with_profile(profile::begin(
                profile::Phase::Io,
                operation,
                Some("s3://secret:test@bucket/object?credential=hidden"),
            ))
        };
        let bytes = make(
            "body_ok_fixture",
            futures::stream::iter([Ok(Bytes::from_static(b"12")), Ok(Bytes::from_static(b"34"))])
                .boxed(),
        )
        .bytes()
        .await
        .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"1234"));
        let error = crate::Error::InvalidArgument {
            msg: "private backend detail".into(),
        };
        assert!(
            make(
                "body_error_fixture",
                futures::stream::iter([Err(error)]).boxed()
            )
            .bytes()
            .await
            .is_err()
        );
        let pending = make("body_cancel_fixture", futures::stream::pending().boxed());
        let mut collecting = Box::pin(pending.bytes());
        assert!(futures::poll!(collecting.as_mut()).is_pending());
        drop(collecting);
        let snapshot = profile::finish().unwrap();
        let event = |name| {
            snapshot
                .events
                .iter()
                .find(|e| e.operation == name)
                .unwrap()
        };
        let ok = event("body_ok_fixture");
        assert_eq!(ok.returned_bytes, Some(4));
        assert_eq!(ok.poll_count, Some(3));
        assert!(ok.active_poll_ns.unwrap() <= ok.duration_ns);
        assert!(!ok.error && !ok.cancelled);
        assert!(event("body_error_fixture").error);
        assert!(event("body_cancel_fixture").cancelled);
        assert!(event("body_cancel_fixture").poll_count.unwrap() >= 1);
    }
}
