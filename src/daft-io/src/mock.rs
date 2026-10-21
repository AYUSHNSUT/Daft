#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use async_stream::stream;
    use async_trait::async_trait;
    use bytes::Bytes;
    use common_file_formats::FileFormat;
    use futures::{StreamExt, TryStreamExt, stream::BoxStream};

    use crate::{
        Error, FileMetadata, FileType, GetRange, GetResult, IOStatsRef, ObjectSource,
        ObjectVersion, Result,
        object_io::{LSResult, StreamingRetryParams},
    };

    /// Which error variant the mock should emit on failure.
    #[derive(Clone)]
    enum MockFailError {
        UnableToReadBytes,
        MiscTransient,
    }

    /// The mocked source used for testing read operations.
    struct MockSource {
        // the object content data
        content: Arc<Vec<u8>>,
        // the object data will be chunked by this value
        chunk_size: usize,
        // will raise an error when accumulated read chunk number of a single request reaches this value
        fail_at_chunk: Option<usize>,
        // which error to emit on stream failure
        fail_error: MockFailError,
        // number of get() calls that should fail before succeeding (simulates dispatch failures)
        fail_get_remaining: Mutex<usize>,
        // the total received get requests
        requests: Arc<Mutex<Vec<Option<GetRange>>>>,
        // Predetermined listing pages returned by ls().
        ls_pages: Mutex<VecDeque<LSResult>>,
        // The continuation token supplied to each ls() request.
        ls_requests: Arc<Mutex<Vec<Option<String>>>>,
        connection_pool: Option<Arc<tokio::sync::Semaphore>>,
        versions: Mutex<Vec<ObjectVersion>>,
        fail_conditional_after: Option<usize>,
    }

    impl MockSource {
        fn new(data: Vec<u8>, chunk_size: usize, fail_at_chunk: Option<usize>) -> Self {
            Self {
                content: Arc::new(data),
                chunk_size,
                fail_at_chunk,
                fail_error: MockFailError::UnableToReadBytes,
                fail_get_remaining: Mutex::new(0),
                requests: Arc::new(Mutex::new(Vec::new())),
                ls_pages: Mutex::new(VecDeque::new()),
                ls_requests: Arc::new(Mutex::new(Vec::new())),
                connection_pool: None,
                versions: Mutex::new(Vec::new()),
                fail_conditional_after: None,
            }
        }

        fn with_ls_pages(self, pages: Vec<LSResult>) -> Self {
            *self.ls_pages.lock().unwrap() = pages.into();
            self
        }

        fn with_fail_error(mut self, err: MockFailError) -> Self {
            self.fail_error = err;
            self
        }

        fn with_fail_get(self, count: usize) -> Self {
            *self.fail_get_remaining.lock().unwrap() = count;
            self
        }

        fn with_connection_pool(mut self, pool: Arc<tokio::sync::Semaphore>) -> Self {
            self.connection_pool = Some(pool);
            self
        }

        fn slice_for_range(&self, range: Option<GetRange>) -> std::ops::Range<usize> {
            let len = self.content.len();
            match range {
                Some(GetRange::Bounded(r)) => {
                    let end = r.end.min(len);
                    r.start.min(end)..end
                }
                Some(GetRange::Offset(o)) => o.min(len)..len,
                Some(GetRange::Suffix(n)) => len.saturating_sub(n)..len,
                None => 0..len,
            }
        }

        fn read_data(
            &self,
            range: Option<GetRange>,
            path: &str,
        ) -> BoxStream<'static, Result<Bytes>> {
            let slice = self.slice_for_range(range);
            let data = self.content[slice].to_vec();
            let chunk = self.chunk_size;
            let fail_idx = self.fail_at_chunk;
            let fail_error = self.fail_error.clone();
            let path_owned = path.to_string();

            stream! {
                for (sent_chunks, chunk_data) in data.chunks(chunk).enumerate() {
                    if let Some(fail) = fail_idx
                        && sent_chunks == fail {
                            let err = match fail_error {
                                MockFailError::UnableToReadBytes => Error::UnableToReadBytes {
                                    path: path_owned.clone(),
                                    source: std::io::Error::new(std::io::ErrorKind::Interrupted, "mock fail"),
                                },
                                MockFailError::MiscTransient => Error::MiscTransient {
                                    path: path_owned.clone(),
                                    source: Box::new(std::io::Error::other("mock transient")),
                                },
                            };
                            yield Err(err);
                            break;
                        }
                    yield Ok(Bytes::copy_from_slice(chunk_data));
                }
            }.boxed()
        }
    }

    #[async_trait]
    impl ObjectSource for MockSource {
        async fn supports_range(&self, _uri: &str) -> Result<bool> {
            Ok(true)
        }

        async fn get(
            &self,
            uri: &str,
            range: Option<GetRange>,
            io_stats: Option<IOStatsRef>,
        ) -> Result<GetResult> {
            {
                let mut remaining = self.fail_get_remaining.lock().unwrap();
                if *remaining > 0 {
                    *remaining -= 1;
                    return Err(Error::MiscTransient {
                        path: uri.to_string(),
                        source: Box::new(std::io::Error::other("mock dispatch failure")),
                    });
                }
            }
            let permit = if let Some(pool) = &self.connection_pool {
                Some(pool.clone().acquire_owned().await.unwrap())
            } else {
                None
            };
            self.requests.lock().unwrap().push(range.clone());
            if let Some(stats) = &io_stats {
                stats.mark_get_requests(1);
            }
            let length = self.slice_for_range(range.clone()).len();
            let s = self.read_data(range, uri);
            Ok(GetResult::Stream(
                crate::stream_utils::io_stats_on_bytestream(s, io_stats),
                Some(length),
                permit,
                None,
            ))
        }

        async fn get_if_match(
            &self,
            uri: &str,
            range: Option<GetRange>,
            version: &ObjectVersion,
            io_stats: Option<IOStatsRef>,
        ) -> Result<GetResult> {
            {
                let mut versions = self.versions.lock().unwrap();
                versions.push(version.clone());
                if self
                    .fail_conditional_after
                    .is_some_and(|limit| versions.len() > limit)
                {
                    return Err(Error::ObjectChanged { path: uri.into() });
                }
            }
            self.get(uri, range, io_stats).await
        }

        async fn put(&self, _uri: &str, _data: Bytes, _io_stats: Option<IOStatsRef>) -> Result<()> {
            Err(Error::NotImplementedSource {
                store: "mock".to_string(),
            })
        }

        async fn get_size(&self, _uri: &str, _io_stats: Option<IOStatsRef>) -> Result<usize> {
            Ok(self.content.len())
        }

        async fn glob(
            self: Arc<Self>,
            _glob_path: &str,
            _fanout_limit: Option<usize>,
            _page_size: Option<i32>,
            _limit: Option<usize>,
            _io_stats: Option<IOStatsRef>,
            _file_format: Option<FileFormat>,
        ) -> Result<BoxStream<'static, Result<FileMetadata>>> {
            Err(Error::NotImplementedSource {
                store: "mock".to_string(),
            })
        }

        async fn ls(
            &self,
            _path: &str,
            _posix: bool,
            continuation_token: Option<&str>,
            _page_size: Option<i32>,
            _io_stats: Option<IOStatsRef>,
        ) -> Result<LSResult> {
            self.ls_requests
                .lock()
                .unwrap()
                .push(continuation_token.map(str::to_owned));

            let page = self.ls_pages.lock().unwrap().pop_front();
            page.ok_or_else(|| Error::NotImplementedSource {
                store: "mock ls page was not configured".to_string(),
            })
        }

        fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
            self
        }
    }

    fn mock_ls_result(
        files: Vec<FileMetadata>,
        continuation_token: Option<&str>,
        not_found_if_empty: bool,
    ) -> LSResult {
        LSResult {
            files,
            continuation_token: continuation_token.map(str::to_owned),
            not_found_if_empty,
        }
    }

    fn mock_file(path: &str) -> FileMetadata {
        FileMetadata {
            filepath: path.to_string(),
            size: Some(10),
            filetype: FileType::File,
        }
    }

    async fn collect_iter_dir(source: &MockSource) -> Result<Vec<FileMetadata>> {
        let stream = source
            .iter_dir("mock://bucket/directory", true, None, None)
            .await?;
        stream.try_collect().await
    }

    #[tokio::test]
    async fn test_iter_dir_follows_token_after_empty_page() {
        let expected_file = mock_file("mock://bucket/directory/file.parquet");
        let source = MockSource::new(Vec::new(), 1, None).with_ls_pages(vec![
            mock_ls_result(Vec::new(), Some("page-2"), true),
            mock_ls_result(vec![expected_file.clone()], None, true),
        ]);

        let files = collect_iter_dir(&source).await.unwrap();

        assert_eq!(files, vec![expected_file]);
        assert_eq!(
            *source.ls_requests.lock().unwrap(),
            vec![None, Some("page-2".to_string())]
        );
    }

    #[tokio::test]
    async fn test_iter_dir_remembers_files_from_earlier_pages() {
        let expected_file = mock_file("mock://bucket/directory/file.parquet");
        let source = MockSource::new(Vec::new(), 1, None).with_ls_pages(vec![
            mock_ls_result(vec![expected_file.clone()], Some("page-2"), true),
            mock_ls_result(Vec::new(), None, true),
        ]);

        let files = collect_iter_dir(&source).await.unwrap();

        assert_eq!(files, vec![expected_file]);
    }

    #[tokio::test]
    async fn test_iter_dir_returns_not_found_after_all_pages_are_empty() {
        let source = MockSource::new(Vec::new(), 1, None).with_ls_pages(vec![
            mock_ls_result(Vec::new(), Some("page-2"), true),
            mock_ls_result(Vec::new(), None, true),
        ]);

        let result = collect_iter_dir(&source).await;

        assert!(matches!(result, Err(Error::NotFound { .. })));
        assert_eq!(
            *source.ls_requests.lock().unwrap(),
            vec![None, Some("page-2".to_string())]
        );
    }

    #[tokio::test]
    async fn test_iter_dir_allows_empty_result_when_source_policy_allows_it() {
        let source = MockSource::new(Vec::new(), 1, None).with_ls_pages(vec![
            mock_ls_result(Vec::new(), Some("page-2"), false),
            mock_ls_result(Vec::new(), None, false),
        ]);

        let files = collect_iter_dir(&source).await.unwrap();

        assert!(files.is_empty());
        assert_eq!(
            *source.ls_requests.lock().unwrap(),
            vec![None, Some("page-2".to_string())]
        );
    }

    #[test]
    fn test_conditional_read_rejects_weak_wildcard_and_multiple_validators() {
        for tag in ["*", "W/\"tag\"", "\"\"", "tag", "\"a\",\"b\"", "\"a\r\nb\""] {
            assert!(
                !ObjectVersion {
                    e_tag: tag.into(),
                    version_id: None
                }
                .is_usable()
            );
        }
        assert!(
            ObjectVersion {
                e_tag: "\"opaque-2\"".into(),
                version_id: Some("version-1".into())
            }
            .is_usable()
        );
    }

    #[tokio::test]
    async fn test_conditional_resumed_body_keeps_etag_and_version_and_never_falls_back() {
        for changed in [false, true] {
            let mut source = MockSource::new(b"abcd".to_vec(), 2, Some(1));
            source.fail_conditional_after = changed.then_some(1);
            let source = Arc::new(source);
            let version = ObjectVersion {
                e_tag: "\"stable\"".into(),
                version_id: Some("v1".into()),
            };
            let range = Some(GetRange::Bounded(0..4));
            let initial = source
                .get_if_match("mock://path", range.clone(), &version, None)
                .await
                .unwrap();
            let result = initial
                .with_retry(
                    StreamingRetryParams::new(source.clone(), "mock://path".into(), range, None)
                        .with_version(Some(version.clone())),
                )
                .bytes()
                .await;
            if changed {
                assert!(matches!(result, Err(Error::ObjectChanged { .. })));
                assert_eq!(source.requests.lock().unwrap().len(), 1);
            } else {
                assert_eq!(result.unwrap().as_ref(), b"abcd");
                assert_eq!(
                    source.requests.lock().unwrap().as_slice(),
                    &[Some(GetRange::Bounded(0..4)), Some(GetRange::Bounded(2..4))]
                );
            }
            assert_eq!(
                source.versions.lock().unwrap().as_slice(),
                &[version.clone(), version]
            );
        }
    }

    #[tokio::test]
    async fn test_resumable_bounded() {
        let data: Vec<u8> = (0..64_000u32).flat_map(|v| v.to_le_bytes()).collect();
        let data_len = data.len();
        let src = Arc::new(MockSource::new(data.clone(), 4096, Some(20)));
        let range = Some(GetRange::Bounded(0..data_len));
        let initial = src.get("mock://path", range.clone(), None).await.unwrap();
        let wrapped = initial.with_retry(StreamingRetryParams::new(
            src.clone(),
            "mock://path".to_string(),
            range.clone(),
            None,
        ));
        let out = wrapped.bytes().await.unwrap();
        assert_eq!(out.len(), data.len());
        assert_eq!(&out[..], &data[..]);
        let requests = src.requests.lock().unwrap().clone();
        assert_eq!(
            requests,
            vec![
                Some(GetRange::Bounded(0..data.len())),
                Some(GetRange::Bounded(81920..data.len())),
                Some(GetRange::Bounded(163840..data.len())),
                Some(GetRange::Bounded(245760..data_len))
            ]
        );
    }

    #[tokio::test]
    async fn test_resumable_counts_retry_bytes_once_and_releases_single_connection() {
        let data = b"12345678".to_vec();
        let pool = Arc::new(tokio::sync::Semaphore::new(1));
        let src =
            Arc::new(MockSource::new(data.clone(), 4, Some(1)).with_connection_pool(pool.clone()));
        let stats = crate::IOStatsContext::new("retry count regression");
        let range = Some(GetRange::Bounded(0..8));
        let initial = src
            .get("mock://path", range.clone(), Some(stats.clone()))
            .await
            .unwrap();
        assert_eq!(pool.available_permits(), 0);
        let wrapped = initial.with_retry(StreamingRetryParams::new(
            src.clone(),
            "mock://path".into(),
            range,
            Some(stats.clone()),
        ));
        let output = tokio::time::timeout(std::time::Duration::from_secs(10), wrapped.bytes())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(output.as_ref(), data.as_slice());
        assert_eq!(stats.load_get_requests(), 2);
        assert_eq!(stats.load_bytes_read(), 8);
        assert_eq!(pool.available_permits(), 1);
        assert_eq!(
            *src.requests.lock().unwrap(),
            vec![Some(GetRange::Bounded(0..8)), Some(GetRange::Bounded(4..8))]
        );
    }

    #[tokio::test]
    async fn test_resumable_retains_replacement_connection_until_consumer_drop() {
        let pool = Arc::new(tokio::sync::Semaphore::new(1));
        let src = Arc::new(
            MockSource::new(b"123456789abc".to_vec(), 4, Some(1))
                .with_connection_pool(pool.clone()),
        );
        let stats = crate::IOStatsContext::new("retry cancel regression");
        let range = Some(GetRange::Bounded(0..12));
        let initial = src
            .get("mock://path", range.clone(), Some(stats.clone()))
            .await
            .unwrap();
        let wrapped = initial.with_retry(StreamingRetryParams::new(
            src,
            "mock://path".into(),
            range,
            Some(stats.clone()),
        ));
        let GetResult::Stream(mut stream, _, _, _) = wrapped else {
            panic!("expected stream")
        };
        assert_eq!(stream.next().await.unwrap().unwrap().as_ref(), b"1234");
        let next = tokio::time::timeout(std::time::Duration::from_secs(10), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(next.as_ref(), b"5678");
        assert_eq!(pool.available_permits(), 0);
        drop(stream);
        assert_eq!(pool.available_permits(), 1);
        assert_eq!(stats.load_get_requests(), 2);
        assert_eq!(stats.load_bytes_read(), 8);
    }

    #[tokio::test]
    async fn test_resumable_offset() {
        let data: Vec<u8> = (0..64_000u32).flat_map(|v| v.to_le_bytes()).collect();
        let chunk_size = 8192;
        let fail_at_chunk = 10;
        let init_off = 10_000;

        let src = Arc::new(MockSource::new(
            data.clone(),
            chunk_size,
            Some(fail_at_chunk),
        ));
        let range = Some(GetRange::Offset(init_off));
        let initial = src.get("mock://path", range.clone(), None).await.unwrap();
        let wrapped = initial.with_retry(StreamingRetryParams::new(
            src.clone(),
            "mock://path".to_string(),
            range.clone(),
            None,
        ));
        let out = wrapped.bytes().await.unwrap();
        assert_eq!(&out[..], &data[init_off..]);

        let requests = src.requests.lock().unwrap().clone();
        assert_eq!(
            requests,
            vec![
                Some(GetRange::Offset(init_off)),
                Some(GetRange::Offset(chunk_size * fail_at_chunk + init_off)),
                Some(GetRange::Offset(chunk_size * fail_at_chunk * 2 + init_off)),
                Some(GetRange::Offset(chunk_size * fail_at_chunk * 3 + init_off)),
            ]
        );
    }

    #[tokio::test]
    async fn test_resumable_suffix() {
        let data: Vec<u8> = (0..32_000u32).flat_map(|v| v.to_le_bytes()).collect();
        let n = 10_000usize.min(data.len());
        let chunk_size = 2048;
        let fail_at_chunk = 2;

        let src = Arc::new(MockSource::new(
            data.clone(),
            chunk_size,
            Some(fail_at_chunk),
        ));
        let range = Some(GetRange::Suffix(n));
        let initial = src.get("mock://path", range.clone(), None).await.unwrap();
        let wrapped = initial.with_retry(StreamingRetryParams::new(
            src.clone(),
            "mock://path".to_string(),
            range.clone(),
            None,
        ));
        let out = wrapped.bytes().await.unwrap();
        assert_eq!(&out[..], &data[data.len().saturating_sub(n)..]);
        let requests = src.requests.lock().unwrap().clone();
        assert_eq!(
            requests,
            vec![
                Some(GetRange::Suffix(n)),
                Some(GetRange::Suffix(n - chunk_size * fail_at_chunk)),
                Some(GetRange::Suffix(n - chunk_size * fail_at_chunk * 2)),
            ]
        );
    }

    #[tokio::test]
    async fn test_non_retry_error_bubbles() {
        let data: Vec<u8> = (0..8_000u32).flat_map(|v| v.to_le_bytes()).collect();
        // fail at chunk 0 so first read errors; but simulate non-retry by producing NotImplementedSource (mapped as Generic)
        let src = Arc::new(MockSource::new(data.clone(), 1024, Some(0)));
        // Override stream to emit non-retry error by creating an initial GetResult::Stream with an immediate error
        let s = stream! { yield Err(Error::NotImplementedSource { store: "mock".to_string() }); }
            .boxed();
        let initial = GetResult::Stream(s, Some(data.len()), None, None);
        let wrapped = initial.with_retry(StreamingRetryParams::new(
            src.clone(),
            "mock://path".to_string(),
            None,
            None,
        ));
        let err = wrapped.bytes().await.err().unwrap();
        match err {
            Error::NotImplementedSource { .. } => {}
            _ => panic!("expected NotImplementedSource error"),
        }
    }

    #[tokio::test]
    async fn test_max_retries_exceeded() {
        // With default max retries = 3, total get calls should be 1 (initial) + 3 (retries) = 4
        let data: Vec<u8> = (0..2_000u32).flat_map(|v| v.to_le_bytes()).collect();
        let _data_size = data.len();
        let chunk_size = 1024;
        let src = Arc::new(MockSource::new(data.clone(), chunk_size, Some(0)));
        let initial = src.get("mock://path", None, None).await.unwrap();
        let wrapped = initial.with_retry(StreamingRetryParams::new(
            src.clone(),
            "mock://path".to_string(),
            None,
            None,
        ));

        let err = wrapped.bytes().await.err().unwrap();
        match err {
            Error::UnableToReadBytes { .. } | Error::SocketError { .. } => {}
            _ => panic!("expected stream read error"),
        }
        let requests = src.requests.lock().unwrap().clone();
        assert_eq!(
            requests,
            vec![
                None,
                Some(GetRange::Offset(0)),
                Some(GetRange::Offset(0)),
                Some(GetRange::Offset(0)),
            ]
        );
    }

    /// Verify that MiscTransient errors during streaming trigger retry (same as SocketError/UnableToReadBytes).
    #[tokio::test]
    async fn test_misc_transient_triggers_streaming_retry() {
        let data: Vec<u8> = (0..16_000u32).flat_map(|v| v.to_le_bytes()).collect();
        let data_len = data.len();
        let chunk_size = 4096;
        let fail_at_chunk = 5;
        let src = Arc::new(
            MockSource::new(data.clone(), chunk_size, Some(fail_at_chunk))
                .with_fail_error(MockFailError::MiscTransient),
        );
        let range = Some(GetRange::Bounded(0..data_len));
        let initial = src.get("mock://path", range.clone(), None).await.unwrap();
        let wrapped = initial.with_retry(StreamingRetryParams::new(
            src.clone(),
            "mock://path".to_string(),
            range.clone(),
            None,
        ));
        let out = wrapped.bytes().await.unwrap();
        assert_eq!(out.len(), data.len());
        assert_eq!(&out[..], &data[..]);
        // Should have retried: initial request + retries on MiscTransient
        let requests = src.requests.lock().unwrap().clone();
        assert!(
            requests.len() > 1,
            "expected retry on MiscTransient, got {} requests",
            requests.len()
        );
    }

    /// Verify that transient get() failures are retried by ExponentialBackoff.
    #[tokio::test]
    async fn test_transient_get_failure_retried() {
        use crate::retry::{ExponentialBackoff, RetryError};

        let data: Vec<u8> = (0..4_000u32).flat_map(|v| v.to_le_bytes()).collect();
        let src: Arc<dyn ObjectSource> = Arc::new(
            MockSource::new(data.clone(), 4096, None).with_fail_get(2), // fail first 2 get() calls
        );

        let get_result = ExponentialBackoff {
            max_tries: 4,
            jitter_ms: 1,
            max_backoff_ms: 10,
            max_waittime_ms: None,
        }
        .retry(|| {
            let source = src.clone();
            async move {
                source
                    .get("mock://path", None, None)
                    .await
                    .map_err(|e| match &e {
                        Error::MiscTransient { .. }
                        | Error::SocketError { .. }
                        | Error::ConnectTimeout { .. } => RetryError::Transient(e),
                        _ => RetryError::Permanent(e),
                    })
            }
        })
        .await
        .expect("should succeed after retries");

        let out = get_result.bytes().await.unwrap();
        assert_eq!(out.len(), data.len());
        assert_eq!(&out[..], &data[..]);
    }
}
