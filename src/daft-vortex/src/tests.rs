use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int64Array, ListArray,
    NullArray, RecordBatch as ArrowRecordBatch, RecordBatchOptions, StringArray, UInt16Array,
    UInt64Array, types::Int64Type,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use common_error::DaftResult;
use daft_core::prelude::{Schema, SchemaRef};
use daft_dsl::{lit, resolved_col};
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, TryStreamExt};
use tempfile::NamedTempFile;
use vortex::{
    VortexSessionDefault, arrow::ArrowSessionExt, buffer::ByteBufferMut,
    file::WriteOptionsSessionExt, io::session::RuntimeSessionExt, session::VortexSession,
};

use crate::{VortexConvertOptions, VortexReadOptions, read::NULLABILITY_METADATA_KEY};

#[path = "semantics_tests.rs"]
mod semantics_tests;

pub(crate) async fn read_vortex_schema(uri: &str) -> DaftResult<SchemaRef> {
    crate::read_vortex_schema(
        uri,
        Arc::new(daft_io::IOClient::new(Default::default())?),
        daft_io::IOStatsContext::new("Vortex schema test"),
        VortexReadOptions::default(),
    )
    .await
}

pub(crate) async fn stream_vortex(
    uri: &str,
    read: VortexReadOptions,
    convert: VortexConvertOptions,
) -> DaftResult<futures::stream::BoxStream<'static, DaftResult<RecordBatch>>> {
    crate::stream_vortex(
        uri,
        Arc::new(daft_io::IOClient::new(Default::default())?),
        daft_io::IOStatsContext::new("Vortex scan test"),
        read,
        convert,
    )
    .await
}

pub(crate) fn primitive_batch(value_nullable: bool) -> ArrowRecordBatch {
    let value = if value_nullable {
        vec![
            None,
            Some(1),
            Some(10),
            Some(20),
            Some(30),
            Some(40),
            None,
            Some(50),
            Some(60),
            Some(70),
        ]
    } else {
        vec![
            Some(0),
            Some(1),
            Some(10),
            Some(20),
            Some(30),
            Some(40),
            Some(0),
            Some(50),
            Some(60),
            Some(70),
        ]
    };
    let fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, value_nullable),
        Field::new("flag", DataType::Boolean, true),
        Field::new("measure", DataType::Float64, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, true),
        Field::new("unsigned", DataType::UInt64, true),
        Field::new("small_signed", DataType::Int8, true),
        Field::new("small_unsigned", DataType::UInt16, true),
        Field::new("single", DataType::Float32, true),
        Field::new("nothing", DataType::Null, true),
    ];
    ArrowRecordBatch::try_new(
        Arc::new(ArrowSchema::new(fields)),
        vec![
            Arc::new(Int64Array::from_iter_values(0..10)),
            Arc::new(Int64Array::from(value)),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(true),
                Some(false),
                Some(true),
                Some(false),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(f64::from_bits(0x7ff8_0000_0000_0042)),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                None,
                Some(1.5),
                Some(-2.5),
                Some(f64::MIN),
                Some(f64::MAX),
            ])),
            Arc::new(StringArray::from(vec![
                Some(""),
                None,
                Some("hello"),
                Some("नमस्ते"),
                Some("🦆"),
                Some("longer than inline string storage"),
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
            ])),
            Arc::new(BinaryArray::from(vec![
                Some(&b""[..]),
                None,
                Some(&b"\0\xff"[..]),
                Some(&b"binary"[..]),
                Some(&b"a"[..]),
                Some(&b"b"[..]),
                Some(&b"c"[..]),
                Some(&b"d"[..]),
                Some(&b"e"[..]),
                Some(&b"f"[..]),
            ])),
            Arc::new(UInt64Array::from(vec![
                Some(0),
                Some(u64::MAX),
                None,
                Some(3),
                Some(4),
                Some(5),
                Some(6),
                Some(7),
                Some(8),
                Some(9),
            ])),
            Arc::new(Int8Array::from(vec![
                Some(i8::MIN),
                Some(i8::MAX),
                None,
                Some(0),
                Some(-1),
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
            ])),
            Arc::new(UInt16Array::from(vec![
                Some(0),
                Some(u16::MAX),
                None,
                Some(3),
                Some(4),
                Some(5),
                Some(6),
                Some(7),
                Some(8),
                Some(9),
            ])),
            Arc::new(Float32Array::from(vec![
                Some(-0.0),
                Some(f32::NAN),
                None,
                Some(3.5),
                Some(4.5),
                Some(5.5),
                Some(6.5),
                Some(7.5),
                Some(8.5),
                Some(9.5),
            ])),
            Arc::new(NullArray::new(10)),
        ],
    )
    .unwrap()
}

pub(crate) async fn write_batch(batch: ArrowRecordBatch) -> NamedTempFile {
    let file = NamedTempFile::new().unwrap();
    let session = VortexSession::default().with_tokio();
    let array = session
        .arrow()
        .from_arrow_record_batch(batch.clone(), batch.schema().as_ref())
        .unwrap();
    let mut bytes = ByteBufferMut::empty();
    session
        .write_options()
        .write(&mut bytes, array.to_array_stream())
        .await
        .unwrap();
    std::fs::write(file.path(), bytes.as_slice()).unwrap();
    file
}

pub(crate) fn options(schema: SchemaRef) -> VortexConvertOptions {
    VortexConvertOptions {
        schema,
        predicate: None,
        include_columns: None,
        limit: None,
    }
}

async fn collect(
    uri: &str,
    batch_size: usize,
    options: VortexConvertOptions,
) -> DaftResult<Vec<RecordBatch>> {
    stream_vortex(
        uri,
        VortexReadOptions {
            batch_size,
            ..Default::default()
        },
        options,
    )
    .await?
    .try_collect()
    .await
}

#[tokio::test]
async fn footer_metadata_preserves_schema_and_exact_row_counts() -> DaftResult<()> {
    let primitive = primitive_batch(true);
    let zero_fields = ArrowRecordBatch::try_new_with_options(
        Arc::new(ArrowSchema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(5)),
    )?;
    for expected in [
        primitive.slice(0, 0),
        primitive.slice(0, 1),
        primitive,
        zero_fields,
    ] {
        let expected_rows = expected.num_rows() as u64;
        let file = write_batch(expected.clone()).await;
        let uri = file.path().to_str().unwrap();
        let (schema, num_rows) = crate::read_vortex_metadata(
            uri,
            Arc::new(daft_io::IOClient::new(Default::default())?),
            daft_io::IOStatsContext::new("Vortex footer metadata test"),
            VortexReadOptions {
                max_concurrent_requests: 1,
                max_in_flight_bytes: 1024 * 1024,
                ..Default::default()
            },
        )
        .await?;
        assert_eq!(num_rows, expected_rows);
        assert_eq!(schema, read_vortex_schema(uri).await?);
        assert_eq!(schema.len(), expected.num_columns());
        for (field, expected_field) in schema.fields().iter().zip(expected.schema().fields()) {
            let expected_nullable = expected_field.is_nullable().to_string();
            assert_eq!(
                field.metadata.get(NULLABILITY_METADATA_KEY),
                Some(&expected_nullable)
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn bulk_footer_metadata_preserves_order_duplicates_and_refreshes_counts() -> DaftResult<()> {
    let mut files = Vec::new();
    for rows in [3, 0, 10] {
        files.push(write_batch(primitive_batch(true).slice(0, rows)).await);
    }
    let uris = [2, 0, 1, 2]
        .map(|index| files[index].path().to_string_lossy().into_owned())
        .to_vec();
    let client = Arc::new(daft_io::IOClient::new(Default::default())?);
    for max_concurrent_files in [1, 2, 64] {
        let stats = daft_io::IOStatsContext::new("Vortex bulk metadata order test");
        let metadata = crate::read_vortex_metadata_bulk(
            &uris,
            client.clone(),
            stats.clone(),
            VortexReadOptions::default(),
            max_concurrent_files,
        )
        .await?;
        assert_eq!(
            metadata.iter().map(|(_, rows)| *rows).collect::<Vec<_>>(),
            [10, 3, 0, 10]
        );
        let counts = crate::read::read_vortex_row_counts_bulk(
            &uris,
            client.clone(),
            daft_io::IOStatsContext::new("Vortex row count parity test"),
            VortexReadOptions::default(),
            max_concurrent_files,
        )
        .await?;
        assert_eq!(counts, [10, 3, 0, 10]);
        let cached_counts = crate::read::read_vortex_row_counts_bulk_cached(
            &uris,
            client.clone(),
            daft_io::IOStatsContext::new("Vortex cached row count parity test"),
            VortexReadOptions::default(),
            max_concurrent_files,
            crate::snapshot::MAX_SNAPSHOT_BYTES,
        )
        .await?;
        assert_eq!(
            cached_counts,
            vec![(10, None), (3, None), (0, None), (10, None)]
        );
        assert!(stats.load_get_requests() >= uris.len());
        assert!(stats.load_bytes_read() > 0);
        assert_eq!(stats.load_list_requests(), 0);
        for (schema, _) in metadata {
            assert_eq!(schema.len(), primitive_batch(true).num_columns());
            assert_eq!(
                schema
                    .get_field("id")?
                    .metadata
                    .get(NULLABILITY_METADATA_KEY)
                    .map(String::as_str),
                Some("false")
            );
        }
    }

    let replacement = write_batch(primitive_batch(true).slice(0, 7)).await;
    std::fs::copy(replacement.path(), files[0].path())?;
    let refreshed = crate::read_vortex_metadata_bulk(
        &uris,
        client.clone(),
        daft_io::IOStatsContext::new("Vortex bulk metadata refresh test"),
        VortexReadOptions::default(),
        2,
    )
    .await?;
    assert_eq!(
        refreshed.iter().map(|(_, rows)| *rows).collect::<Vec<_>>(),
        [10, 7, 0, 10]
    );
    assert_eq!(
        crate::read::read_vortex_row_counts_bulk(
            &uris,
            client,
            daft_io::IOStatsContext::new("Vortex row count refresh test"),
            VortexReadOptions::default(),
            2,
        )
        .await?,
        [10, 7, 0, 10]
    );
    Ok(())
}

#[tokio::test]
async fn bulk_footer_metadata_accepts_empty_inputs_without_io() -> DaftResult<()> {
    let client = Arc::new(daft_io::IOClient::new(Default::default())?);
    for max_concurrent_files in [1, 8, 64] {
        let stats = daft_io::IOStatsContext::new("Vortex empty bulk metadata test");
        let metadata = crate::read_vortex_metadata_bulk(
            &[],
            client.clone(),
            stats.clone(),
            VortexReadOptions::default(),
            max_concurrent_files,
        )
        .await?;
        assert!(metadata.is_empty());
        assert_eq!(stats.load_head_requests(), 0);
        assert_eq!(stats.load_get_requests(), 0);
        assert_eq!(stats.load_list_requests(), 0);
        assert_eq!(stats.load_bytes_read(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn bulk_footer_metadata_rejects_invalid_concurrency_before_io() -> DaftResult<()> {
    let client = Arc::new(daft_io::IOClient::new(Default::default())?);
    let missing = vec!["missing.vortex".to_string()];
    for max_concurrent_files in [0, 65, usize::MAX] {
        let stats = daft_io::IOStatsContext::new("Vortex invalid bulk metadata test");
        let error = crate::read_vortex_metadata_bulk(
            &missing,
            client.clone(),
            stats.clone(),
            VortexReadOptions::default(),
            max_concurrent_files,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("max_concurrent_files"));
        assert_eq!(stats.load_head_requests(), 0);
        assert_eq!(stats.load_get_requests(), 0);
        assert_eq!(stats.load_bytes_read(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn primitive_values_nulls_and_retained_batches_roundtrip() -> DaftResult<()> {
    let expected = primitive_batch(true);
    let file = write_batch(expected.clone()).await;
    let uri = url::Url::from_file_path(file.path()).unwrap().to_string();
    let schema = read_vortex_schema(&uri).await?;
    let mut stream = stream_vortex(
        &uri,
        VortexReadOptions {
            batch_size: 3,
            ..Default::default()
        },
        options(schema.clone()),
    )
    .await?;
    let retained = stream.try_next().await?.unwrap();
    drop(stream);
    assert_eq!(retained.get_column(0).i64()?.get(0), Some(0));
    assert_eq!(retained.len(), 3);

    let batches = collect(&uri, 3, options(schema)).await?;
    assert!(batches.iter().all(|batch| batch.len() <= 3));
    let actual = ArrowRecordBatch::try_from(RecordBatch::concat(&batches)?)?;
    assert_eq!(actual.num_rows(), expected.num_rows());
    for (expected, actual) in expected.columns().iter().zip(actual.columns()) {
        let restored = arrow::compute::cast(actual, expected.data_type())?;
        // Valid float bits (including NaN payload/signed zero) and null locations
        // are compared; unused storage beneath nulls is irrelevant.
        assert_eq!(expected.to_data(), restored.to_data());
    }
    assert!(
        actual
            .schema()
            .fields()
            .iter()
            .all(|field| !field.metadata().contains_key(NULLABILITY_METADATA_KEY))
    );
    Ok(())
}

#[tokio::test]
async fn complete_residual_precedes_projection_and_exact_limit() -> DaftResult<()> {
    let file = write_batch(primitive_batch(true)).await;
    let uri = file.path().to_str().unwrap();
    let mut convert = options(read_vortex_schema(uri).await?);
    convert.predicate = Some(resolved_col("value").gt(lit(19_i64)));
    convert.include_columns = Some(vec!["id".into()]);
    convert.limit = Some(2);
    let batches = collect(uri, 4, convert).await?;
    assert_eq!(
        batches.iter().map(RecordBatch::len).collect::<Vec<_>>(),
        [1, 1]
    );
    let result = RecordBatch::concat(&batches)?;
    assert_eq!(result.num_columns(), 1);
    assert_eq!(
        result.get_column(0).i64()?.into_iter().collect::<Vec<_>>(),
        [Some(3), Some(4)]
    );
    Ok(())
}

#[tokio::test]
async fn zero_column_projection_keeps_post_filter_cardinality() -> DaftResult<()> {
    let file = write_batch(primitive_batch(true)).await;
    let uri = file.path().to_str().unwrap();
    let mut convert = options(read_vortex_schema(uri).await?);
    convert.predicate = Some(resolved_col("value").is_null());
    convert.include_columns = Some(vec![]);
    convert.limit = Some(1);
    let batches = collect(uri, 2, convert).await?;
    assert_eq!(batches.iter().map(RecordBatch::len).sum::<usize>(), 1);
    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    Ok(())
}

#[tokio::test]
async fn empty_and_zero_field_files_preserve_schema_and_rows() -> DaftResult<()> {
    let empty = write_batch(primitive_batch(true).slice(0, 0)).await;
    let uri = empty.path().to_str().unwrap();
    let schema = read_vortex_schema(uri).await?;
    assert_eq!(schema.len(), primitive_batch(true).num_columns());
    assert!(collect(uri, 2, options(schema)).await?.is_empty());

    let zero_fields = ArrowRecordBatch::try_new_with_options(
        Arc::new(ArrowSchema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(5)),
    )?;
    let file = write_batch(zero_fields).await;
    let uri = file.path().to_str().unwrap();
    let schema = read_vortex_schema(uri).await?;
    assert!(schema.is_empty());
    let batches = collect(uri, 2, options(schema)).await?;
    assert_eq!(batches.iter().map(RecordBatch::len).sum::<usize>(), 5);
    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    Ok(())
}

#[tokio::test]
async fn full_schema_nullability_is_checked_before_projection_and_zero_limit() -> DaftResult<()> {
    let first = write_batch(primitive_batch(true)).await;
    let other = write_batch(primitive_batch(false)).await;
    let schema = read_vortex_schema(first.path().to_str().unwrap()).await?;
    let uri = other.path().to_str().unwrap();
    let mut convert = options(schema);
    convert.include_columns = Some(vec!["id".into()]);
    convert.limit = Some(0);
    let error = stream_vortex(uri, VortexReadOptions::default(), convert)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains(uri));
    assert!(error.contains("source nullability differs"));
    assert!(error.contains("value"));
    Ok(())
}

#[tokio::test]
async fn full_schema_rejects_order_type_and_extra_columns() -> DaftResult<()> {
    let batch = primitive_batch(true);
    let first = write_batch(batch.clone()).await;
    let schema = read_vortex_schema(first.path().to_str().unwrap()).await?;
    let mut indices = (0..batch.num_columns()).collect::<Vec<_>>();
    indices.swap(0, 1);
    let reordered = batch.project(&indices)?;
    let mut fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields[0] = fields[0].clone().with_data_type(DataType::UInt64);
    let mut arrays = batch.columns().to_vec();
    arrays[0] = arrow::compute::cast(&arrays[0], &DataType::UInt64)?;
    let changed_type = ArrowRecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), arrays)?;
    let fewer_columns = batch.project(&[0])?;
    for batch in [reordered, changed_type, fewer_columns] {
        let file = write_batch(batch).await;
        let uri = file.path().to_str().unwrap();
        let mut convert = options(schema.clone());
        convert.include_columns = Some(vec!["id".into()]);
        let error = stream_vortex(uri, VortexReadOptions::default(), convert)
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains(uri));
        assert!(error.contains("schema"));
    }
    Ok(())
}

#[tokio::test]
async fn schema_without_source_nullability_marker_is_rejected() -> DaftResult<()> {
    let file = write_batch(primitive_batch(true)).await;
    let uri = file.path().to_str().unwrap();
    let plain = Arc::new(Schema::try_from(primitive_batch(true).schema().as_ref())?);
    let error = stream_vortex(uri, VortexReadOptions::default(), options(plain))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("lacks source nullability"));
    assert!(error.contains(uri));
    Ok(())
}

#[tokio::test]
async fn unsupported_nested_type_is_rejected_at_schema_inference() {
    let array =
        ListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(1), None]), None]);
    let batch = ArrowRecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            "nested",
            array.data_type().clone(),
            true,
        )])),
        vec![Arc::new(array)],
    )
    .unwrap();
    let file = write_batch(batch).await;
    let uri = file.path().to_str().unwrap();
    let error = read_vortex_schema(uri).await.unwrap_err().to_string();
    assert!(error.contains(uri));
    assert!(error.contains("flat primitive"));
}

#[tokio::test]
async fn invalid_sources_and_zero_batch_size_have_pathful_errors() -> DaftResult<()> {
    for uri in [
        "gs://bucket/data.vortex",
        "ftp://example.com/data.vortex",
        "file://remote-host/data.vortex",
    ] {
        assert!(
            read_vortex_schema(uri)
                .await
                .unwrap_err()
                .to_string()
                .contains(uri)
        );
    }
    let corrupt = NamedTempFile::new()?;
    std::fs::write(corrupt.path(), b"not a Vortex file")?;
    let uri = corrupt.path().to_str().unwrap();
    assert!(
        read_vortex_schema(uri)
            .await
            .unwrap_err()
            .to_string()
            .contains(uri)
    );

    let file = write_batch(primitive_batch(true)).await;
    let uri = file.path().to_str().unwrap();
    let schema = read_vortex_schema(uri).await?;
    let error = stream_vortex(
        uri,
        VortexReadOptions {
            batch_size: 0,
            ..Default::default()
        },
        options(schema.clone()),
    )
    .await
    .err()
    .unwrap()
    .to_string();
    assert!(error.contains(uri));
    assert!(error.contains("positive"));
    let mut convert = options(schema);
    convert.limit = Some(0);
    assert!(
        stream_vortex(uri, VortexReadOptions::default(), convert)
            .await?
            .next()
            .await
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn file_uris_preserve_literal_percent_names_and_reject_hosts() -> DaftResult<()> {
    let directory = tempfile::tempdir()?;
    let literal = directory.path().join("a%20b.vortex");
    let decoded = directory.path().join("a b.vortex");
    let literal_source = write_batch(primitive_batch(true).slice(0, 2)).await;
    let decoded_source = write_batch(primitive_batch(true).slice(0, 5)).await;
    std::fs::copy(literal_source.path(), &literal)?;
    std::fs::copy(decoded_source.path(), &decoded)?;
    let path = literal.to_str().unwrap().replace('\\', "/");
    let uri = if path.starts_with('/') {
        format!("file://{path}")
    } else {
        format!("file:///{path}")
    };
    let schema = read_vortex_schema(&uri).await?;
    let batches = collect(&uri, 2, options(schema)).await?;
    assert_eq!(batches.iter().map(RecordBatch::len).sum::<usize>(), 2);

    let decoded_path = decoded.to_str().unwrap();
    let schema = read_vortex_schema(decoded_path).await?;
    let batches = collect(decoded_path, 2, options(schema)).await?;
    assert_eq!(batches.iter().map(RecordBatch::len).sum::<usize>(), 5);

    for uri in [
        "file://localhost/tmp/data.vortex",
        "file://remote-host/tmp/data.vortex",
    ] {
        let error = read_vortex_schema(uri).await.unwrap_err().to_string();
        assert!(error.contains("must not contain a host"));
        assert!(error.contains(uri));
    }
    for suffix in ["?query=true", "#fragment"] {
        let uri = format!("{uri}{suffix}");
        let error = read_vortex_schema(&uri).await.unwrap_err().to_string();
        assert!(error.contains("query or fragment"));
        assert!(error.contains(&crate::io::display_uri(&uri)));
        assert!(!error.contains(suffix));
    }
    Ok(())
}

#[tokio::test]
async fn rejected_source_urls_redact_credentials_and_queries_before_io() {
    for (uri, expected) in [
        (
            "gs://user:password-sentinel@example.test/data?token=query-sentinel#fragment-sentinel",
            "supports only",
        ),
        (
            "https://user:password-sentinel@example.test:invalid/data?token=query-sentinel",
            "Invalid Vortex URL",
        ),
        (
            "file://user:password-sentinel@example.test/data?token=query-sentinel",
            "Invalid Vortex URL",
        ),
        (
            "file://localhost/data?token=query-sentinel#fragment-sentinel",
            "query or fragment",
        ),
        (
            "file:///tmp/data?token=query-sentinel#fragment-sentinel",
            "query or fragment",
        ),
        (
            "s3:///data?token=query-sentinel#fragment-sentinel",
            "requires a host",
        ),
    ] {
        let error = read_vortex_schema(uri).await.unwrap_err().to_string();
        assert!(error.contains(expected), "{error}");
        assert!(error.contains(&crate::io::display_uri(uri)), "{error}");
        for secret in ["password-sentinel", "query-sentinel", "fragment-sentinel"] {
            assert!(!error.contains(secret), "{error}");
        }
    }
}

#[test]
fn schema_validation_redacts_signed_source_urls() {
    use vortex::dtype::{DType, Nullability};

    use crate::read::{schemas, validate_schema};

    let uri =
        "https://user:password-sentinel@example.test/data?token=query-sentinel#fragment-sentinel";
    let session = VortexSession::default();
    let top_level_error = schemas(uri, &session, &DType::Bool(Nullability::NonNullable))
        .unwrap_err()
        .to_string();
    let nested_schema = ArrowSchema::new(vec![Field::new(
        "nested",
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    )]);
    let nested_dtype = session.arrow().from_arrow_schema(&nested_schema).unwrap();
    let nested_error = schemas(uri, &session, &nested_dtype)
        .unwrap_err()
        .to_string();
    let arrow = primitive_batch(true).schema();
    let dtype = session.arrow().from_arrow_schema(&arrow).unwrap();
    let (_, schema) = schemas(uri, &session, &dtype).unwrap();
    let mismatch_error = validate_schema(uri, &Schema::empty(), &schema)
        .unwrap_err()
        .to_string();
    let plain = Schema::try_from(arrow.as_ref()).unwrap();
    let marker_error = validate_schema(uri, &plain, &schema)
        .unwrap_err()
        .to_string();
    for (error, expected) in [
        (top_level_error, "top-level struct"),
        (nested_error, "flat primitive"),
        (mismatch_error, "full first-file schema"),
        (marker_error, "source nullability"),
    ] {
        assert!(error.contains(expected), "{error}");
        assert!(error.contains("https://example.test/data"), "{error}");
        for secret in ["password-sentinel", "query-sentinel", "fragment-sentinel"] {
            assert!(!error.contains(secret), "{error}");
        }
    }
}
