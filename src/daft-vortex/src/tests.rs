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

use crate::{
    VortexConvertOptions, VortexReadOptions, read::NULLABILITY_METADATA_KEY, read_vortex_schema,
    stream_vortex,
};

fn primitive_batch(value_nullable: bool) -> ArrowRecordBatch {
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

async fn write_batch(batch: ArrowRecordBatch) -> NamedTempFile {
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

fn options(schema: SchemaRef) -> VortexConvertOptions {
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
    stream_vortex(uri, VortexReadOptions { batch_size }, options)
        .await?
        .try_collect()
        .await
}

#[tokio::test]
async fn primitive_values_nulls_and_retained_batches_roundtrip() -> DaftResult<()> {
    let expected = primitive_batch(true);
    let file = write_batch(expected.clone()).await;
    let uri = url::Url::from_file_path(file.path()).unwrap().to_string();
    let schema = read_vortex_schema(&uri).await?;
    let mut stream = stream_vortex(
        &uri,
        VortexReadOptions { batch_size: 3 },
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
        "s3://bucket/data.vortex",
        "https://example.com/data.vortex",
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
        VortexReadOptions { batch_size: 0 },
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
        assert!(error.contains(&uri));
    }
    Ok(())
}
