use std::sync::Arc;

use arrow_array::RecordBatch as ArrowRecordBatch;
use arrow_schema::DataType;
use daft_dsl::{lit, resolved_col};
use futures::TryStreamExt;
use vortex::{VortexSessionDefault, arrow::ArrowSessionExt, session::VortexSession};

use super::{VortexExecutionContext, schemas_with_offsets};
use crate::{
    VortexReadOptions,
    tests::{options, primitive_batch, read_vortex_schema, stream_vortex, write_batch},
};

#[test]
fn direct_export_uses_large_offsets_without_changing_validation_schema() {
    let batch = primitive_batch(true);
    let session = VortexSession::default();
    let dtype = session
        .arrow()
        .from_arrow_schema(batch.schema().as_ref())
        .unwrap();
    let (normal, expected) = schemas_with_offsets("test", &session, &dtype, false).unwrap();
    let (large, actual) = schemas_with_offsets("test", &session, &dtype, true).unwrap();
    assert_eq!(expected, actual);
    assert!(
        normal
            .fields()
            .iter()
            .any(|f| f.data_type() == &DataType::Utf8)
    );
    assert!(
        large
            .fields()
            .iter()
            .any(|f| f.data_type() == &DataType::LargeUtf8)
    );
    assert!(
        !large
            .fields()
            .iter()
            .any(|f| matches!(f.data_type(), DataType::Utf8 | DataType::Binary))
    );
}

#[tokio::test]
async fn reader_controls_preserve_order_limits_and_batch_bound() {
    let file = write_batch(primitive_batch(true)).await;
    let uri = file.path().to_str().unwrap();
    let schema = read_vortex_schema(uri).await.unwrap();
    for bits in 0..16 {
        let read = VortexReadOptions {
            batch_size: 2,
            enable_large_offsets: bits & 1 != 0,
            execution_context: (bits & 2 != 0).then(|| Arc::new(VortexExecutionContext::default())),
            enable_file_pruning: bits & 4 != 0,
            enable_layout_splitting: bits & 8 != 0,
            ..Default::default()
        };
        for limit in [0, 1, 3, 100] {
            let mut convert = options(schema.clone());
            convert.predicate = Some(resolved_col("id").gt_eq(lit(2_i64)));
            convert.include_columns = Some(vec!["text".into(), "id".into()]);
            convert.limit = Some(limit);
            let batches = stream_vortex(uri, read.clone(), convert)
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert!(batches.iter().all(|batch| batch.len() <= 2));
            let ids = batches
                .iter()
                .flat_map(|batch| batch.get_column(1).i64().unwrap().into_iter())
                .collect::<Vec<_>>();
            assert_eq!(ids, (2..10).take(limit).map(Some).collect::<Vec<_>>());
        }
        let mut convert = options(schema.clone());
        convert.predicate = Some(resolved_col("id").lt(lit(-1_i64)));
        assert!(
            stream_vortex(uri, read, convert)
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn shared_context_refreshes_files_and_does_not_hide_schema_errors() {
    let batch = primitive_batch(true);
    let file = write_batch(batch.clone()).await;
    let uri = file.path().to_str().unwrap();
    let schema = read_vortex_schema(uri).await.unwrap();
    let context = Arc::new(VortexExecutionContext::default());
    for expected in [10, 2] {
        let replacement = write_batch(batch.slice(0, expected)).await;
        std::fs::copy(replacement.path(), file.path()).unwrap();
        let read = VortexReadOptions {
            execution_context: Some(context.clone()),
            ..Default::default()
        };
        let batches = stream_vortex(uri, read, options(schema.clone()))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.len()).sum::<usize>(),
            expected
        );
    }
    let replacement = write_batch(
        ArrowRecordBatch::try_new(
            batch.schema().project(&[0]).unwrap().into(),
            vec![batch.column(0).clone()],
        )
        .unwrap(),
    )
    .await;
    std::fs::copy(replacement.path(), file.path()).unwrap();
    let mut convert = options(schema);
    convert.predicate = Some(resolved_col("id").lt(lit(-1_i64)));
    let read = VortexReadOptions {
        enable_file_pruning: true,
        execution_context: Some(context),
        ..Default::default()
    };
    assert!(stream_vortex(uri, read, convert).await.is_err());
}
