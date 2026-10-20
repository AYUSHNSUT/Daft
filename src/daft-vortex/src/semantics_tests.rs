use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array,
    RecordBatch as ArrowRecordBatch, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use daft_core::{lit::Literal, operator::Operator};
use daft_dsl::{
    Column, Expr, ExprRef,
    expr::{PlanRef, UnresolvedColumn, bound_expr::BoundExpr},
    lit, resolved_col,
};
use futures::TryStreamExt;
use vortex::{VortexSessionDefault, arrow::ArrowSessionExt, session::VortexSession};

use super::{options, primitive_batch, read_vortex_schema, stream_vortex, write_batch};
use crate::{VortexConvertOptions, VortexReadOptions, plan::plan_scan};

struct IntegerCase {
    name: &'static str,
    array: ArrayRef,
    literal: Literal,
    values: Vec<Option<i128>>,
    threshold: i128,
}

fn integer_cases() -> Vec<IntegerCase> {
    macro_rules! signed {
        ($name:literal, $ty:ty, $array:ty, $literal:ident) => {{
            let values = vec![
                Some(<$ty>::MIN),
                Some(-2),
                Some(-1),
                Some(0),
                Some(1),
                Some(2),
                Some(<$ty>::MAX),
                None,
            ];
            IntegerCase {
                name: $name,
                array: Arc::new(<$array>::from(values.clone())),
                literal: Literal::$literal(0),
                values: values
                    .into_iter()
                    .map(|value| value.map(i128::from))
                    .collect(),
                threshold: 0,
            }
        }};
    }
    macro_rules! unsigned {
        ($name:literal, $ty:ty, $array:ty, $literal:ident) => {{
            let values = vec![
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(<$ty>::MAX - 1),
                Some(<$ty>::MAX),
                None,
            ];
            IntegerCase {
                name: $name,
                array: Arc::new(<$array>::from(values.clone())),
                literal: Literal::$literal(2),
                values: values
                    .into_iter()
                    .map(|value| value.map(i128::from))
                    .collect(),
                threshold: 2,
            }
        }};
    }
    vec![
        signed!("i8", i8, Int8Array, Int8),
        signed!("i16", i16, Int16Array, Int16),
        signed!("i32", i32, Int32Array, Int32),
        signed!("i64", i64, Int64Array, Int64),
        unsigned!("u8", u8, UInt8Array, UInt8),
        unsigned!("u16", u16, UInt16Array, UInt16),
        unsigned!("u32", u32, UInt32Array, UInt32),
        unsigned!("u64", u64, UInt64Array, UInt64),
    ]
}

fn binary(op: Operator, left: ExprRef, right: ExprRef) -> ExprRef {
    Arc::new(Expr::BinaryOp { op, left, right })
}

fn compare(op: Operator, left: i128, right: i128) -> bool {
    match op {
        Operator::Eq => left == right,
        Operator::NotEq => left != right,
        Operator::Lt => left < right,
        Operator::LtEq => left <= right,
        Operator::Gt => left > right,
        Operator::GtEq => left >= right,
        _ => unreachable!(),
    }
}

async fn result_ids(
    uri: &str,
    convert: VortexConvertOptions,
    projection: bool,
    filter: bool,
) -> Vec<i64> {
    let batches = stream_vortex(
        uri,
        VortexReadOptions {
            batch_size: 3,
            enable_projection_pushdown: projection,
            enable_filter_pushdown: filter,
            ..Default::default()
        },
        convert,
    )
    .await
    .unwrap()
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .get_column(0)
                .i64()
                .unwrap()
                .into_iter()
                .map(Option::unwrap)
        })
        .collect()
}

#[tokio::test]
async fn exact_integer_comparisons_match_independent_oracle_for_all_flags() {
    let cases = integer_cases();
    let mut fields = vec![Field::new("id", ArrowType::Int64, false)];
    fields.extend(
        cases
            .iter()
            .map(|case| Field::new(case.name, case.array.data_type().clone(), true)),
    );
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(0..8))];
    arrays.extend(cases.iter().map(|case| case.array.clone()));
    let batch = ArrowRecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), arrays).unwrap();
    let file = write_batch(batch.clone()).await;
    let uri = file.path().to_str().unwrap();
    let schema = read_vortex_schema(uri).await.unwrap();
    let dtype = VortexSession::default()
        .arrow()
        .from_arrow_schema(batch.schema().as_ref())
        .unwrap();
    for case in cases {
        for op in [
            Operator::Eq,
            Operator::NotEq,
            Operator::Lt,
            Operator::LtEq,
            Operator::Gt,
            Operator::GtEq,
        ] {
            for reversed in [false, true] {
                let column = resolved_col(case.name);
                let literal = Arc::new(Expr::Literal(case.literal.clone()));
                let (left, right) = if reversed {
                    (literal, column)
                } else {
                    (column, literal)
                };
                let predicate = binary(op, left, right);
                let mut convert = options(schema.clone());
                convert.predicate = Some(predicate.clone());
                convert.include_columns = Some(vec!["id".into()]);
                let plan =
                    plan_scan(&schema, &dtype, &VortexReadOptions::default(), &convert).unwrap();
                assert!(plan.pushed_filter.is_some(), "{predicate}");
                assert!(
                    plan.filter_fallbacks.is_empty(),
                    "{:?}",
                    plan.filter_fallbacks
                );
                let expected = case
                    .values
                    .iter()
                    .enumerate()
                    .filter_map(|(id, value)| {
                        value
                            .filter(|value| {
                                if reversed {
                                    compare(op, case.threshold, *value)
                                } else {
                                    compare(op, *value, case.threshold)
                                }
                            })
                            .map(|_| id as i64)
                    })
                    .collect::<Vec<_>>();
                for projection in [false, true] {
                    for filter in [false, true] {
                        assert_eq!(
                            result_ids(uri, convert.clone(), projection, filter).await,
                            expected,
                            "{predicate}; projection={projection}, filter={filter}"
                        );
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn boolean_equality_and_null_predicates_keep_three_valued_semantics() {
    let batch = ArrowRecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowType::Int64, false),
            Field::new("flag", ArrowType::Boolean, true),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..4)),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
        ],
    )
    .unwrap();
    let file = write_batch(batch).await;
    let uri = file.path().to_str().unwrap();
    let schema = read_vortex_schema(uri).await.unwrap();
    let cases = [
        (resolved_col("flag").eq(lit(true)), vec![0, 3]),
        (resolved_col("flag").not_eq(lit(true)), vec![1]),
        (lit(false).eq(resolved_col("flag")), vec![1]),
        (lit(false).not_eq(resolved_col("flag")), vec![0, 3]),
        (resolved_col("flag").is_null(), vec![2]),
        (resolved_col("flag").not_null(), vec![0, 1, 3]),
        (resolved_col("id").is_null(), vec![]),
        (resolved_col("id").not_null(), vec![0, 1, 2, 3]),
    ];
    for (predicate, expected) in cases {
        let mut convert = options(schema.clone());
        convert.predicate = Some(predicate);
        convert.include_columns = Some(vec!["id".into()]);
        for projection in [false, true] {
            for filter in [false, true] {
                assert_eq!(
                    result_ids(uri, convert.clone(), projection, filter).await,
                    expected
                );
            }
        }
    }
}

#[tokio::test]
async fn bound_residual_is_remapped_and_unsupported_and_child_keeps_dependencies() {
    let batch = primitive_batch(true);
    let file = write_batch(batch.clone()).await;
    let uri = file.path().to_str().unwrap();
    let schema = read_vortex_schema(uri).await.unwrap();
    let dtype = VortexSession::default()
        .arrow()
        .from_arrow_schema(batch.schema().as_ref())
        .unwrap();
    let predicate = resolved_col("value")
        .gt(lit(19_i64))
        .and(binary(Operator::Plus, resolved_col("unsigned"), lit(0_u64)).gt(lit(3_u64)));
    let bound = BoundExpr::try_new(predicate, &schema).unwrap().into_inner();
    let mut convert = options(schema.clone());
    convert.predicate = Some(bound);
    convert.include_columns = Some(vec!["id".into()]);
    let plan = plan_scan(&schema, &dtype, &VortexReadOptions::default(), &convert).unwrap();
    assert_eq!(plan.physical_columns, ["id", "value", "unsigned"]);
    assert!(plan.pushed_filter.is_some());
    assert_eq!(plan.filter_fallbacks.len(), 1);
    for projection in [false, true] {
        for filter in [false, true] {
            assert_eq!(
                result_ids(uri, convert.clone(), projection, filter).await,
                [4, 5, 7, 8, 9]
            );
        }
    }
}

#[tokio::test]
async fn unsupported_or_not_cast_and_mixed_integer_filters_remain_complete_residuals() {
    let batch = ArrowRecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowType::Int64, false),
            Field::new("unsigned", ArrowType::UInt64, true),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..5)),
            Arc::new(UInt64Array::from(vec![
                Some((1_u64 << 53) - 1),
                Some(1_u64 << 53),
                Some((1_u64 << 53) + 1),
                Some(u64::MAX),
                None,
            ])),
        ],
    )
    .unwrap();
    let file = write_batch(batch.clone()).await;
    let uri = file.path().to_str().unwrap();
    let schema = read_vortex_schema(uri).await.unwrap();
    let dtype = VortexSession::default()
        .arrow()
        .from_arrow_schema(batch.schema().as_ref())
        .unwrap();
    // Mixed signed/unsigned comparison follows Daft's Float64 coercion: the
    // adjacent integers at 2^53 compare equal. Exact-integer pushdown is unsafe.
    let cases = [
        (resolved_col("unsigned").eq(lit(1_i64 << 53)), vec![1, 2]),
        (
            resolved_col("id")
                .eq(lit(0_i64))
                .or(resolved_col("id").eq(lit(2_i64))),
            vec![0, 2],
        ),
        (resolved_col("id").eq(lit(0_i64)).not(), vec![1, 2, 3, 4]),
        (
            resolved_col("id")
                .cast(&daft_core::prelude::DataType::Float64)
                .gt(lit(1_f64)),
            vec![2, 3, 4],
        ),
    ];
    for (predicate, expected) in cases {
        let mut convert = options(schema.clone());
        convert.predicate = Some(predicate);
        convert.include_columns = Some(vec!["id".into()]);
        let plan = plan_scan(&schema, &dtype, &VortexReadOptions::default(), &convert).unwrap();
        assert!(plan.pushed_filter.is_none());
        assert!(!plan.filter_fallbacks.is_empty());
        for projection in [false, true] {
            for filter in [false, true] {
                assert_eq!(
                    result_ids(uri, convert.clone(), projection, filter).await,
                    expected
                );
            }
        }
    }
}

#[tokio::test]
async fn reordered_and_empty_projections_preserve_values_and_cardinality() {
    let file = write_batch(primitive_batch(true)).await;
    let uri = file.path().to_str().unwrap();
    let schema = read_vortex_schema(uri).await.unwrap();
    for projection in [false, true] {
        for filter in [false, true] {
            let read_options = VortexReadOptions {
                batch_size: 3,
                enable_projection_pushdown: projection,
                enable_filter_pushdown: filter,
                ..Default::default()
            };
            let mut convert = options(schema.clone());
            convert.include_columns = Some(vec!["text".into(), "id".into()]);
            convert.limit = Some(2);
            let batches = stream_vortex(uri, read_options.clone(), convert)
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(batches[0].schema.names(), ["text", "id"]);
            assert_eq!(
                batches[0]
                    .get_column(0)
                    .utf8()
                    .unwrap()
                    .into_iter()
                    .collect::<Vec<_>>(),
                [Some(""), None]
            );
            assert_eq!(
                batches[0]
                    .get_column(1)
                    .i64()
                    .unwrap()
                    .into_iter()
                    .collect::<Vec<_>>(),
                [Some(0), Some(1)]
            );

            for (predicate, expected) in [(None, 5), (Some(lit(true)), 5), (Some(lit(false)), 0)] {
                let mut convert = options(schema.clone());
                convert.include_columns = Some(vec![]);
                convert.predicate = predicate;
                convert.limit = Some(5);
                let batches = stream_vortex(uri, read_options.clone(), convert)
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                assert_eq!(
                    batches.iter().map(|batch| batch.len()).sum::<usize>(),
                    expected
                );
                assert!(batches.iter().all(|batch| batch.num_columns() == 0));
            }
            for (predicate, expected) in [
                (
                    resolved_col("nothing").is_null(),
                    (0..10).collect::<Vec<_>>(),
                ),
                (resolved_col("nothing").not_null(), vec![]),
            ] {
                let mut convert = options(schema.clone());
                convert.include_columns = Some(vec!["id".into()]);
                convert.predicate = Some(predicate);
                assert_eq!(result_ids(uri, convert, projection, filter).await, expected);
            }
        }
    }
}

#[tokio::test]
async fn invalid_projection_and_bound_column_errors_are_not_silent_fallbacks() {
    let batch = primitive_batch(true);
    let file = write_batch(batch.clone()).await;
    let uri = file.path().to_str().unwrap();
    let schema = read_vortex_schema(uri).await.unwrap();
    let dtype = VortexSession::default()
        .arrow()
        .from_arrow_schema(batch.schema().as_ref())
        .unwrap();
    for (columns, expected) in [
        (vec!["id".into(), "id".into()], "duplicate"),
        (vec!["missing".into()], "missing"),
    ] {
        let mut convert = options(schema.clone());
        convert.include_columns = Some(columns);
        let error = plan_scan(&schema, &dtype, &VortexReadOptions::default(), &convert)
            .err()
            .unwrap();
        assert!(error.to_string().contains(expected), "{error}");
    }
    let mut convert = options(schema.clone());
    convert.predicate = Some(daft_dsl::expr::bound_col(999, schema[0].clone()).eq(lit(0_i64)));
    let error = plan_scan(&schema, &dtype, &VortexReadOptions::default(), &convert)
        .err()
        .unwrap();
    assert!(error.to_string().contains("outside the full schema"));
    convert.predicate = Some(resolved_col("missing").eq(lit(0_i64)));
    let error = plan_scan(&schema, &dtype, &VortexReadOptions::default(), &convert)
        .err()
        .unwrap();
    assert!(error.to_string().contains("missing"));

    for plan_ref in [PlanRef::Alias("other_source".into()), PlanRef::Id(7)] {
        let column = Arc::new(Expr::Column(Column::Unresolved(UnresolvedColumn {
            name: "id".into(),
            plan_ref,
            plan_schema: None,
        })));
        convert.predicate = Some(column.eq(lit(0_i64)));
        let error = plan_scan(&schema, &dtype, &VortexReadOptions::default(), &convert)
            .err()
            .unwrap();
        assert!(error.to_string().contains("qualified"), "{error}");
    }
    let unqualified = Arc::new(Expr::Column(Column::Unresolved(UnresolvedColumn {
        name: "id".into(),
        plan_ref: PlanRef::Unqualified,
        plan_schema: None,
    })));
    convert.predicate = Some(unqualified.eq(lit(0_i64)));
    let plan = plan_scan(&schema, &dtype, &VortexReadOptions::default(), &convert).unwrap();
    assert!(plan.pushed_filter.is_some());
    assert_eq!(result_ids(uri, convert, true, true).await, [0]);
}
