use std::{collections::HashSet, sync::Arc};

use common_error::{DaftError, DaftResult};
use daft_core::{
    lit::Literal,
    operator::Operator,
    prelude::{DataType, Schema},
};
use daft_dsl::{
    Column, Expr, ExprRef,
    expr::{BoundColumn, PlanRef, ResolvedColumn, UnresolvedColumn, bound_expr::BoundExpr},
    resolved_col,
};
use vortex::{
    array::expr::{self as vx, BoundExpression, Expression},
    dtype::{DType, FieldName},
};

use crate::{VortexConvertOptions, VortexReadOptions};

pub(crate) struct ScanPlan {
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    pub output_columns: Vec<String>,
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    pub physical_columns: Vec<String>,
    pub physical_indices: Vec<usize>,
    pub projection: BoundExpression,
    pub pushed_filter: Option<BoundExpression>,
    pub residual: Option<BoundExpr>,
    pub output_indices: Vec<usize>,
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    pub filter_fallbacks: Vec<String>,
}

/// Resolve every accepted column representation to its validated source name.
/// Rebinding these names after physical projection avoids stale bound indices.
fn normalize_columns(
    expression: &ExprRef,
    schema: &Schema,
    dependencies: &mut HashSet<String>,
) -> DaftResult<ExprRef> {
    if let Expr::Column(column) = expression.as_ref() {
        let name = match column {
            Column::Resolved(ResolvedColumn::Basic(name))
            | Column::Unresolved(UnresolvedColumn {
                name,
                plan_ref: PlanRef::Unqualified,
                ..
            }) => name.as_ref(),
            Column::Bound(BoundColumn { index, field }) => {
                let actual = schema.fields().get(*index).ok_or_else(|| {
                    DaftError::SchemaMismatch(format!(
                        "Vortex residual bound column index {index} is outside the full schema"
                    ))
                })?;
                if actual.name != field.name || actual.dtype != field.dtype {
                    return Err(DaftError::SchemaMismatch(format!(
                        "Vortex residual bound column {index} expects {field}, found {actual}"
                    )));
                }
                field.name.as_ref()
            }
            _ => {
                return Err(DaftError::ValueError(format!(
                    "Vortex residual cannot reference a qualified, join-side, or outer column: {column}"
                )));
            }
        };
        schema.get_index(name)?;
        dependencies.insert(name.to_string());
        return Ok(resolved_col(name));
    }
    let children = expression
        .children()
        .iter()
        .map(|child| normalize_columns(child, schema, dependencies))
        .collect::<DaftResult<Vec<_>>>()?;
    Ok(Arc::new(expression.with_new_children(children)))
}

fn direct_column(expression: &Expr) -> Option<&str> {
    match expression {
        Expr::Column(Column::Resolved(ResolvedColumn::Basic(name))) => Some(name),
        _ => None,
    }
}

fn typed_literal(literal: &Literal) -> Option<(DataType, Expression)> {
    Some(match literal {
        Literal::Int8(value) => (DataType::Int8, vx::lit(*value)),
        Literal::Int16(value) => (DataType::Int16, vx::lit(*value)),
        Literal::Int32(value) => (DataType::Int32, vx::lit(*value)),
        Literal::Int64(value) => (DataType::Int64, vx::lit(*value)),
        Literal::UInt8(value) => (DataType::UInt8, vx::lit(*value)),
        Literal::UInt16(value) => (DataType::UInt16, vx::lit(*value)),
        Literal::UInt32(value) => (DataType::UInt32, vx::lit(*value)),
        Literal::UInt64(value) => (DataType::UInt64, vx::lit(*value)),
        Literal::Boolean(value) => (DataType::Boolean, vx::lit(*value)),
        _ => return None,
    })
}

fn translate_comparison(
    op: Operator,
    left: &Expr,
    right: &Expr,
    schema: &Schema,
) -> Result<Expression, &'static str> {
    let (name, literal, reversed) = match (direct_column(left), direct_column(right)) {
        (Some(name), _) => match right {
            Expr::Literal(literal) => (name, literal, false),
            _ => return Err("comparison requires a direct column and a literal"),
        },
        (_, Some(name)) => match left {
            Expr::Literal(literal) => (name, literal, true),
            _ => return Err("comparison requires a direct column and a literal"),
        },
        _ => {
            return Err("comparison requires a direct column and a literal; casts are not removed");
        }
    };
    let (literal_type, literal) =
        typed_literal(literal).ok_or("only non-null integer and Boolean literals are supported")?;
    let field = schema.get_field(name).expect("normalized column exists");
    if field.dtype != literal_type {
        return Err("comparison requires exact matching types; numeric coercions are not pushed");
    }
    if literal_type == DataType::Boolean && !matches!(op, Operator::Eq | Operator::NotEq) {
        return Err("Boolean pushdown supports only equality and inequality");
    }
    let column = vx::col(name);
    let (left, right) = if reversed {
        (literal, column)
    } else {
        (column, literal)
    };
    match op {
        Operator::Eq => Ok(vx::eq(left, right)),
        Operator::NotEq => Ok(vx::not_eq(left, right)),
        Operator::Lt => Ok(vx::lt(left, right)),
        Operator::LtEq => Ok(vx::lt_eq(left, right)),
        Operator::Gt => Ok(vx::gt(left, right)),
        Operator::GtEq => Ok(vx::gt_eq(left, right)),
        _ => Err("comparison operator is not supported"),
    }
}

/// Only AND permits partial extraction. OR and NOT are deliberately opaque,
/// even when every child looks translatable, so no unsafe weakening occurs.
fn translate_filter(
    expression: &ExprRef,
    schema: &Schema,
    fallbacks: &mut Vec<String>,
) -> Option<Expression> {
    if let Expr::BinaryOp {
        op: Operator::And,
        left,
        right,
    } = expression.as_ref()
    {
        let left = translate_filter(left, schema, fallbacks);
        let right = translate_filter(right, schema, fallbacks);
        return match (left, right) {
            (Some(left), Some(right)) => Some(vx::and(left, right)),
            (some @ Some(_), None) | (None, some @ Some(_)) => some,
            (None, None) => None,
        };
    }
    let translated = match expression.as_ref() {
        Expr::BinaryOp {
            op: Operator::Or, ..
        } => Err("OR subtrees remain in Daft"),
        Expr::Not(_) => Err("NOT subtrees remain in Daft"),
        Expr::BinaryOp { op, left, right }
            if matches!(
                op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) =>
        {
            translate_comparison(*op, left, right, schema)
        }
        Expr::IsNull(child) => direct_column(child)
            .map(|name| vx::is_null(vx::col(name)))
            .ok_or("null checks require a direct column"),
        Expr::NotNull(child) => direct_column(child)
            .map(|name| vx::is_not_null(vx::col(name)))
            .ok_or("null checks require a direct column"),
        _ => Err("expression is outside the safe predicate subset"),
    };
    match translated {
        Ok(expression) => Some(expression),
        Err(reason) => {
            fallbacks.push(format!("{reason}: {expression}"));
            None
        }
    }
}

pub(crate) fn plan_scan(
    schema: &Schema,
    dtype: &DType,
    read_options: &VortexReadOptions,
    convert_options: &VortexConvertOptions,
) -> DaftResult<ScanPlan> {
    let output_columns = convert_options.include_columns.clone().unwrap_or_else(|| {
        schema
            .fields()
            .iter()
            .map(|field| field.name.to_string())
            .collect()
    });
    let mut output_names = HashSet::new();
    for name in &output_columns {
        if !output_names.insert(name) {
            return Err(DaftError::ValueError(format!(
                "Vortex output projection contains duplicate column {name:?}"
            )));
        }
        schema.get_index(name)?;
    }
    let mut required: HashSet<String> = output_columns.iter().cloned().collect();
    let normalized = convert_options
        .predicate
        .as_ref()
        .map(|predicate| normalize_columns(predicate, schema, &mut required))
        .transpose()?;
    if let Some(predicate) = &normalized {
        let result_type = predicate.to_field(schema)?.dtype;
        if result_type != DataType::Boolean {
            return Err(DaftError::TypeError(format!(
                "Vortex residual filter must return Boolean, got {result_type}"
            )));
        }
    }
    let physical_indices = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            !read_options.enable_projection_pushdown || required.contains(field.name.as_ref())
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let physical_schema = Schema::new(physical_indices.iter().map(|index| schema[*index].clone()));
    let physical_columns = physical_schema
        .fields()
        .iter()
        .map(|field| field.name.to_string())
        .collect::<Vec<_>>();
    let projection = vx::select(
        physical_columns
            .iter()
            .map(|name| FieldName::from(name.as_str()))
            .collect::<Vec<_>>(),
        vx::root(),
    )
    .bind(dtype)
    .map_err(|error| DaftError::ComputeError(format!("Vortex projection binding: {error}")))?;
    let output_indices = output_columns
        .iter()
        .map(|name| physical_schema.get_index(name))
        .collect::<DaftResult<Vec<_>>>()?;
    let residual = normalized
        .as_ref()
        .map(|predicate| BoundExpr::try_new(predicate.clone(), &physical_schema))
        .transpose()?;
    let mut filter_fallbacks = Vec::new();
    let pushed_filter = if let Some(predicate) = &normalized {
        if read_options.enable_filter_pushdown {
            translate_filter(predicate, schema, &mut filter_fallbacks).and_then(|expression| {
                match expression.bind(dtype) {
                    Ok(bound) => Some(bound),
                    Err(error) => {
                        filter_fallbacks.push(format!(
                            "Vortex predicate binding rejected the translated expression: {error}"
                        ));
                        None
                    }
                }
            })
        } else {
            filter_fallbacks.push("filter pushdown disabled".to_string());
            None
        }
    } else {
        None
    };
    Ok(ScanPlan {
        output_columns,
        physical_columns,
        physical_indices,
        projection,
        pushed_filter,
        residual,
        output_indices,
        filter_fallbacks,
    })
}
