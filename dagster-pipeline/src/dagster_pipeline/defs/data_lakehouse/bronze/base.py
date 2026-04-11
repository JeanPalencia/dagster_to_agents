# defs/data_lakehouse/bronze/base.py
"""
Bronze base: single public API.

- make_bronze_asset(source_type, table_name=..., ...): one function; source_type is
  mysql_prod (S2P), bigquery, geospot_postgres, or chatbot_postgres.
  Use table_name for simple SELECT * FROM table, or query= + asset_name= + description= for custom SQL.
"""
from typing import Callable, Dict, List, Literal, Optional

import dagster as dg
import polars as pl

from dagster_pipeline.defs.data_lakehouse.shared import (
    daily_partitions,
    query_bronze_source,
    build_bronze_s3_key,
    write_polars_to_s3,
    resolve_stale_reference_date,
)

BronzeSourceType = Literal["mysql_prod", "bigquery", "geospot_postgres", "chatbot_postgres", "staging_postgres"]

_SOURCE_LABEL: dict[str, str] = {
    "mysql_prod": "Spot2 Platform (MySQL Production)",
    "bigquery": "BigQuery",
    "geospot_postgres": "Geospot PostgreSQL",
    "chatbot_postgres": "Chatbot PostgreSQL",
    "staging_postgres": "Staging (Spot Chatbot PostgreSQL)",
}


# ============ Generic shell (internal) ============


def _run_bronze_extract(
    context: dg.AssetExecutionContext,
    asset_name: str,
    source_type: str,
    query: str,
) -> pl.DataFrame:
    """Common logic: run query, log, persist to S3, return df."""
    df = query_bronze_source(query, source_type=source_type, context=context)
    ref_date = resolve_stale_reference_date(context)
    context.log.info(
        f"📥 {asset_name}: {df.height:,} rows, {df.width} columns (ref_date={ref_date})"
    )
    s3_key = build_bronze_s3_key(asset_name, file_format="parquet")
    write_polars_to_s3(df, s3_key, context, file_format="parquet")
    return df


def _build_asset(
    asset_name: str,
    connector: str,
    query: str,
    description: str,
    deps: Optional[List[str]] = None,
    *,
    partitioned: bool = True,
    tags: Optional[Dict[str, str]] = None,
) -> Callable:
    """Build Dagster asset from name, connector, query, description."""
    def _asset(context):
        from dagster_pipeline.defs.pipeline_asset_error_handling import iter_job_wrapped_compute

        def body():
            return _run_bronze_extract(
                context,
                asset_name=asset_name,
                source_type=connector,
                query=query,
            )

        yield from iter_job_wrapped_compute(context, body)
    _asset.__name__ = asset_name
    asset_kwargs = dict(
        name=asset_name,
        group_name="bronze",
        description=description,
        io_manager_key="s3_bronze",
    )
    if partitioned:
        asset_kwargs["partitions_def"] = daily_partitions
    if deps:
        asset_kwargs["deps"] = deps
    if tags:
        asset_kwargs["tags"] = tags
    return dg.asset(**asset_kwargs)(_asset)


# ============ Public API ============


def make_bronze_asset(
    source_type: BronzeSourceType,
    *,
    table_name: Optional[str] = None,
    query: Optional[str] = None,
    asset_name: Optional[str] = None,
    description: Optional[str] = None,
    where_clause: Optional[str] = None,
    description_suffix: str = "",
    deps: Optional[List[str]] = None,
    partitioned: bool = True,
    tags: Optional[Dict[str, str]] = None,
) -> dg.AssetsDefinition:
    """
    Build a Bronze asset for a given source.

    - For simple table extractions: pass table_name (and optionally where_clause).
      asset_name and description are derived by source_type (e.g. mysql_prod ->
      raw_s2p_{table}_new; bigquery -> raw_bq_{last_part_of_table}_new).
    - For custom SQL: pass query, asset_name, and description (required).
    - partitioned=False: asset sin daily_partitions (p. ej. raw S2P usado en bt_conv y lk_*).
    - tags: optional Dagster asset tags (e.g. lk_spots_pipeline for per-job concurrency limits).
    """
    if query is None and table_name is None:
        raise ValueError("Either table_name or query must be provided.")
    if query is not None and table_name is not None:
        raise ValueError("Provide either table_name or query, not both.")

    label = _SOURCE_LABEL.get(source_type, source_type)

    if query is not None:
        if asset_name is None or description is None:
            raise ValueError("For custom query, asset_name and description are required.")
        return _build_asset(
            asset_name,
            source_type,
            query,
            description,
            deps=deps,
            partitioned=partitioned,
            tags=tags,
        )

    # table_name provided
    if source_type == "mysql_prod":
        base_asset_name = f"raw_s2p_{table_name}_new"
        base_description = f"Bronze: raw extraction of {table_name} from {label}." + description_suffix
        if where_clause:
            query = f"SELECT * FROM {table_name} WHERE {where_clause}"
        else:
            query = f"SELECT * FROM {table_name}"
        name = asset_name if asset_name is not None else base_asset_name
        desc = description if description is not None else base_description
        return _build_asset(
            name, source_type, query, desc, deps=deps, partitioned=partitioned, tags=tags
        )
    elif source_type == "bigquery":
        # table_name is project.dataset.table → default asset uses last part (e.g. funnel_with_channel)
        bq_table_short = table_name.split(".")[-1]
        base_asset_name = f"raw_bq_{bq_table_short}_new"
        base_description = f"Bronze: raw extraction of {bq_table_short} from {label}."
        query = f"SELECT * FROM `{table_name}`"
        name = asset_name if asset_name is not None else base_asset_name
        desc = description if description is not None else base_description
        return _build_asset(
            name, source_type, query, desc, deps=deps, partitioned=partitioned, tags=tags
        )
    elif source_type in ("geospot_postgres", "chatbot_postgres"):
        if source_type == "geospot_postgres":
            base_asset_name = f"raw_gs_{table_name}_new"
            base_description = f"Bronze: raw extraction of {table_name} from {label}."
        else:
            base_asset_name = f"raw_cb_{table_name}_new"
            base_description = f"Bronze: raw extraction of {table_name} from {label}."
        if where_clause:
            query = f"SELECT * FROM {table_name} WHERE {where_clause}"
        else:
            query = f"SELECT * FROM {table_name}"
        name = asset_name if asset_name is not None else base_asset_name
        desc = description if description is not None else base_description
        return _build_asset(
            name, source_type, query, desc, deps=deps, partitioned=partitioned, tags=tags
        )
    else:
        raise ValueError(f"Unsupported source_type: {source_type}")
