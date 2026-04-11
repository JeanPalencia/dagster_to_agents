"""Main logic for the LK Visitors pipeline."""

from datetime import datetime
from pathlib import Path

import pandas as pd

from dagster_pipeline.defs.lk_visitors.utils.database import query_bigquery


QUERIES_DIR = Path(__file__).parent / "queries"


def get_data(kind: str) -> pd.DataFrame:
    """Load data from SQL query."""
    match kind:
        case "funnel_with_channel":
            query_path = QUERIES_DIR / "funnel_with_channel.sql"
            with open(query_path, "r") as f:
                query = f.read()
            return query_bigquery(query)

        case _:
            raise ValueError(f"Unknown kind: {kind}")


def create_primary_key(row) -> str:
    """Create primary key from user_pseudo_id and vis_create_date."""
    user_id = row.get("user_pseudo_id")
    create_date = row.get("vis_create_date")

    user_str = str(user_id) if pd.notna(user_id) else ""
    date_str = pd.to_datetime(create_date).strftime("%Y%m%d") if pd.notna(create_date) else ""

    return f"{user_str}_{date_str}"


def format_output(df: pd.DataFrame) -> pd.DataFrame:
    """Format DataFrame for final output with correct columns and types."""
    result = df.copy()

    # Create primary key
    result["vis_id"] = result.apply(create_primary_key, axis=1)

    # Ensure vis_create_date is date only (no time)
    result["vis_create_date"] = pd.to_datetime(result["vis_create_date"]).dt.date

    # Add audit columns
    now = datetime.now()
    today = now.date()
    result["aud_inserted_date"] = today
    result["aud_inserted_at"] = now
    result["aud_updated_date"] = today
    result["aud_updated_at"] = now
    result["aud_job"] = "lk_visitors_pipeline"

    # Select and order final columns
    final_columns = [
        "vis_id",
        "user_pseudo_id",
        "vis_create_date",
        "vis_is_scraping",
        "vis_source",
        "vis_medium",
        "vis_campaign_name",
        "vis_channel",
        "vis_traffic_type",
        "aud_inserted_date",
        "aud_inserted_at",
        "aud_updated_date",
        "aud_updated_at",
        "aud_job",
    ]

    # Add missing columns as None
    for col in final_columns:
        if col not in result.columns:
            result[col] = None

    return result[final_columns]


def prepare_funnel_for_output(funnel: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate funnel and format for output."""
    funnel_dedup = funnel.drop_duplicates(
        subset=["user_pseudo_id", "vis_create_date"],
        keep="first",
    )
    return format_output(funnel_dedup)


def run_pipeline() -> pd.DataFrame:
    """Run the complete LK Visitors pipeline."""
    print("Loading data...")
    funnel_with_channel = get_data("funnel_with_channel")

    print("Deduplicating and formatting output...")
    result = prepare_funnel_for_output(funnel_with_channel)

    print(f"Pipeline completed. Output: {len(result)} rows")
    return result
