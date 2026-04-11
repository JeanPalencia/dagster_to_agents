"""
Validates that all data sources required by the channel attribution PRD
exist, have data, and contain the expected fields.
"""
import sys
sys.path.insert(0, "/home/luis/Spot2/dagster/dagster-pipeline/src")

from dagster_pipeline.defs.data_lakehouse.shared import query_bronze_source


def check_geospot_table(table_name: str, json_fields: list[str], extra_fields: list[str] = []):
    print("=" * 80)
    print(f"TABLE: {table_name} (geospot_postgres)")
    print("=" * 80)

    df_count = query_bronze_source(
        f"SELECT COUNT(*) as total FROM {table_name}",
        source_type="geospot_postgres",
    )
    total = df_count["total"][0]
    print(f"Total rows: {total:,}")

    if total == 0:
        print("  *** TABLE IS EMPTY ***")
        return

    cols = ", ".join(
        [f"COUNT(*) FILTER (WHERE jsonb_array_length(COALESCE({f}, '[]'::jsonb)) > 0) as {f}_nonempty" for f in json_fields]
    )
    df_summary = query_bronze_source(
        f"SELECT COUNT(*) as total, {cols} FROM {table_name}",
        source_type="geospot_postgres",
    )
    print("\nJSON array field coverage:")
    for f in json_fields:
        nonempty = df_summary[f"{f}_nonempty"][0]
        pct = nonempty / total * 100
        print(f"  {f:30s} -> {nonempty:,} / {total:,} non-empty ({pct:.1f}%)")

    sample_cols = ", ".join(extra_fields + [f"{f}::text as {f}" for f in json_fields] + ["updated_at", "created_at"])
    df_sample = query_bronze_source(
        f"SELECT {sample_cols} FROM {table_name} ORDER BY updated_at DESC LIMIT 3",
        source_type="geospot_postgres",
    )
    print(f"\nSample (3 most recent):")
    print(df_sample)
    print()


def check_chatbot_spot_confirmations():
    print("=" * 80)
    print("TABLE: conversation_events WHERE event_type = 'spot_confirmation' (chatbot_postgres)")
    print("=" * 80)

    df_count = query_bronze_source(
        "SELECT COUNT(*) as total FROM conversation_events WHERE event_type = 'spot_confirmation'",
        source_type="chatbot_postgres",
    )
    total = df_count["total"][0]
    print(f"Total rows: {total:,}")

    if total == 0:
        print("  *** NO spot_confirmation EVENTS FOUND ***")
        return

    df_cols = query_bronze_source(
        """SELECT column_name, data_type
           FROM information_schema.columns
           WHERE table_name = 'conversation_events'
           ORDER BY ordinal_position""",
        source_type="chatbot_postgres",
    )
    print(f"\nconversation_events schema ({df_cols.height} columns):")
    for row in df_cols.iter_rows():
        print(f"  {row[0]:30s} {row[1]}")

    df_sample = query_bronze_source(
        """SELECT id, conversation_id, client_id, spot_id, event_type, created_at
           FROM conversation_events
           WHERE event_type = 'spot_confirmation'
           ORDER BY created_at DESC
           LIMIT 5""",
        source_type="chatbot_postgres",
    )
    print(f"\nSample (5 most recent spot_confirmation events):")
    print(df_sample)

    df_nulls = query_bronze_source(
        """SELECT
              COUNT(*) as total,
              COUNT(client_id) as has_client_id,
              COUNT(spot_id) as has_spot_id,
              COUNT(created_at) as has_created_at
           FROM conversation_events
           WHERE event_type = 'spot_confirmation'""",
        source_type="chatbot_postgres",
    )
    print(f"\nNull check for key fields:")
    print(df_nulls)
    print()


if __name__ == "__main__":
    check_geospot_table(
        "recommendation_projects",
        json_fields=["spots_suggested", "white_list", "black_list"],
        extra_fields=["project_id"],
    )

    check_geospot_table(
        "recommendation_chatbot",
        json_fields=["spots_suggested", "white_list", "black_list"],
        extra_fields=["id"],
    )

    check_geospot_table(
        "recommendation_visitspotsrecommendations",
        json_fields=["suggested_spots", "white_list", "black_list"],
        extra_fields=["project_id", "client_id"],
    )

    check_chatbot_spot_confirmations()

    print("\n" + "=" * 80)
    print("VALIDATION COMPLETE")
    print("=" * 80)
