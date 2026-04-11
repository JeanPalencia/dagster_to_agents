"""Functions for processing lead events data."""
import pandas as pd


def process_lead_events(lead_events, exclude_spot2_emails: bool = True):
    """Process lead events: filter, enrich, and aggregate.

    Args:
        lead_events: DataFrame with lead events.
        exclude_spot2_emails: If True, remove users with @spot2 email. If False, include them.
    """
    df = lead_events.copy()

    # Identify users with contact
    users_with_contact = df.query("email.notna() | phone.notna()")[
        "user_pseudo_id"
    ].unique()
    df["has_contact"] = df["user_pseudo_id"].isin(users_with_contact)

    # Remove spot2 internal users (when configured)
    if exclude_spot2_emails:
        ids_spot2 = df[df["email"].str.lower().str.contains("@spot2", na=False)][
            "user_pseudo_id"
        ].unique()
        df = df[~df["user_pseudo_id"].isin(ids_spot2)].copy()

    # Detect Blog Submit events
    blog_users = df[
        df["event_name"].str.lower().str.contains("clientsubmitformblog", na=False)
    ]["user_pseudo_id"].unique()
    df["has_blog_submit"] = df["user_pseudo_id"].isin(blog_users)

    # Add period column
    df["year_month"] = df["event_datetime"].dt.to_period("M")

    # Get first event per user
    df_sorted = df.sort_values("event_datetime")
    df_sorted["rn"] = df_sorted.groupby("user_pseudo_id")["event_datetime"].rank(
        method="first"
    )
    df_first = df_sorted[df_sorted["rn"] == 1].copy()

    # Aggregate by month
    result_total = (
        df_first.groupby("year_month")["user_pseudo_id"]
        .nunique()
        .reset_index(name="unique_users_first_event")
    )
    result_by_event = (
        df_first.groupby(["year_month", "event_name"])["user_pseudo_id"]
        .nunique()
        .reset_index(name="count")
    )
    result_pivot = (
        result_by_event.pivot(index="year_month", columns="event_name", values="count")
        .fillna(0)
        .reset_index()
    )
    result = result_total.merge(result_pivot, on="year_month", how="left")

    # WhatsApp breakdowns by device (solo si hay event_name granular y device_category; con funnel solo no aplica)
    whatsapp = df_first[df_first["event_name"] == "clientRequestedWhatsappForm"]
    if "device_category" in df_first.columns and len(whatsapp) > 0:
        whatsapp_desktop = (
            whatsapp[whatsapp["device_category"] == "desktop"]
            .groupby("year_month")["user_pseudo_id"]
            .nunique()
            .reset_index(name="clientRequestedWhatsappForm_desktop")
        )
        whatsapp_mobile = (
            whatsapp[whatsapp["device_category"] != "desktop"]
            .groupby("year_month")["user_pseudo_id"]
            .nunique()
            .reset_index(name="clientRequestedWhatsappForm_mobile_other")
        )
    else:
        whatsapp_desktop = pd.DataFrame(columns=["year_month", "clientRequestedWhatsappForm_desktop"])
        whatsapp_mobile = pd.DataFrame(columns=["year_month", "clientRequestedWhatsappForm_mobile_other"])

    result = (
        result.merge(whatsapp_desktop, on="year_month", how="left")
        .merge(whatsapp_mobile, on="year_month", how="left")
        .fillna(0)
    )
    numeric_cols = [col for col in result if col != "year_month"]
    result[numeric_cols] = result[numeric_cols].astype(int)

    return df_first, result

