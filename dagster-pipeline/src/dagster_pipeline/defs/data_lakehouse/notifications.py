# defs/data_lakehouse/notifications.py
"""
Email notifications for data pipeline fallbacks and stale data usage.

This module handles notifications when assets use stale data (data from previous days)
due to upstream asset failures.
"""
import dagster as dg
from typing import List, Dict
from datetime import datetime

from dagster_pipeline.defs.data_lakehouse.shared import resolve_stale_reference_date


def send_stale_data_notification(
    context: dg.AssetExecutionContext,
    stale_tables: List[Dict[str, str]],
    current_asset_name: str = None,
    current_layer: str = None,
) -> None:
    """
    Sends email notification when stale data is used.
    
    Args:
        context: Dagster context
        stale_tables: List of upstream tables that used stale data. Each dict should have:
            {
                'table_name': str,  # e.g., 'raw_s2p_clients_new' (upstream table)
                'expected_date': str,  # e.g., '2026-01-14'
                'available_date': str,  # e.g., '2026-01-13' (what's in S3)
                'layer': str,  # 'bronze', 'silver', or 'gold' (upstream layer)
                'file_path': str,  # S3 path
            }
        current_asset_name: Name of the current asset that is using stale data (optional)
        current_layer: Layer of the current asset (optional)
    """
    if not stale_tables:
        return
    
    # Determine current asset info if not provided
    if current_asset_name is None:
        current_asset_name = context.asset_key.path[-1] if context.asset_key else "Unknown"
    if current_layer is None:
        # Infer layer from asset name
        if current_asset_name.startswith("raw_"):
            current_layer = "bronze"
        elif current_asset_name.startswith("stg_") or current_asset_name.startswith("core_"):
            current_layer = "silver"
        elif current_asset_name.startswith("gold_"):
            current_layer = "gold"
        else:
            current_layer = "unknown"
    
    ref_date = resolve_stale_reference_date(context)
    subject = f"⚠️ Data Pipeline Stale Data Alert - {ref_date}"

    body = f"""
Data Pipeline Stale Data Notification

Date: {ref_date}
Run ID: {context.run_id}
Timestamp: {datetime.now().isoformat()}

Asset: {current_asset_name} ({current_layer})
This asset is using stale data from the following upstream tables:

"""
    
    for info in stale_tables:
        upstream_layer = info['layer']
        upstream_table = info['table_name']
        body += f"""
- {upstream_table} ({upstream_layer})
  Expected date: {info['expected_date']}
  Available in S3: {info['available_date']} (from previous day)
  Path: {info['file_path']}
  
  ⚠️  The {upstream_layer} asset '{upstream_table}' failed or hasn't run for {ref_date}.
      Using data from {info['available_date']} instead.
"""
    
    body += f"""
    
Impact:
- {current_asset_name} ({current_layer}) is using data from previous day(s)
- This may affect downstream analytics and reporting
- Please investigate why the upstream {upstream_layer} asset(s) failed

---
This is an automated notification from the Dagster Data Pipeline.
"""
    
    # Log the notification (for now, until email is configured)
    context.log.warning(f"📧 EMAIL NOTIFICATION:\n{subject}\n{body}")
    
    # TODO: Implement actual email sending
    # Options:
    # 1. Use AWS SES (Simple Email Service)
    # 2. Use SMTP (Gmail, SendGrid, etc.)
    # 3. Use Slack/Discord webhook
    # 4. Use PagerDuty/Opsgenie for critical alerts
    
    # Example with AWS SES:
    # import boto3
    # ses = boto3.client('ses', region_name='us-east-1')
    # ses.send_email(
    #     Source='dagster-pipeline@example.com',
    #     Destination={'ToAddresses': ['data-team@example.com']},
    #     Message={
    #         'Subject': {'Data': subject},
    #         'Body': {'Text': {'Data': body}}
    #     }
    # )
