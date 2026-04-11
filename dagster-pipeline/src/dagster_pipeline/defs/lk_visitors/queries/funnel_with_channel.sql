SELECT
  user as user_pseudo_id,
  event_date as vis_create_date,
  user_sospechoso as vis_is_scraping,
  `source` as vis_source,
  medium as vis_medium,
  campaign_name as vis_campaign_name,
  Channel as vis_channel,
  Traffic_type AS vis_traffic_type,
  NULL as aud_inserted_date, 
  NULL as aud_inserted_at, 
  NULL as aud_updated_date, 
  NULL as aud_updated_at, 
  NULL as aud_job
FROM `spot2-mx-ga4-bq.analitics_spot2.funnel_with_channel`
WHERE user IS NOT NULL
  AND TRIM(SAFE_CAST(user AS STRING)) != ''
