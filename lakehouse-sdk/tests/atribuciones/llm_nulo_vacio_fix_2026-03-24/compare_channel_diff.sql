-- Comparison query: computes BOTH channel classifications (original vs fixed)
-- in a single BigQuery scan. Only returns rows where the classification differs.
--- Aca tomo a todos los usuarios
--- Me fijo en el dia cual fue la primera interacción que tuvo con nosotros, por donde vino....  
with pre_users_sin_exluir as (
  select event_date,
    user_pseudo_id,
    min(event_timestamp) as event_timestamp_day
  FROM `analytics_276054961.events_*`,
    UNNEST(event_params) AS ep
  WHERE event_date >= '20250201'
    and event_date < FORMAT_DATE('%Y%m%d', CURRENT_DATE())
    AND ep.key IN ('ga_session_id')
  GROUP BY event_date,
    user_pseudo_id
),
--excluimos lo que tenemos sospechas que son scraping
scraping as (
  select user_pseudo_id
  FROM `analytics_276054961.events_*`,
    UNNEST(event_params) AS ep
  where session_traffic_source_last_click.cross_channel_campaign.default_channel_group in ('Direct', 'Unassigned')
    and event_name = 'first_visit'
    and ep.key = 'page_location'
    and ep.value.string_value like 'https://spot2.mx/%'
    and device.web_info.browser_version = '129.0.6668.71'
  group by 1
),
pre_users as (
  select a11.*,
    case
      when a12.user_pseudo_id is null then 0
      else 1
    end as user_sospechoso
  from pre_users_sin_exluir a11
    left join scraping a12 on a11.user_pseudo_id = a12.user_pseudo_id
),
users AS (
  SELECT a11.event_date,
    a11.user_pseudo_id,
    CASE
      WHEN a11.session_traffic_source_last_click.cross_channel_campaign.source IS NULL
      OR TRIM(
        COALESCE(
          a11.session_traffic_source_last_click.cross_channel_campaign.source,
          ''
        )
      ) IN ('', '(not set)') THEN a11.traffic_source.source
      ELSE a11.session_traffic_source_last_click.cross_channel_campaign.source
    END AS source,
    CASE
      WHEN a11.session_traffic_source_last_click.cross_channel_campaign.medium IS NULL
      OR TRIM(
        COALESCE(
          a11.session_traffic_source_last_click.cross_channel_campaign.medium,
          ''
        )
      ) IN ('', '(not set)') THEN a11.traffic_source.medium
      ELSE a11.session_traffic_source_last_click.cross_channel_campaign.medium
    END AS medium,
    CASE
      WHEN a11.session_traffic_source_last_click.cross_channel_campaign.campaign_name IS NULL
      OR TRIM(
        COALESCE(
          a11.session_traffic_source_last_click.cross_channel_campaign.campaign_name,
          ''
        )
      ) IN ('', '(not set)') THEN a11.traffic_source.name
      ELSE a11.session_traffic_source_last_click.cross_channel_campaign.campaign_name
    END AS campaign_name,
    a12.user_sospechoso
  FROM `analytics_276054961.events_*` a11,
    UNNEST(event_params) AS ep
    inner join pre_users a12 on a11.event_date = a12.event_date
    and a11.user_pseudo_id = a12.user_pseudo_id
    and a11.event_timestamp = a12.event_timestamp_day
  WHERE a11.event_date >= '20250201'
    and a11.event_date < FORMAT_DATE('%Y%m%d', CURRENT_DATE())
    AND ep.key IN ('ga_session_id', 'page_location')
  GROUP BY event_date, user_pseudo_id, source, medium, campaign_name, user_sospechoso
),
classified AS (
  SELECT
    event_date,
    user_pseudo_id,
    source,
    medium,
    campaign_name,
    user_sospechoso,

    -- ORIGINAL classification (Nulo-Vacío before LLMs)
    CASE
      WHEN COALESCE(TRIM(medium), '') = ''
      or (
        (LOWER(source) like '%l.wl.co%' or LOWER(source) like 't.co%'
         or LOWER(source) like '%github.com%' or LOWER(source) like '%statics.teams.cdn.office.net%')
        and medium = 'referral'
      ) THEN 'Nulo-Vacío'
      WHEN (campaign_name = '(organic)' and lower(source) not in ('adwords') and lower(medium) not in ('spot'))
        OR medium = 'organic'
        OR ((COALESCE(source, '') = '') AND (COALESCE(medium, '') = '') AND LOWER(campaign_name) NOT LIKE '%comunicado%')
        or ((LOWER(source) like '%search.yam.com%' or LOWER(source) like '%copilot.microsoft.com%'
             or LOWER(source) like '%search.google.com' or LOWER(source) like '%msn.com') and medium = 'referral')
        or (REGEXP_CONTAINS(LOWER(source), r'(google|bing|yahoo|duckduckgo|ecosia|search|adwords)')
            and medium not in ('cpc', 'spot', 'paid', 'referral')
            and REGEXP_CONTAINS(LOWER(medium), r'(cpc,|spot,|paid,|(not set))') = false)
        or (REGEXP_CONTAINS(LOWER(source), r'(search.yahoo.com)') and medium = 'referral')
      THEN 'Organic Search'
      WHEN REGEXP_CONTAINS(LOWER(source), r'(chatgpt|perplexity|claude|bard|gemini|anthropic|openai|llm|ai\s*chat|ai\s*assistant)')
      THEN 'Organic LLMs'
      WHEN LOWER(source) = 'mail' OR (REGEXP_CONTAINS(LOWER(source), r'(gmail|outlook|yahoo|hotmail|protonmail|icloud|zoho|mail\.ru|yandex|aol|live\.com|office365|exchange|mailchimp|sendgrid|mailgun|postmark|amazonses|sendinblue|brevo|active_campaign|active_campaing)') and REGEXP_CONTAINS(LOWER(source), r'(search.)') = false)
      THEN 'Mail'
      WHEN LOWER(campaign_name) LIKE '%_display_%' or LOWER(campaign_name) LIKE '%_disp_%' THEN 'Display'
      WHEN source = '(direct)' THEN 'Direct'
      WHEN (campaign_name = '(referral)' OR LOWER(medium) LIKE '%referral%') AND NOT REGEXP_CONTAINS(LOWER(source), r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd.in)') THEN 'Referral'
      WHEN LOWER(campaign_name) LIKE '%cross-network%' OR LOWER(campaign_name) LIKE '%pmax%' OR LOWER(source) LIKE '%syndicatedsearch.goog%' OR (LOWER(source) LIKE '%google%' AND medium IN ('', 'cross-network')) or (LOWER(source) LIKE '%nova.taboolanews.com%' and medium = 'referral') THEN 'Cross-network'
      WHEN LOWER(campaign_name) LIKE '%youtube%' OR REGEXP_CONTAINS(LOWER(campaign_name), r'yt_') OR REGEXP_CONTAINS(LOWER(campaign_name), r'_yt') THEN 'Paid Video'
      WHEN medium IN ('cpc', 'spot', 'paid') AND (REGEXP_CONTAINS(LOWER(source), r'(google|bing|yahoo|duckduckgo|ecosia|search\.)') OR LOWER(campaign_name) LIKE '%search%' OR LOWER(campaign_name) LIKE '%srch%') or (LOWER(source) = 'adwords' and lower(medium) = '(not set)') THEN 'Paid Search'
      WHEN medium IN ('cpc', 'paid', 'paid_social') AND REGEXP_CONTAINS(LOWER(source), r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd.in)') THEN 'Paid Social'
      WHEN medium IN ('social', 'rss') OR (REGEXP_CONTAINS(LOWER(source), r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd.in)') AND medium NOT IN ('cpc', 'paid') and REGEXP_CONTAINS(LOWER(campaign_name), r'(meta)') = false) THEN 'Organic Social'
      ELSE 'Unassigned'
    END AS channel_original,

    -- FIXED classification (LLMs BEFORE Nulo-Vacío)
    CASE
      WHEN REGEXP_CONTAINS(LOWER(source), r'(chatgpt|perplexity|claude|bard|gemini|anthropic|openai|llm|ai\s*chat|ai\s*assistant)')
      THEN 'Organic LLMs'
      WHEN COALESCE(TRIM(medium), '') = ''
      or (
        (LOWER(source) like '%l.wl.co%' or LOWER(source) like 't.co%'
         or LOWER(source) like '%github.com%' or LOWER(source) like '%statics.teams.cdn.office.net%')
        and medium = 'referral'
      ) THEN 'Nulo-Vacío'
      WHEN (campaign_name = '(organic)' and lower(source) not in ('adwords') and lower(medium) not in ('spot'))
        OR medium = 'organic'
        OR ((COALESCE(source, '') = '') AND (COALESCE(medium, '') = '') AND LOWER(campaign_name) NOT LIKE '%comunicado%')
        or ((LOWER(source) like '%search.yam.com%' or LOWER(source) like '%copilot.microsoft.com%'
             or LOWER(source) like '%search.google.com' or LOWER(source) like '%msn.com') and medium = 'referral')
        or (REGEXP_CONTAINS(LOWER(source), r'(google|bing|yahoo|duckduckgo|ecosia|search|adwords)')
            and medium not in ('cpc', 'spot', 'paid', 'referral')
            and REGEXP_CONTAINS(LOWER(medium), r'(cpc,|spot,|paid,|(not set))') = false)
        or (REGEXP_CONTAINS(LOWER(source), r'(search.yahoo.com)') and medium = 'referral')
      THEN 'Organic Search'
      WHEN LOWER(source) = 'mail' OR (REGEXP_CONTAINS(LOWER(source), r'(gmail|outlook|yahoo|hotmail|protonmail|icloud|zoho|mail\.ru|yandex|aol|live\.com|office365|exchange|mailchimp|sendgrid|mailgun|postmark|amazonses|sendinblue|brevo|active_campaign|active_campaing)') and REGEXP_CONTAINS(LOWER(source), r'(search.)') = false)
      THEN 'Mail'
      WHEN LOWER(campaign_name) LIKE '%_display_%' or LOWER(campaign_name) LIKE '%_disp_%' THEN 'Display'
      WHEN source = '(direct)' THEN 'Direct'
      WHEN (campaign_name = '(referral)' OR LOWER(medium) LIKE '%referral%') AND NOT REGEXP_CONTAINS(LOWER(source), r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd.in)') THEN 'Referral'
      WHEN LOWER(campaign_name) LIKE '%cross-network%' OR LOWER(campaign_name) LIKE '%pmax%' OR LOWER(source) LIKE '%syndicatedsearch.goog%' OR (LOWER(source) LIKE '%google%' AND medium IN ('', 'cross-network')) or (LOWER(source) LIKE '%nova.taboolanews.com%' and medium = 'referral') THEN 'Cross-network'
      WHEN LOWER(campaign_name) LIKE '%youtube%' OR REGEXP_CONTAINS(LOWER(campaign_name), r'yt_') OR REGEXP_CONTAINS(LOWER(campaign_name), r'_yt') THEN 'Paid Video'
      WHEN medium IN ('cpc', 'spot', 'paid') AND (REGEXP_CONTAINS(LOWER(source), r'(google|bing|yahoo|duckduckgo|ecosia|search\.)') OR LOWER(campaign_name) LIKE '%search%' OR LOWER(campaign_name) LIKE '%srch%') or (LOWER(source) = 'adwords' and lower(medium) = '(not set)') THEN 'Paid Search'
      WHEN medium IN ('cpc', 'paid', 'paid_social') AND REGEXP_CONTAINS(LOWER(source), r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd.in)') THEN 'Paid Social'
      WHEN medium IN ('social', 'rss') OR (REGEXP_CONTAINS(LOWER(source), r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd.in)') AND medium NOT IN ('cpc', 'paid') and REGEXP_CONTAINS(LOWER(campaign_name), r'(meta)') = false) THEN 'Organic Social'
      ELSE 'Unassigned'
    END AS channel_fixed

  FROM users
)
SELECT
  source,
  medium,
  campaign_name,
  channel_original,
  channel_fixed,
  COUNT(*) AS user_sessions,
  COUNT(DISTINCT user_pseudo_id) AS unique_users
FROM classified
WHERE channel_original != channel_fixed
GROUP BY source, medium, campaign_name, channel_original, channel_fixed
ORDER BY unique_users DESC, user_sessions DESC
