-- =============================================================================
-- lk_leads_v2: Gold table for leads with project count and funnel data
-- =============================================================================
-- Source: gold_lk_leads asset
-- Dependencies: stg_s2p_leads, stg_s2p_projects, core_project_funnel
-- 
-- Changes from v1:
--   + Added: lead_max_type_id, lead_campaign_type_id, lead_campaign_type
--   + Added: lead_project_funnel_relevant_id, lead_project_funnel_relevant
--   + Added: lead_lds_relevant_id, lead_lds_relevant (for BT_LDS_LEAD_SPOTS)
--   + Added: lead_type_date_start, lead_type_datetime_start, lead_profiling_completed_at
--   + Added: Most advanced project fields (lead_adv_*)
--   + Added: Most recent project fields (lead_rec_*)
--   - Removed: lead_dollar_exchange, filter
-- =============================================================================

DROP TABLE IF EXISTS lk_leads_v2;

CREATE TABLE lk_leads_v2 (
    -- -------------------------------------------------------------------------
    -- Identification
    -- -------------------------------------------------------------------------
    lead_id                             BIGINT,
    lead_name                           VARCHAR(255),
    lead_last_name                      VARCHAR(255),
    lead_mothers_last_name              VARCHAR(255),
    lead_email                          VARCHAR(255),
    lead_domain                         VARCHAR(255),
    lead_phone_indicator                VARCHAR(10),
    lead_phone_number                   VARCHAR(100),
    lead_full_phone_number              VARCHAR(100),
    lead_company                        VARCHAR(255),
    lead_position                       VARCHAR(255),

    -- -------------------------------------------------------------------------
    -- Origin
    -- -------------------------------------------------------------------------
    lead_origin_id                      SMALLINT,
    lead_origin                         VARCHAR(50),

    -- -------------------------------------------------------------------------
    -- Lead flags (L0-L4, Supply, S0-S3)
    -- -------------------------------------------------------------------------
    lead_l0                             BIGINT,
    lead_l1                             BIGINT,
    lead_l2                             BIGINT,
    lead_l3                             BIGINT,
    lead_l4                             BIGINT,
    lead_supply                         BIGINT,
    lead_s0                             BIGINT,
    lead_s1                             BIGINT,
    lead_s3                             BIGINT,

    -- -------------------------------------------------------------------------
    -- Max type (NEW in v2)
    -- -------------------------------------------------------------------------
    lead_max_type_id                    SMALLINT,
    lead_max_type                       VARCHAR(20),

    -- -------------------------------------------------------------------------
    -- Client type
    -- -------------------------------------------------------------------------
    lead_client_type_id                 SMALLINT,
    lead_client_type                    VARCHAR(50),

    -- -------------------------------------------------------------------------
    -- Users and Agents
    -- -------------------------------------------------------------------------
    user_id                             BIGINT,
    agent_id                            BIGINT,
    lead_agent_by_api                   BOOLEAN,
    agent_kam_id                        BIGINT,

    -- -------------------------------------------------------------------------
    -- Sector
    -- -------------------------------------------------------------------------
    spot_sector_id                      SMALLINT,
    spot_sector                         VARCHAR(50),

    -- -------------------------------------------------------------------------
    -- Status
    -- -------------------------------------------------------------------------
    lead_status_id                      SMALLINT,
    lead_status                         VARCHAR(100),

    -- -------------------------------------------------------------------------
    -- Contact
    -- -------------------------------------------------------------------------
    lead_first_contact_time             INTEGER,

    -- -------------------------------------------------------------------------
    -- Space requirements
    -- -------------------------------------------------------------------------
    spot_min_square_space               DOUBLE PRECISION,
    spot_max_square_space               DOUBLE PRECISION,

    -- -------------------------------------------------------------------------
    -- Source
    -- -------------------------------------------------------------------------
    lead_sourse_id                      SMALLINT,
    lead_sourse                         VARCHAR(100),

    -- -------------------------------------------------------------------------
    -- HubSpot fields
    -- -------------------------------------------------------------------------
    lead_hs_first_conversion            VARCHAR(255),
    lead_hs_utm_campaign                VARCHAR(255),

    -- -------------------------------------------------------------------------
    -- Campaign type (NEW in v2)
    -- -------------------------------------------------------------------------
    lead_campaign_type_id               SMALLINT,
    lead_campaign_type                  VARCHAR(20),

    -- -------------------------------------------------------------------------
    -- Analytics
    -- -------------------------------------------------------------------------
    lead_hs_analytics_source_data_1     VARCHAR(255),
    lead_hs_recent_conversion           VARCHAR(255),
    lead_hs_recent_conversion_date      TIMESTAMP,

    -- -------------------------------------------------------------------------
    -- Industrial profiling
    -- -------------------------------------------------------------------------
    lead_hs_industrial_profiling        TEXT,
    lead_industrial_profile_state       VARCHAR(255),
    lead_industrial_profile_zone        TEXT,
    lead_industrial_profile_size_id     SMALLINT,
    lead_industrial_profile_size        VARCHAR(100),
    lead_industrial_profile_budget_id   SMALLINT,
    lead_industrial_profile_budget      VARCHAR(100),

    -- -------------------------------------------------------------------------
    -- Office profiling
    -- -------------------------------------------------------------------------
    lead_hs_office_profiling            TEXT,
    lead_office_profile_state           VARCHAR(255),
    lead_office_profile_zone            TEXT,
    lead_office_profile_size_id         SMALLINT,
    lead_office_profile_size            VARCHAR(50),
    lead_office_profile_budget_id       SMALLINT,
    lead_office_profile_budget          VARCHAR(100),

    -- -------------------------------------------------------------------------
    -- Funnel relevance (NEW in v2)
    -- -------------------------------------------------------------------------
    lead_project_funnel_relevant_id     SMALLINT,
    lead_project_funnel_relevant        VARCHAR(10),

    -- -------------------------------------------------------------------------
    -- LDS relevance (for BT_LDS_LEAD_SPOTS)
    -- -------------------------------------------------------------------------
    lead_lds_relevant_id                SMALLINT,
    lead_lds_relevant                   VARCHAR(10),

    -- -------------------------------------------------------------------------
    -- Projects count (from JOIN with stg_s2p_projects)
    -- -------------------------------------------------------------------------
    lead_projects_count                 INTEGER,

    -- =========================================================================
    -- MOST ADVANCED PROJECT (furthest in funnel) - NEW in v2
    -- =========================================================================
    lead_adv_project_id                 BIGINT,
    lead_adv_funnel_lead_date           DATE,
    lead_adv_funnel_visit_created_date  DATE,
    lead_adv_funnel_visit_realized_at   TIMESTAMP,
    lead_adv_funnel_visit_confirmed_at  TIMESTAMP,
    lead_adv_funnel_loi_date            DATE,
    lead_adv_funnel_contract_date       DATE,
    lead_adv_funnel_transaction_date    DATE,
    lead_adv_funnel_flow                VARCHAR(50),

    -- =========================================================================
    -- MOST RECENT PROJECT (latest created) - NEW in v2
    -- =========================================================================
    lead_rec_project_id                 BIGINT,
    lead_rec_funnel_lead_date           DATE,
    lead_rec_funnel_visit_created_date  DATE,
    lead_rec_funnel_visit_realized_at   TIMESTAMP,
    lead_rec_funnel_visit_confirmed_at  TIMESTAMP,
    lead_rec_funnel_loi_date            DATE,
    lead_rec_funnel_contract_date       DATE,
    lead_rec_funnel_transaction_date    DATE,
    lead_rec_funnel_flow                VARCHAR(50),

    -- -------------------------------------------------------------------------
    -- Lead timestamps
    -- -------------------------------------------------------------------------
    lead_lead0_at                       TIMESTAMP,
    lead_lead1_at                       TIMESTAMP,
    lead_lead2_at                       TIMESTAMP,
    lead_lead3_at                       TIMESTAMP,
    lead_lead4_at                       TIMESTAMP,
    lead_supply_at                      TIMESTAMP,
    lead_supply0_at                     TIMESTAMP,
    lead_supply1_at                     TIMESTAMP,
    lead_supply3_at                     TIMESTAMP,
    lead_client_at                      TIMESTAMP,

    -- -------------------------------------------------------------------------
    -- Type datetime/date start (NEW in v2)
    -- -------------------------------------------------------------------------
    lead_type_datetime_start            TIMESTAMP,   -- Full timestamp
    lead_type_date_start                DATE,        -- Date only

    -- -------------------------------------------------------------------------
    -- Profiling completed (NEW in v2)
    -- -------------------------------------------------------------------------
    lead_profiling_completed_at         TIMESTAMP,

    -- -------------------------------------------------------------------------
    -- Created/Updated/Deleted timestamps
    -- -------------------------------------------------------------------------
    lead_created_at                     TIMESTAMP,
    lead_created_date                   DATE,
    lead_updated_at                     TIMESTAMP,
    lead_updated_date                   DATE,
    lead_deleted_at                     TIMESTAMP,
    lead_deleted_date                   DATE,

    -- -------------------------------------------------------------------------
    -- Audit fields
    -- -------------------------------------------------------------------------
    aud_inserted_at                     TIMESTAMP,
    aud_inserted_date                   DATE,
    aud_updated_at                      TIMESTAMP,
    aud_updated_date                    DATE,
    aud_job                             VARCHAR(200)
);

-- =============================================================================
-- Indexes for common query patterns
-- =============================================================================

-- Primary lookup
CREATE INDEX idx_lk_leads_v2_lead_id ON lk_leads_v2 (lead_id);

-- User/Agent lookups
CREATE INDEX idx_lk_leads_v2_user_id ON lk_leads_v2 (user_id);
CREATE INDEX idx_lk_leads_v2_agent_id ON lk_leads_v2 (agent_id);

-- Sector filtering
CREATE INDEX idx_lk_leads_v2_spot_sector_id ON lk_leads_v2 (spot_sector_id);

-- Date-based queries
CREATE INDEX idx_lk_leads_v2_created_date ON lk_leads_v2 (lead_created_date);
CREATE INDEX idx_lk_leads_v2_type_date_start ON lk_leads_v2 (lead_type_date_start);

-- Funnel relevance filtering
CREATE INDEX idx_lk_leads_v2_funnel_relevant ON lk_leads_v2 (lead_project_funnel_relevant_id);

-- LDS relevance filtering
CREATE INDEX idx_lk_leads_v2_lds_relevant ON lk_leads_v2 (lead_lds_relevant_id);

-- Status filtering
CREATE INDEX idx_lk_leads_v2_status_id ON lk_leads_v2 (lead_status_id);

-- Advanced project lookup
CREATE INDEX idx_lk_leads_v2_adv_project_id ON lk_leads_v2 (lead_adv_project_id);

-- Recent project lookup
CREATE INDEX idx_lk_leads_v2_rec_project_id ON lk_leads_v2 (lead_rec_project_id);

-- =============================================================================
-- Comments
-- =============================================================================
COMMENT ON TABLE lk_leads_v2 IS 'Gold layer: Leads with project count and funnel enrichment. Source: gold_lk_leads asset.';

-- Max type fields
COMMENT ON COLUMN lk_leads_v2.lead_max_type_id IS 'Numeric ID of max lead type (0-4, 99=Others)';
COMMENT ON COLUMN lk_leads_v2.lead_max_type IS 'Max lead type reached (L0-L4, Others)';

-- Campaign type fields
COMMENT ON COLUMN lk_leads_v2.lead_campaign_type_id IS 'Campaign type: 0=Paid, 1=Organic';
COMMENT ON COLUMN lk_leads_v2.lead_campaign_type IS 'Campaign type label: Paid or Organic';

-- Funnel relevance fields
COMMENT ON COLUMN lk_leads_v2.lead_project_funnel_relevant_id IS 'Funnel relevance: 1=Yes, 0=No';
COMMENT ON COLUMN lk_leads_v2.lead_project_funnel_relevant IS 'Funnel relevance label: Yes or No';

-- LDS relevance fields (for BT_LDS_LEAD_SPOTS)
COMMENT ON COLUMN lk_leads_v2.lead_lds_relevant_id IS 'LDS relevance: 1=Yes, 0=No (has_any_lead_at, not deleted, not spot2 email)';
COMMENT ON COLUMN lk_leads_v2.lead_lds_relevant IS 'LDS relevance label: Yes or No';

-- Type date
COMMENT ON COLUMN lk_leads_v2.lead_type_date_start IS 'Earliest date among lead0_at to lead4_at';
COMMENT ON COLUMN lk_leads_v2.lead_type_datetime_start IS 'Earliest datetime among lead0_at to lead4_at (full timestamp)';

-- Projects count
COMMENT ON COLUMN lk_leads_v2.lead_projects_count IS 'Count of projects associated with this lead';

-- Most advanced project fields
COMMENT ON COLUMN lk_leads_v2.lead_adv_project_id IS 'Project ID of the most advanced project in funnel';
COMMENT ON COLUMN lk_leads_v2.lead_adv_funnel_lead_date IS 'Lead date of the most advanced project';
COMMENT ON COLUMN lk_leads_v2.lead_adv_funnel_visit_created_date IS 'Visit created date of the most advanced project';
COMMENT ON COLUMN lk_leads_v2.lead_adv_funnel_visit_realized_at IS 'Visit realized timestamp of the most advanced project';
COMMENT ON COLUMN lk_leads_v2.lead_adv_funnel_visit_confirmed_at IS 'Visit confirmed timestamp of the most advanced project';
COMMENT ON COLUMN lk_leads_v2.lead_adv_funnel_loi_date IS 'LOI date of the most advanced project';
COMMENT ON COLUMN lk_leads_v2.lead_adv_funnel_contract_date IS 'Contract date of the most advanced project';
COMMENT ON COLUMN lk_leads_v2.lead_adv_funnel_transaction_date IS 'Transaction (won) date of the most advanced project';
COMMENT ON COLUMN lk_leads_v2.lead_adv_funnel_flow IS 'Flow attribution of the most advanced project';

-- Most recent project fields
COMMENT ON COLUMN lk_leads_v2.lead_rec_project_id IS 'Project ID of the most recently created project';
COMMENT ON COLUMN lk_leads_v2.lead_rec_funnel_lead_date IS 'Lead date of the most recent project';
COMMENT ON COLUMN lk_leads_v2.lead_rec_funnel_visit_created_date IS 'Visit created date of the most recent project';
COMMENT ON COLUMN lk_leads_v2.lead_rec_funnel_visit_realized_at IS 'Visit realized timestamp of the most recent project';
COMMENT ON COLUMN lk_leads_v2.lead_rec_funnel_visit_confirmed_at IS 'Visit confirmed timestamp of the most recent project';
COMMENT ON COLUMN lk_leads_v2.lead_rec_funnel_loi_date IS 'LOI date of the most recent project';
COMMENT ON COLUMN lk_leads_v2.lead_rec_funnel_contract_date IS 'Contract date of the most recent project';
COMMENT ON COLUMN lk_leads_v2.lead_rec_funnel_transaction_date IS 'Transaction (won) date of the most recent project';
COMMENT ON COLUMN lk_leads_v2.lead_rec_funnel_flow IS 'Flow attribution of the most recent project';
