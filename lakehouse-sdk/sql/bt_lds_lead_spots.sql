-- ============================================================================
-- Table: bt_lds_lead_spots
-- Description: Event log table for leads, projects, spots, and visits lifecycle
-- Source: gold_bt_lds_lead_spots (Dagster asset)
-- ============================================================================

CREATE TABLE bt_lds_lead_spots (
    -- Core identifiers
    lead_id                             BIGINT NOT NULL,
    project_id                          BIGINT,
    spot_id                             BIGINT,
    appointment_id                      BIGINT,
    apd_id                              BIGINT,
    alg_id                              BIGINT,

    -- Status fields
    appointment_last_date_status_id     BIGINT,
    appointment_last_status_at          TIMESTAMP,  -- Date when last status was assigned
    apd_status_id                       BIGINT,
    alg_event                           VARCHAR(100),

    -- Event fields
    lds_event_id                        SMALLINT NOT NULL,
    lds_event                           VARCHAR(50) NOT NULL,
    lds_event_at                        TIMESTAMP,

    -- Segmentation fields
    lead_max_type_id                    SMALLINT,
    lead_max_type                       VARCHAR(20),
    spot_sector_id                      SMALLINT,
    spot_sector                         VARCHAR(50),
    user_industria_role_id              INTEGER,
    user_industria_role                 VARCHAR(100),

    -- Cohort fields
    lds_cohort_type_id                  SMALLINT,
    lds_cohort_type                     VARCHAR(20),
    lds_cohort_at                       DATE,

    -- Channel attribution fields (only populated for lds_event_id = 3, Spot Added)
    lds_channel_attribution_id          SMALLINT,
    lds_channel_attribution             VARCHAR(30),
    lds_algorithm_evidence_at           TIMESTAMP,
    lds_chatbot_evidence_at             TIMESTAMP,
    is_recommended_algorithm_id         SMALLINT,
    is_recommended_algorithm            VARCHAR(3),
    is_recommended_chatbot_id           SMALLINT,
    is_recommended_chatbot              VARCHAR(3),

    -- Audit fields
    aud_inserted_at                     TIMESTAMP,
    aud_inserted_date                   DATE,
    aud_updated_at                      TIMESTAMP,
    aud_updated_date                    DATE,
    aud_job                             VARCHAR(200)
);

-- Indexes for common query patterns
CREATE INDEX idx_bt_lds_lead_spots_lead_id ON bt_lds_lead_spots(lead_id);
CREATE INDEX idx_bt_lds_lead_spots_project_id ON bt_lds_lead_spots(project_id);
CREATE INDEX idx_bt_lds_lead_spots_spot_id ON bt_lds_lead_spots(spot_id);
CREATE INDEX idx_bt_lds_lead_spots_event_id ON bt_lds_lead_spots(lds_event_id);
CREATE INDEX idx_bt_lds_lead_spots_event_at ON bt_lds_lead_spots(lds_event_at);
CREATE INDEX idx_bt_lds_lead_spots_cohort_at ON bt_lds_lead_spots(lds_cohort_at);
CREATE INDEX idx_bt_lds_lead_spots_channel ON bt_lds_lead_spots(lds_channel_attribution_id);

-- Comments
COMMENT ON TABLE bt_lds_lead_spots IS 'Event log table for leads, projects, spots, and visits lifecycle';
COMMENT ON COLUMN bt_lds_lead_spots.lead_id IS 'Lead identifier';
COMMENT ON COLUMN bt_lds_lead_spots.project_id IS 'Project identifier (null for lead-level events)';
COMMENT ON COLUMN bt_lds_lead_spots.spot_id IS 'Spot identifier (null for lead/project events)';
COMMENT ON COLUMN bt_lds_lead_spots.appointment_id IS 'Appointment identifier (null for non-visit events)';
COMMENT ON COLUMN bt_lds_lead_spots.apd_id IS 'Appointment date ID (null until confirmed/realized events)';
COMMENT ON COLUMN bt_lds_lead_spots.alg_id IS 'Activity log ID (null until LOI/contract events)';
COMMENT ON COLUMN bt_lds_lead_spots.appointment_last_date_status_id IS 'Last status ID of appointment';
COMMENT ON COLUMN bt_lds_lead_spots.appointment_last_status_at IS 'Date when last status was assigned (MIN of updated_at from appointments and appointment_dates)';
COMMENT ON COLUMN bt_lds_lead_spots.apd_status_id IS 'Status ID of specific appointment date';
COMMENT ON COLUMN bt_lds_lead_spots.alg_event IS 'Activity log event name (null until LOI/contract events)';
COMMENT ON COLUMN bt_lds_lead_spots.lds_event_id IS 'Event type ID (1=Lead Created, 2=Project Created, 3=Spot Added, 4=Visit Created, 5=Visit Confirmed, 6=Visit Realized, 7=LOI, 8=Contract, 9=Spot Won, 10=Transaction)';
COMMENT ON COLUMN bt_lds_lead_spots.lds_event IS 'Event type description';
COMMENT ON COLUMN bt_lds_lead_spots.lds_event_at IS 'Event timestamp';
COMMENT ON COLUMN bt_lds_lead_spots.lead_max_type_id IS 'Maximum lead type ID (1-4, 99)';
COMMENT ON COLUMN bt_lds_lead_spots.lead_max_type IS 'Maximum lead type (L1, L2, L3, L4, Others)';
COMMENT ON COLUMN bt_lds_lead_spots.spot_sector_id IS 'Spot sector ID';
COMMENT ON COLUMN bt_lds_lead_spots.spot_sector IS 'Spot sector (Industrial, Retail, Office, Flex)';
COMMENT ON COLUMN bt_lds_lead_spots.user_industria_role_id IS 'Industry role ID';
COMMENT ON COLUMN bt_lds_lead_spots.user_industria_role IS 'Industry role (Tenant, Landlord, Broker, Developer, Unknown)';
COMMENT ON COLUMN bt_lds_lead_spots.lds_cohort_type_id IS '1 = new (project within 30 days of lead), 2 = reactivated';
COMMENT ON COLUMN bt_lds_lead_spots.lds_cohort_type IS 'New or Reactivated';
COMMENT ON COLUMN bt_lds_lead_spots.lds_cohort_at IS 'Cohort date (lead date if new, project date if reactivated)';
COMMENT ON COLUMN bt_lds_lead_spots.lds_channel_attribution_id IS 'Channel attribution ID: 1=Algorithm, 2=Chatbot, 3=Manual. Only for lds_event_id=3';
COMMENT ON COLUMN bt_lds_lead_spots.lds_channel_attribution IS 'Channel attribution: Algorithm, Chatbot, Manual. Only for lds_event_id=3';
COMMENT ON COLUMN bt_lds_lead_spots.lds_algorithm_evidence_at IS 'Timestamp of algorithm recommendation evidence (recommendation_projects.updated_at)';
COMMENT ON COLUMN bt_lds_lead_spots.lds_chatbot_evidence_at IS 'Timestamp of chatbot confirmation evidence (conversation_events.created_at)';
COMMENT ON COLUMN bt_lds_lead_spots.is_recommended_algorithm_id IS '1 if spot appears in any recommendation_projects list, 0 otherwise. Only for lds_event_id=3';
COMMENT ON COLUMN bt_lds_lead_spots.is_recommended_algorithm IS 'Yes/No if spot appears in any recommendation_projects list. Only for lds_event_id=3';
COMMENT ON COLUMN bt_lds_lead_spots.is_recommended_chatbot_id IS '1 if spot appears in any recommendation_chatbot list, 0 otherwise. Only for lds_event_id=3';
COMMENT ON COLUMN bt_lds_lead_spots.is_recommended_chatbot IS 'Yes/No if spot appears in any recommendation_chatbot list. Only for lds_event_id=3';
