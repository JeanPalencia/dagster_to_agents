# Gold Tables Schema

Partition: 2026-01-23

================================================================================

## gold_lk_leads_new

* lead_id - Integer
* lead_name - String
* lead_last_name - String
* lead_mothers_last_name - String
* lead_email - String
* lead_domain - String
* lead_phone_indicator - String
* lead_phone_number - String
* lead_full_phone_number - String
* lead_company - String
* lead_position - String
* lead_origin_id - Integer
* lead_origin - String
* lead_l0 - Boolean
* lead_l1 - Boolean
* lead_l2 - Boolean
* lead_l3 - Boolean
* lead_l4 - Boolean
* lead_supply - Boolean
* lead_s0 - Boolean
* lead_s1 - Boolean
* lead_s3 - Boolean
* lead_max_type_id - Integer
* lead_max_type - String
* lead_client_type_id - Integer
* lead_client_type - String
* user_id - Integer
* agent_id - Integer
* lead_agent_by_api - Boolean
* agent_kam_id - Integer
* spot_sector_id - Integer
* spot_sector - String
* lead_status_id - Integer
* lead_status - String
* lead_first_contact_time - Integer
* spot_min_square_space - Float
* spot_max_square_space - Float
* lead_sourse_id - Integer
* lead_sourse - String
* lead_hs_first_conversion - String
* lead_hs_utm_campaign - String
* lead_campaign_type_id - Integer
* lead_campaign_type - String
* lead_hs_analytics_source_data_1 - String
* lead_hs_recent_conversion - String
* lead_hs_recent_conversion_date - DateTime(time_unit='us', time_zone=None)
* lead_hs_industrial_profiling - String
* lead_industrial_profile_state - String
* lead_industrial_profile_zone - String
* lead_industrial_profile_size_id - Integer
* lead_industrial_profile_size - String
* lead_industrial_profile_budget_id - Integer
* lead_industrial_profile_budget - String
* lead_hs_office_profiling - String
* lead_office_profile_state - String
* lead_office_profile_zone - String
* lead_office_profile_size_id - Integer
* lead_office_profile_size - String
* lead_office_profile_budget_id - Integer
* lead_office_profile_budget - String
* lead_project_funnel_relevant_id - Integer
* lead_project_funnel_relevant - String
* lead_lds_relevant_id - Integer
* lead_lds_relevant - String
* lead_projects_count - UInteger
* lead_adv_project_id - Integer
* lead_adv_funnel_lead_date - Date
* lead_adv_funnel_visit_created_date - Date
* lead_adv_funnel_visit_realized_at - DateTime(time_unit='us', time_zone=None)
* lead_adv_funnel_visit_confirmed_at - DateTime(time_unit='us', time_zone=None)
* lead_adv_funnel_loi_date - Date
* lead_adv_funnel_contract_date - Date
* lead_adv_funnel_transaction_date - Date
* lead_adv_funnel_flow - String
* lead_rec_project_id - Integer
* lead_rec_funnel_lead_date - Date
* lead_rec_funnel_visit_created_date - Date
* lead_rec_funnel_visit_realized_at - DateTime(time_unit='us', time_zone=None)
* lead_rec_funnel_visit_confirmed_at - DateTime(time_unit='us', time_zone=None)
* lead_rec_funnel_loi_date - Date
* lead_rec_funnel_contract_date - Date
* lead_rec_funnel_transaction_date - Date
* lead_rec_funnel_flow - String
* lead_lead0_at - DateTime(time_unit='us', time_zone=None)
* lead_lead1_at - DateTime(time_unit='us', time_zone=None)
* lead_lead2_at - DateTime(time_unit='us', time_zone=None)
* lead_lead3_at - DateTime(time_unit='us', time_zone=None)
* lead_lead4_at - DateTime(time_unit='us', time_zone=None)
* lead_supply_at - DateTime(time_unit='us', time_zone=None)
* lead_supply0_at - DateTime(time_unit='us', time_zone=None)
* lead_supply1_at - DateTime(time_unit='us', time_zone=None)
* lead_supply3_at - DateTime(time_unit='us', time_zone=None)
* lead_client_at - DateTime(time_unit='us', time_zone=None)
* lead_type_datetime_start - DateTime(time_unit='us', time_zone=None)
* lead_type_date_start - Date
* lead_profiling_completed_at - DateTime(time_unit='us', time_zone=None)
* lead_created_at - DateTime(time_unit='us', time_zone=None)
* lead_created_date - Date
* lead_updated_at - DateTime(time_unit='us', time_zone=None)
* lead_updated_date - Date
* lead_deleted_at - DateTime(time_unit='us', time_zone=None)
* lead_deleted_date - Date
* user_pseudo_id - String
* conversion_source - String
* lead_creation_date - DateTime(time_unit='us', time_zone=None)
* aud_inserted_at - DateTime(time_unit='us', time_zone=None)
* aud_inserted_date - Date
* aud_updated_at - DateTime(time_unit='us', time_zone=None)
* aud_updated_date - Date
* aud_job - String

## gold_lk_projects_new

* project_id - Integer
* project_name - String
* lead_id - Integer
* spot_sector - String
* lead_max_type - String
* lead_campaign_type - String
* lead_profiling_completed_at - DateTime(time_unit='us', time_zone=None)
* lead_project_funnel_relevant_id - Integer
* user_id - Integer
* user_industria_role_id - Integer
* user_industria_role - String
* project_spot_type_id - Integer
* project_spot_sector_id - Integer
* project_min_square_space - Float
* project_max_square_space - Float
* project_currency_id - Integer
* spot_currency_sale_id - Integer
* spot_currency_sale - String
* spot_price_sqm - Integer
* project_min_rent_price - Float
* project_max_rent_price - Float
* project_min_sale_price - Float
* project_max_sale_price - Float
* spot_price_sqm_mxn_sale_min - Float
* spot_price_sqm_mxn_sale_max - Float
* spot_price_sqm_mxn_rent_min - Float
* spot_price_sqm_mxn_rent_max - Float
* project_rent_months - Integer
* project_commission - Float
* project_last_spot_stage_id - Integer
* project_last_spot_stage - String
* project_enable_id - Boolean
* project_enable - String
* project_disable_reason_id - Integer
* project_disable_reason - String
* project_funnel_relevant_id - Integer
* project_funnel_relevant - String
* project_won_date - Date
* project_funnel_lead_date - Date
* project_funnel_visit_created_date - Date
* project_funnel_visit_created_per_project_date - Date
* project_funnel_visit_status - String
* project_funnel_visit_realized_at - DateTime(time_unit='us', time_zone=None)
* project_funnel_visit_realized_per_project_date - Date
* project_funnel_visit_confirmed_at - DateTime(time_unit='us', time_zone=None)
* project_funnel_visit_confirmed_per_project_date - Date
* project_funnel_loi_date - Date
* project_funnel_loi_per_project_date - Date
* project_funnel_contract_date - Date
* project_funnel_contract_per_project_date - Date
* project_funnel_transaction_date - Date
* project_funnel_transaction_per_project_date - Date
* project_funnel_flow - String
* project_funnel_events - List(String)
* project_created_at - DateTime(time_unit='us', time_zone=None)
* project_created_date - Date
* project_updated_at - DateTime(time_unit='us', time_zone=None)
* project_updated_date - Date
* creation_date - DateTime(time_unit='us', time_zone=None)
* status - String
* aud_inserted_at - DateTime(time_unit='us', time_zone=None)
* aud_inserted_date - Date
* aud_updated_at - DateTime(time_unit='us', time_zone=None)
* aud_updated_date - Date
* aud_job - String

## gold_lk_users_new

* user_id - Integer
* user_uuid - String
* user_name - String
* user_last_name - String
* user_mothers_last_name - String
* user_full_name - String
* user_email - String
* user_domain - String
* user_phone_indicator - String
* user_phone_number - String
* user_phone_full_number - String
* user_has_whatsapp - Boolean
* user_profile_id - Integer
* user_owner_id - Integer
* contact_id - Integer
* user_hubspot_owner_id - Integer
* user_hubspot_user_id - Integer
* user_person_type_id - Integer
* user_person_type - String
* user_max_role_id - Boolean
* user_max_role - String
* user_industria_role_id - Integer
* user_industria_role - String
* user_type_id - Integer
* user_type - String
* user_category_id - Integer
* user_category - String
* user_affiliation_id - Integer
* user_affiliation - String
* agent_kam_id - Integer
* agent_kam_full_name - String
* user_is_kam - Integer
* user_industrial_count - Integer
* user_office_count - Integer
* user_retail_count - Integer
* user_land_count - Integer
* user_spot_count - Integer
* user_public_spot_count - Integer
* user_total_industrial_sqm - Float
* user_total_office_sqm - Float
* user_total_retail_sqm - Float
* user_total_land_sqm - Float
* user_total_spot_sqm - Float
* user_first_spot_created_at - DateTime(time_unit='us', time_zone=None)
* user_last_spot_created_at - DateTime(time_unit='us', time_zone=None)
* user_last_spot_updated_at - DateTime(time_unit='us', time_zone=None)
* user_last_spot_activity_at - DateTime(time_unit='us', time_zone=None)
* user_days_since_last_spot_created - Integer
* user_days_since_last_spot_updated - Integer
* user_days_since_last_spot_activity - Integer
* user_last_log_at - DateTime(time_unit='us', time_zone=None)
* user_days_since_last_log - Boolean
* user_total_log_count - Boolean
* user_kyc_status_id - Boolean
* user_kyc_status - String
* user_kyc_created_at - DateTime(time_unit='us', time_zone=None)
* user_created_at - DateTime(time_unit='us', time_zone=None)
* user_created_date - Date
* user_updated_at - DateTime(time_unit='us', time_zone=None)
* user_updated_date - Date
* user_deleted_at - DateTime(time_unit='us', time_zone=None)
* user_deleted_date - Date
* filter - Integer
* aud_inserted_at - DateTime(time_unit='us', time_zone=None)
* aud_inserted_date - Date
* aud_updated_at - DateTime(time_unit='us', time_zone=None)
* aud_updated_date - Date
* aud_job - String

## gold_lk_spots_new

* spot_id - Integer
* spot_link - String
* spot_public_link - String
* spot_parent_status_id - Integer
* spot_parent_status - String
* spot_parent_status_reason_id - Integer
* spot_parent_status_reason - String
* spot_parent_status_full_id - Integer
* spot_parent_status_full - String
* spot_status_id - Integer
* spot_status - String
* spot_status_reason_id - Integer
* spot_status_reason - String
* spot_status_full_id - Integer
* spot_status_full - String
* spot_sector_id - Integer
* spot_sector - String
* spot_type_id - Integer
* spot_type - String
* spot_is_complex - Boolean
* spot_parent_id - Integer
* spot_title - String
* spot_description - String
* spot_address - String
* spot_street - String
* spot_ext_number - String
* spot_int_number - String
* spot_settlement - String
* spot_data_settlement_id - Integer
* spot_data_settlement - String
* spot_municipality_id - Integer
* spot_municipality - String
* spot_data_municipality_id - Integer
* spot_data_municipality - String
* spot_state_id - Integer
* spot_state - String
* spot_data_state - String
* spot_region_id - Integer
* spot_region - String
* spot_corridor_id - Integer
* spot_corridor - String
* spot_nearest_corridor_id - Integer
* spot_nearest_corridor - String
* spot_latitude - Float
* spot_longitude - Float
* spot_zip_code_id - Integer
* spot_zip_code - String
* spot_settlement_type_id - Integer
* spot_settlement_type - String
* spot_settlement_type_en - String
* spot_zone_type_id - Integer
* spot_zone_type - String
* spot_zone_type_en - String
* spot_municipality_zip_code - String
* spot_state_zip_code - String
* spot_listing_id - Integer
* spot_listing_representative_status_id - Integer
* spot_listing_representative_status - String
* spot_listing_status_id - Integer
* spot_listing_status - String
* spot_listing_hierarchy - Integer
* spot_is_listing_id - Integer
* spot_is_listing - String
* spot_area_in_sqm - Float
* spot_modality_id - Integer
* spot_modality - String
* spot_price_area_rent_id - Integer
* spot_price_area_rent - String
* spot_price_area_sale_id - Integer
* spot_price_area_sale - String
* spot_currency_rent_id - Integer
* spot_currency_rent - String
* spot_currency_sale_id - Integer
* spot_currency_sale - String
* spot_price_total_mxn_rent - Float
* spot_price_total_usd_rent - Float
* spot_price_sqm_mxn_rent - Float
* spot_price_sqm_usd_rent - Float
* spot_price_total_mxn_sale - Float
* spot_price_total_usd_sale - Float
* spot_price_sqm_mxn_sale - Float
* spot_price_sqm_usd_sale - Float
* spot_maintenance_cost_mxn - Float
* spot_maintenance_cost_usd - Float
* spot_sub_min_price_total_mxn_rent - Float
* spot_sub_max_price_total_mxn_rent - Float
* spot_sub_mean_price_total_mxn_rent - Float
* spot_sub_min_price_sqm_mxn_rent - Float
* spot_sub_max_price_sqm_mxn_rent - Float
* spot_sub_mean_price_sqm_mxn_rent - Float
* spot_sub_min_price_total_usd_rent - Float
* spot_sub_max_price_total_usd_rent - Float
* spot_sub_mean_price_total_usd_rent - Float
* spot_sub_min_price_sqm_usd_rent - Float
* spot_sub_max_price_sqm_usd_rent - Float
* spot_sub_mean_price_sqm_usd_rent - Float
* spot_sub_min_price_total_mxn_sale - Float
* spot_sub_max_price_total_mxn_sale - Float
* spot_sub_mean_price_total_mxn_sale - Float
* spot_sub_min_price_sqm_mxn_sale - Float
* spot_sub_max_price_sqm_mxn_sale - Float
* spot_sub_mean_price_sqm_mxn_sale - Float
* spot_sub_min_price_total_usd_sale - Float
* spot_sub_max_price_total_usd_sale - Float
* spot_sub_mean_price_total_usd_sale - Float
* spot_sub_min_price_sqm_usd_sale - Float
* spot_sub_max_price_sqm_usd_sale - Float
* spot_sub_mean_price_sqm_usd_sale - Float
* contact_id - Integer
* contact_email - String
* contact_domain - String
* contact_subgroup_id - Integer
* contact_subgroup - String
* contact_category_id - Integer
* contact_category - String
* contact_company - String
* user_id - Integer
* user_profile_id - Integer
* user_max_role_id - Integer
* user_max_role - String
* user_industria_role_id - Integer
* user_industria_role - String
* user_email - String
* user_domain - String
* user_affiliation_id - Integer
* user_affiliation - String
* user_level_id - Integer
* user_level - String
* user_broker_next_id - Integer
* user_broker_next - String
* spot_external_id - String
* spot_external_updated_at - Date
* spot_quality_control_status_id - Integer
* spot_quality_control_status - String
* spot_photo_count - Integer
* spot_photo_platform_count - Integer
* spot_parent_photo_count - Integer
* spot_parent_photo_platform_count - Integer
* spot_photo_effective_count - Integer
* spot_photo_platform_effective_count - Integer
* spot_class_id - Integer
* spot_class - String
* spot_parking_spaces - Integer
* spot_parking_space_by_area - String
* spot_condition_id - Integer
* spot_condition - String
* spot_construction_date - Date
* spot_score - Float
* spot_exclusive_id - Integer
* spot_exclusive - String
* spot_landlord_exclusive_id - Integer
* spot_landlord_exclusive - String
* spot_is_area_in_range_id - Integer
* spot_is_area_in_range - String
* spot_is_rent_price_in_range_id - Integer
* spot_is_rent_price_in_range - String
* spot_is_sale_price_in_range_id - Integer
* spot_is_sale_price_in_range - String
* spot_is_maintenance_price_in_range_id - Integer
* spot_is_maintenance_price_in_range - String
* spot_origin_id - Integer
* spot_origin - String
* spot_last_active_report_reason_id - Integer
* spot_last_active_report_reason - String
* spot_last_active_report_user_id - Integer
* spot_last_active_report_created_at - DateTime(time_unit='us', time_zone=None)
* spot_has_active_report - Integer
* spot_reports_full_history - String
* spot_created_at - DateTime(time_unit='us', time_zone=None)
* spot_created_date - Date
* spot_updated_at - DateTime(time_unit='us', time_zone=None)
* spot_updated_date - Date
* spot_valid_through - Date
* spot_valid_through_date - Date
* spot_deleted_at - DateTime(time_unit='us', time_zone=None)
* spot_deleted_date - Date
* filter - Integer
* filter_sub - Integer
* aud_inserted_at - DateTime(time_unit='us', time_zone=None)
* aud_inserted_date - Date
* aud_updated_at - DateTime(time_unit='us', time_zone=None)
* aud_updated_date - Date
* aud_job - String

## gold_bt_lds_lead_spots_new

* lead_id - Integer
* project_id - Integer
* spot_id - Integer
* appointment_id - Integer
* apd_id - Integer
* alg_id - Integer
* appointment_last_date_status_id - Integer
* appointment_last_status_at - DateTime(time_unit='us', time_zone=None)
* apd_status_id - Integer
* alg_event - String
* lds_event_id - Integer
* lds_event - String
* lds_event_at - DateTime(time_unit='us', time_zone=None)
* lead_max_type_id - Integer
* lead_max_type - String
* spot_sector_id - Integer
* spot_sector - String
* user_industria_role_id - Integer
* user_industria_role - String
* lds_cohort_type_id - Integer
* lds_cohort_type - String
* lds_cohort_at - Date
* aud_inserted_at - DateTime(time_unit='us', time_zone=None)
* aud_inserted_date - Date
* aud_updated_at - DateTime(time_unit='us', time_zone=None)
* aud_updated_date - Date
* aud_job - String

## gold_bt_conv_conversations_new

* conv_id - String
* lead_phone_number - String
* conv_start_date - DateTime(time_unit='ns', time_zone=None)
* conv_end_date - DateTime(time_unit='ns', time_zone=None)
* conv_text - String
* conv_messages - String
* conv_last_message_date - DateTime(time_unit='ns', time_zone=None)
* lead_id - Integer
* lead_created_at - DateTime(time_unit='us', time_zone=None)
* vis_pseudo_id - String
* spot_id - String
* aud_inserted_at - DateTime(time_unit='us', time_zone=None)
* aud_inserted_date - Date
* aud_updated_at - DateTime(time_unit='us', time_zone=None)
* aud_updated_date - Date
* aud_job - String

## gold_lk_matches_visitors_to_leads_new

* user_pseudo_id - String
* event_datetime_first - DateTime(time_unit='ns', time_zone=None)
* event_name_first - String
* channel_first - String
* match_source - String
* client_id - Float
* conversation_start - DateTime(time_unit='ns', time_zone=None)
* event_datetime - DateTime(time_unit='ns', time_zone=None)
* event_name - String
* channel - String
* source - String
* medium - String
* campaign_name - String
* phone_number - String
* email_clients - String
* lead_min_at - DateTime(time_unit='ns', time_zone=None)
* lead_min_type - String
* lead_type - String
* lead_date - DateTime(time_unit='ns', time_zone=None)
* year_month_first - String
* with_match - String
* aud_inserted_at - DateTime(time_unit='us', time_zone=None)
* aud_inserted_date - Date
* aud_updated_at - DateTime(time_unit='us', time_zone=None)
* aud_updated_date - Date
* aud_job - String