SELECT a11.project_id, project_created_date, a12.lead_id ,

 CAST(LEAST(
            COALESCE(lead_lead0_at, '9999-12-31'),
            COALESCE(lead_lead1_at , '9999-12-31'),
            COALESCE(lead_lead2_at , '9999-12-31'),
            COALESCE(lead_lead3_at , '9999-12-31'),
            COALESCE(lead_lead4_at , '9999-12-31')
        ) AS DATE) AS lead_creation_date, a11.spot_sector, a11.lead_max_type
    FROM lk_projects a11 
	    join lk_leads a12
		  on a11.lead_id = a12.lead_id
		 WHERE (lead_l0 OR lead_l1 OR lead_l2 OR lead_l3 OR lead_l4 ) 
      AND (lead_domain NOT IN ('spot2.mx') OR lead_domain IS NULL)  
	
order by project_created_date desc