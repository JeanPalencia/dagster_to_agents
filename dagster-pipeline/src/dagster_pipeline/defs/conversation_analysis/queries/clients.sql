-- Query for fetching clients data from MySQL (Prod DB)
-- Returns: lead_id, phone_number, fecha_creacion_cliente

SELECT 
    id as lead_id, 
    phone_number, 
    created_at as fecha_creacion_cliente 
FROM clients 
ORDER BY id, created_at DESC

