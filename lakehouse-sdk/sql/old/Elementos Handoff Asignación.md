# Elementos de atención en Adquisición

1. ## Proceso de match de usuarios anónimos a leads:

   [Repositorio](https://github.com/Spot2HQ/dagster/tree/main/dagster-pipeline/src/dagster_pipeline/defs/lk_matches_visitors_to_leads)  
     
   Este proceso se encarga de por distintas metodologías tratar de encontrar a qué client\_id/lead\_id corresponden los usuarios anónimos que realizan los **eventos de conversión** \[ver nota\]. No todos los usuarios terminan realmente convirtiendo o dejando sus datos así que es normal ver user\_pseudo\_ids sin client\_id asignado. En el momento de creación de este documento, la precisión del algoritmo ronda el 95%, es decir, logra encontrar un usuario anónimo para el 95% de los clientes que se crean por día (teniendo en cuenta su fecha mínima entre lead0\_at,lead1\_at,lead2\_at,lead3\_at,lead4\_at).

2. ## Dash y Modelo de Metabase para flujos y conversiones por canales:

   [Dashboard GrannFunnelGrann](https://metabase.spot2.mx/dashboard/147-granfunnelgran?campaign_name=&channel=&cohort_type=&conversion_point=&date_picker=2026-03-01~2026-03-20&entry_point=&lead_id=&nivel_lead=&tab=156-big-picture&time_grouping=day&tipo_evento=&traffic_type=&user_sospechoso=0)  
     
   En este Dash el equipo monitorea lo relacionado a los canales desde los que llegan los usuarios anónimos y por donde se generan los leads, este dash lo miran bastante y sirve para identificar picos anómalos en días periodos específicos para un canal en especial.  
   Tanto este punto como el anterior se ven influenciado por la forma en la que se “etiquetan” los canales, esa forma está documentada en: [Reglas de asignación de Channels](https://spot2mx.atlassian.net/wiki/spaces/Data/pages/581107713/Reglas+de+Asignaci+n+de+Channels). También la regla aplicada en código se puede encontrar en la consulta programada: [Consulta Programada Funnel With Channel](https://console.cloud.google.com/bigquery/scheduled-queries/locations/us/configs/69d966a8-0000-2cfc-9c4f-582429b844bc/runs?project=spot2-mx-ga4-bq).

   lead0\_at,lead1\_at,lead2\_at,lead3\_at,lead4\_at).

3. ## Atención a comportamientos anómalos:

   Cuando se detectan comportamientos anómalos en algún día referente a un canal en especial (tanto en el dashboard como en los informes/gráficos autogenerados de Google Analytics), se suele tener que acudir a la tabla cruda de eventos de bigquery para inspeccionar en ese día elementos que nos den luces de esos picos o valles según sea el caso. Usualmente revisamos si estamos recibiendo alguna campañas en especial, o hay presencia/ausencia de una combinación de fuente y medio en especial como primer paso. Luego ya nos fijamos en elementos como las page\_locations , navegadores que se usan, duración de las sesiones, si predomina mobile o desktop, si se concentra en un solo usuario anónimo, etc.

   Lo ideal es poder generar un reporte conciso para presentar a Pablo, Marisol y Jorge Moreno sobre los findings que tengamos. 

   Usualmente si tenemos sospecha de scraping, solemos apoyarnos en Tech para ello, con Dante y Fabio.