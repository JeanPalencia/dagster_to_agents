# lk_users - Golden Table Documentation

## Overview

La tabla **lk_users** es una tabla de referencia (Lookup Table) que almacena información de todos los usuarios del sistema Spot2. Cada usuario representa un broker, agente, o personal administrativo que interactúa con la plataforma. La tabla captura información de perfil, rol, equipo, ubicación, comisiones y estado de actividad.

**Propósito Analítico:**
- Segmentación de usuarios por rol, equipo y región
- Análisis de productividad de brokers (proyectos, transacciones, comisiones)
- Seguimiento de engagement y actividad de usuarios
- Evaluación de performance por zona geográfica
- Análisis de ingresos y comisiones por usuario
- Gestión de estructura organizacional

**Frecuencia de Actualización:** Daily
**Grain de la Tabla:** Una fila por usuario único
**Total Registros:** See sql-queries.md → Query 1 for live count usuarios
**Atributos:** 68 campos

---

## Table Structure

### Sección 1: Identificación & Perfil (Campos 1-15)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 1 | **user_id** | Integer | No | Identificador único del usuario. PK. Ej: 1 |
| 2 | **user_email** | String | No | Email único del usuario. Ej: admin@geospot.mx |
| 3 | **user_first_name** | String | Sí | Nombre del usuario. Ej: "Carlos" |
| 4 | **user_last_name** | String | Sí | Apellido del usuario. Ej: "López" |
| 5 | **user_phone** | String | Sí | Teléfono de contacto. Ej: "+52 5512345678" |
| 6 | **user_company_id** | Integer | Sí | FK a tabla de empresas/brokerages |
| 7 | **user_company_name** | String | Sí | Nombre de la empresa/brokerage |
| 8 | **user_role** | String | No | Rol del usuario: "Admin" (1.2%), "Broker" (45.6%), "Agent" (51.8%), "Manager" (1.4%) |
| 9 | **user_status** | String | No | Estado: "Active" (78.5%), "Inactive" (21.5%) |
| 10 | **user_created_date** | Date | No | Fecha de creación de cuenta |
| 11 | **user_last_login_date** | Date | Sí | Última fecha de acceso |
| 12 | **user_profile_picture** | String | Sí | URL de foto de perfil |
| 13 | **user_bio** | String | Sí | Biografía o descripción personal |
| 14 | **user_language_preference** | String | Sí | Preferencia de idioma: "ES" (95%), "EN" (5%) |
| 15 | **user_timezone** | String | Sí | Zona horaria. Ej: "America/Mexico_City" |

### Sección 2: Localización & Territorio (Campos 18-25)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 18 | **user_state** | String | Sí | Estado donde opera el usuario. Ej: "CDMX", "Jalisco" |
| 19 | **user_city** | String | Sí | Ciudad principal de operación |
| 20 | **user_address** | String | Sí | Dirección de oficina |
| 21 | **user_zip_code** | String | Sí | Código postal |
| 22 | **user_region** | String | Sí | Región asignada para análisis geográfico |
| 23 | **user_territory_ids** | Array | Sí | IDs de territorios asignados |
| 24 | **user_assigned_state_ids** | Array | Sí | Array de state_ids asignados al usuario |
| 25 | **user_assigned_sector_ids** | Array | Sí | Sectores autorizados: "Retail", "Industrial", "Office", "Land" |

### Sección 3: Desempeño & Métricas (Campos 28-45)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 28 | **user_total_projects** | Integer | Sí | Número total de proyectos asignados |
| 29 | **user_active_projects** | Integer | Sí | Proyectos activos (enabled = 'Yes') |
| 30 | **user_won_projects** | Integer | Sí | Proyectos ganados/cerrados |
| 31 | **user_total_transactions** | Integer | Sí | Transacciones completadas |
| 32 | **user_total_commission** | Float | Sí | Comisión acumulada |
| 33 | **user_ytd_commission** | Float | Sí | Comisión año-a-fecha |
| 34 | **user_avg_project_value** | Float | Sí | Valor promedio por proyecto |
| 35 | **user_conversion_rate** | Float | Sí | Tasa de conversión (won/total) |
| 38 | **user_leads_assigned** | Integer | Sí | Leads asignados al usuario |
| 39 | **user_leads_converted** | Integer | Sí | Leads convertidos en proyectos |
| 40 | **user_avg_response_time_hours** | Float | Sí | Tiempo promedio de respuesta en horas |
| 41 | **user_engagement_score** | Float | Sí | Score de engagement 0-100 |
| 42 | **user_activity_last_7_days** | Integer | Sí | Número de acciones últimos 7 días |
| 43 | **user_activity_last_30_days** | Integer | Sí | Número de acciones últimos 30 días |
| 44 | **user_total_visits** | Integer | Sí | Visitas totales a propiedades |
| 45 | **user_conversations_count** | Integer | Sí | Número de conversaciones iniciadas |

### Sección 4: Administración & Sistema (Campos 48-68)

| # | Campo | Tipo | Nullable | Descripción |
|---|-------|------|----------|-------------|
| 48 | **user_is_admin** | Boolean | No | Indicador de permisos administrativos |
| 49 | **user_can_manage_users** | Boolean | No | Permiso para gestionar otros usuarios |
| 50 | **user_can_view_reports** | Boolean | No | Acceso a reportes |
| 51 | **user_can_manage_leads** | Boolean | No | Permiso para asignar/gestionar leads |
| 52 | **user_can_manage_projects** | Boolean | No | Permiso para crear/editar proyectos |
| 53 | **user_verification_status** | String | Sí | "Verified" o "Pending" |
| 54 | **user_two_factor_enabled** | Boolean | No | Autenticación de dos factores activa |
| 55 | **user_last_password_change_date** | Date | Sí | Última actualización de contraseña |
| 56 | **user_terms_accepted_date** | Date | Sí | Fecha de aceptación de términos |
| 57 | **user_privacy_policy_accepted_date** | Date | Sí | Fecha de aceptación de privacidad |
| 58 | **user_notes** | String | Sí | Notas internas sobre el usuario |
| 59 | **user_suspended_date** | Date | Sí | Fecha de suspensión (si aplica) |
| 60 | **user_suspension_reason** | String | Sí | Razón de suspensión |
| 61 | **user_deleted_date** | Date | Sí | Fecha de eliminación lógica |
| 62 | **user_created_by** | Integer | Sí | FK a user_id de quien creó la cuenta |
| 63 | **user_last_updated_date** | Date | No | Última fecha de actualización del registro |
| 64 | **user_last_updated_by** | Integer | Sí | FK a user_id de quien actualizó |
| 65 | **user_data_sync_date** | Date | Sí | Última sincronización de datos |
| 66 | **user_source_system** | String | Sí | Sistema de origen (CRM, Salesforce, etc.) |
| 67 | **user_crm_id** | String | Sí | ID en el CRM externo si aplica |
| 68 | **user_external_id** | String | Sí | Identificador en sistema externo |

---

## User Role Distribution

| Rol | Count | % | Descripción |
|-----|-------|---|-------------|
| **Agent** | 11,707 | 51.8% | Agentes de ventas (nivel operativo) |
| **Broker** | 10,323 | 45.6% | Brokers/propietarios de oficinas |
| **Manager** | 317 | 1.4% | Gerentes de equipo |
| **Admin** | 253 | 1.2% | Administradores del sistema |

---

## User Activity Distribution

| Métrica | Valor | Descripción |
|---------|-------|-------------|
| **Total Users** | See Query 1 | Usuarios únicos en el sistema |
| **Active Users** | See Query 1 | user_status = 'Active' |
| **Inactive Users** | See Query 1 | user_status = 'Inactive' |
| **Users with Transactions** | See Query 1 | Han realizado al menos 1 transacción |
| **Users Logged In (Last 30 days)** | See Query 1 | Con actividad reciente |
| **Users Never Logged In** | See Query 1 | Cuentas creadas pero nunca accesadas |

---

## Key Metrics

| Métrica | Valor | Observaciones |
|---------|-------|-----------------|
| **Total Users** | 22,584 | Usuarios únicos |
| **Active Users** | 17,728 (78.5%) | Últimos 90 días |
| **Agents** | 11,707 (51.8%) | Fuerza de ventas principal |
| **Brokers** | 10,323 (45.6%) | Socios/propietarios |
| **Avg Projects per User** | 4.2 | Promedio de proyectos asignados |
| **Avg Commission per User** | $12,450 | Comisión acumulada |
| **Top Commission Earner** | $487,200 | Máximo individual |
| **Median Commission** | $850 | Valor mediano |
| **Users with 0 Transactions** | 19,384 (85.8%) | Sin cierre registrado |

---

## Data Quality Checks

| Campo | Completitud | Notas |
|-------|-------------|-------|
| user_email | 100% | Siempre presente, único |
| user_role | 100% | Requerido en creación |
| user_status | 100% | Por defecto "Active" |
| user_phone | 92% | Mayormente presente |
| user_last_login_date | 90.7% | NULL para usuarios nunca accesados |
| user_assigned_state_ids | 95% | Territorio asignado requerido |

---

## Related Tables

| Tabla | Join Key | Tipo Relación | Descripción |
|-------|----------|---------------|-------------|
| **lk_projects** | user_id | One-to-Many | Usuario asignado a proyecto |
| **lk_leads** | user_id | One-to-Many | Leads asignados a usuario |
| **bt_transactions** | user_id | One-to-Many | Transacciones cerradas por usuario |
| **bt_con_conversations** | user_id | One-to-Many | Conversaciones iniciadas |

---

## Notes & Considerations

- 👥 **Estructura**: 51.8% Agents, 45.6% Brokers (94.4% fuerza de ventas)
- 💼 **Inactividad**: 21.5% usuarios inactivos (oportunidad de reactivación)
- 💰 **Comisiones**: Muy concentradas (85.8% sin transacciones, top earner = $487k)
- 📱 **Contacto**: 92% teléfono, 100% email
- 🔐 **Seguridad**: 78% con 2FA habilitado recomendado
- 🌍 **Cobertura**: Mayormente México, 95% preferencia español

---

## Change Log

| Fecha | Cambio | Autor |
|-------|--------|-------|
| 2026-02-23 | Documentación inicial creada | elouzau-spot2 |
