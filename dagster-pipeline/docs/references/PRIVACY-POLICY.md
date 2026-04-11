# 🔐 Política de Privacidad - Spot2 Data Skills

**Última actualización**: 2026-02-05

---

## 📋 Propósito

Este documento establece guías estrictas para proteger la privacidad de usuarios Spot2 al trabajar con datos en análisis, dashboards y documentación.

---

## 🚫 Datos PROHIBIDOS de Exponer

### Categoría 1: Información de Contacto (🔴 MÁXIMA RESTRICCIÓN)

```
❌ Teléfono:           lead_phone_number, user_phone, contact_phone
❌ Email:              user_email, lead_email, email_personal
❌ WhatsApp/Telegram:  messaging_contacts, social_handles
❌ Dirección exacta:   street_address, exact_location, home_address
```

**Qué Hacer**:
- No incluir en dashboards públicos
- No mostrar en reportes compartidos
- No loguear en documentación
- Usar `[REDACTED]` o `[PLACEHOLDER]` en ejemplos

**Ejemplo CORRECTO**:
```json
{
  "lead_id": "****12345",
  "lead_phone": "[REDACTED_SENSITIVE_DATA]",
  "conv_start_date": "2025-01-10 16:30:00"
}
```

**Ejemplo INCORRECTO** ❌:
```json
{
  "lead_id": "12345",
  "lead_phone": "+525527370984",
  "email": "usuario@gmail.com"
}
```

### Categoría 2: Identificación Personal (🔴 RESTRICCIÓN ALTA)

```
❌ Nombres completos:     user_name, lead_name, contact_name
❌ Empresa/Negocio:       company_name, business_name (si es identificable)
❌ DNI/RFC/ID Oficial:    identification_number, passport_id
❌ Datos de pago:         credit_card, bank_account, billing_info
```

### Categoría 3: Ubicación Precisa (🔴 RESTRICCIÓN MEDIA-ALTA)

```
❌ Coordenadas exactas:   latitude, longitude (precisión < 1km)
❌ Direcciones completas: full_address, street_number
✅ Región generalizada:   state, municipality (OK - bajo riesgo)
✅ Sector inmobiliario:   spot_sector (Office, Retail - OK)
```

### Categoría 4: Historial/Comportamiento Sensible (🟡 CUIDADO)

```
❌ Transcripciones chat completas:    conv_text con datos personales
❌ Búsquedas específicas:             detailed_search_history con nombres
❌ Preferencias religiosas/políticas: sensitive_preferences
✅ Patrones agregados:                "30% de usuarios buscan Oficinas"
```

### Categoría 5: Identificadores sin Hash (🟡 CUIDADO)

```
❌ user_pseudo_id sin anonimizar:     "860188043.1757558455"
❌ conv_id con datos de usuario:      "525527370984_20260110"
✅ IDs hasheados:                     "****12345"
✅ IDs seudo-anonimizados:            "[pseudo_id_hash]"
```

---

## ✅ Datos SEGUROS de Usar

Estos datos se pueden incluir en dashboards, reportes y documentación pública:

```
✅ Agregaciones (no individuales):
   - "1,230 leads en Sector Office"
   - "45% conversion rate"
   - "Promedio 5 días a LOI"

✅ Dimensiones de negocio:
   - Sector (Office, Retail, Industrial)
   - Tipo usuario (Demand, Supply, Internal)
   - Etapa funnel (Lead, Visit, LOI, Won)
   - Mes/Trimestre (no hora exacta)

✅ Métricas derivadas:
   - Tasas de conversión
   - Velocidad de funnel
   - Tendencias por sector
   - Performance MoM/QoQ

✅ Metadatos públicos:
   - Timestamp general (sin microsegundos)
   - Status público (lead_level, project_status)
   - Conteos agregados
```

---

## 🛡️ Mejores Prácticas por Rol

### Data Analysts

**HACER**:
- ✅ Usar PII redaction en todas las queries
- ✅ Agregar datos antes de visualizar
- ✅ Documentar qué campos son sensibles
- ✅ Usar alias/masking en tablas de trabajo

**NO HACER**:
- ❌ Incluir teléfono/email en SELECT
- ❌ Copiar datos crudos a sheets/archivos
- ❌ Compartir CSVs con PII
- ❌ Loguear datos personales en scripts

**Ejemplo CORRECTO de Query**:
```sql
-- ✅ Análisis de funnel sin PII
SELECT
  sector,
  COUNT(DISTINCT lead_id) as total_leads,
  COUNT(DISTINCT CASE WHEN lead_l2 THEN lead_id END) as l2_leads,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN lead_l2 THEN lead_id END) /
    COUNT(DISTINCT lead_id), 2) as conversion_rate_pct
FROM lk_leads
WHERE lead_domain NOT IN ('spot2.mx')
  AND lead_deleted_at IS NULL
GROUP BY sector
ORDER BY conversion_rate_pct DESC;

-- ❌ NO INCLUIR:
-- lead_phone_number, email, user_name
```

### Dashboard Creators

**HACER**:
- ✅ Mostrar solo métricas agregadas
- ✅ Usar sector/sector como dimensiones
- ✅ Mostrar tendencias, no individuos
- ✅ Restringir acceso a usuarios autorizados

**NO HACER**:
- ❌ Mostrar "Top 10 leads por email"
- ❌ Publicar listas con teléfonos
- ❌ Detallar búsquedas individuales
- ❌ Mostrar historial de conversaciones

**Ejemplo de Dashboard SEGURO**:
```
Título: "Spot2 - Funnel Performance by Sector"

Visualizaciones permitidas:
- Pie: "Total Leads by Sector" (45% Retail, 30% Office, etc)
- Line: "Conversion Rate Trend (L0→L2)" por mes
- Table: "Sector Metrics" (conversión, velocidad)

Visualizaciones PROHIBIDAS ❌:
- "Top 100 Leads List" con teléfonos
- "Email Campaign Performance" detallado
- "Chat Conversations" transcripciones
```

### Data Engineers

**HACER**:
- ✅ Implementar column-level encryption para PII
- ✅ Usar row-level security (RLS)
- ✅ Auditar accesos a datos sensibles
- ✅ Aplicar masking en ambientes no-prod

**NO HACER**:
- ❌ Exponer PII en logs
- ❌ Copiar datos de prod a dev sin redacción
- ❌ Permitir acceso no autorizado
- ❌ Guardar datos sensibles en caché

---

## 📋 Checklist antes de Compartir Datos

Antes de compartir cualquier análisis, dashboard o documento:

- [ ] ¿Incluye teléfono, email o dirección exacta? → REDACTAR
- [ ] ¿Muestra datos individuales identificables? → AGREGAR
- [ ] ¿Contiene nombres personales? → REMOVER
- [ ] ¿Tiene transcripciones completas del chatbot? → REDACTAR
- [ ] ¿Está compartiendo CSVs o exports? → VALIDAR PII
- [ ] ¿Quién tiene acceso? → VERIFICAR PERMISOS
- [ ] ¿Está en documentación pública? → APLICAR MÁXIMAS RESTRICCIONES

---

## 🔍 Cumplimiento Legal

### LGPD (Brasil)
- **Artículo 5**: Dados pessoais do usuário são protegidos
- **Dever de Sigilo**: Manter dados confidenciais
- **Direito de Acesso**: Usuários podem solicitar sus dados

### GDPR (Europa)
- **Right to be Forgotten**: Datos pueden ser eliminados
- **Data Minimization**: Solo usar datos necesarios
- **Consent**: Consentimiento informado requerido

### CCPA (California)
- **Right to Know**: Usuarios pueden saber qué datos hay
- **Right to Delete**: Pueden solicitar eliminación
- **Right to Opt-Out**: Pueden no participar en venta de datos

### México (LFPDPPP)
- **Consentimiento**: Requerido para recopilar datos
- **Aviso de Privacidad**: Informar cómo se usan datos
- **Seguridad**: Implementar medidas de protección

---

## ⚙️ Implementación Técnica

### Masking en SQL

```sql
-- Enmascarar email
SELECT
  SUBSTRING(email, 1, 1) || '****' || SUBSTRING(email FROM POSITION('@' in email)) as email_masked,
  COUNT(*) as count
FROM lk_users
GROUP BY email_masked;

-- Enmascarar teléfono
SELECT
  '****' || SUBSTRING(phone FROM LENGTH(phone)-3 FOR 4) as phone_masked,
  COUNT(*) as count
FROM lk_leads
GROUP BY phone_masked;

-- Hashear ID
SELECT
  MD5(CAST(lead_id AS VARCHAR)) as lead_id_hash,
  COUNT(*) as count
FROM lk_leads
GROUP BY lead_id_hash;
```

### Redacción en Python

```python
import re

# Redactar email
def redact_email(email):
    return re.sub(r'(.{1})(.*)(@.*)$', r'\1****\3', email)

# Redactar teléfono
def redact_phone(phone):
    return '****' + phone[-4:]

# Ejemplos
redact_email("usuario@gmail.com")      # u****@gmail.com
redact_phone("+525527370984")          # ****0984
```

---

## 📞 Reportar Violaciones

Si encuentras que datos sensibles están siendo expuestos:

1. **NO compartas públicamente** la violación
2. **Contacta a**: [data-privacy@spot2.mx]
3. **Describe**: Qué datos, dónde, cuándo
4. **Esperado**: Respuesta en 24 horas

---

## 🎓 Recursos

- [LGPD en Español](https://www.gov.br/cidadania/pt-br/acesso-a-informacao/lgpd)
- [GDPR Compliance](https://gdpr-info.eu/)
- [CCPA Rights](https://oag.ca.gov/privacy/ccpa)
- [LFPDPPP México](https://www.gob.mx/inai)

---

**Responsable**: Data Governance Team
**Última revisión**: 2026-02-05
**Próxima revisión**: 2026-05-05
