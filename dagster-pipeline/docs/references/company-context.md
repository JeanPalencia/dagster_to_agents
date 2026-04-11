# Contexto de Spot2 - Información de la Compañía

## 🏢 Acerca de Spot2

**Spot2** es el primer **marketplace dedicado 100% al sector inmobiliario comercial en LATAM**.

Reinventamos la manera en que empresas y negocios encuentran y rentan espacios comerciales en todo México.

**Misión:**
Conectar a usuarios con espacios comerciales disponibles de la manera más fácil, rápida y transparente.

**Visión:**
Ser el marketplace inmobiliario donde todas tus necesidades encuentran solución.

**Lema:**
¡Encuentra tu espacio ideal!

---

## 🎯 Enfoque de Negocio

### Segmentos de Propiedad (Spot Sectors)
- **Oficinas (Office)** - Espacios para empresas y profesionales
- **Locales Comerciales (Retail)** - Tiendas, restaurants, servicios
- **Naves Industriales (Industrial)** - Almacenes, manufactura, logística
- **Residential** - Espacios residenciales (secundario)
- **Mixed Use** - Propiedades mixtas

### Geografía
- **Cobertura**: Todo México
- **Expansión**: LATAM
- **Enfoque**: Ciudades principales y mercados emergentes

---

## 👥 Usuarios de la Plataforma

### Lado de Demanda (Compradores/Rentadores)
- **Pequeñas y medianas empresas (PYMEs)**
- **Startups y emprendedores**
- **Empresas grandes**
- **Individuos que buscan locales comerciales**

### Lado de Oferta (Vendedores/Arrendadores)
- **Dueños de propiedades**
- **Inmobiliarios**
- **Desarrolladores**
- **Agentes comerciales**

---

## 💰 Modelo de Negocio

### Cómo Genera Ingresos Spot2
1. **Comisiones por transacción** - % sobre el valor del contrato
2. **Suscripciones premium** - Para anunciantes (publicar más spots)
3. **Servicios adicionales** - Validación, certificación, mediación
4. **Publicidad** - Spots destacados, top listings

### Valor Propuesto
- **Para compradores**: Transparencia, rapidez, variedad
- **Para vendedores**: Alcance, exposición, facilidad
- **Para ambos**: Plataforma digital confiable y segura

---

## 📊 Métricas de Negocio Clave

### Lead Level Definitions (L0-L4)
```
L0 → Creación de cuenta - El lead se registró
L1 → Landing profiling, consultoría o click en modal 100 segundos - Completó formulario de requerimientos
L2 → Spot Interested - Expresó interés en un espacio
L3 → Outbound contact - Lead contactado proactivamente por ventas
L4 → Treble source - Lead originado desde Trebble
```

### Funnel de Conversión Principal
```
Leads
  ↓
Proyectos (busquedas)
  ↓
Solicitud de Visita (Request Visit)
  ↓
Visita Confirmada (Confirmed Visit)
  ↓
Visita Completada (Completed Visit)
  ↓
LOI - Carta de Intención (Letter of Intent)
  ↓
Contrato (Contract)
  ↓
Won - Transacción Cerrada
```

### KPIs Principales a Monitorear
| KPI | Descripción | Importancia |
|-----|-------------|------------|
| **Leads** | Nuevos usuarios en plataforma | Alta |
| **Conversion Rate** | % de leads que avanzan en funnel | Alta |
| **Time to LOI** | Días de lead a LOI | Alta |
| **Completed Visits** | Visitas realizadas sobre solicitadas | Alta |
| **Won Rate** | % de LOIs que cierran en contrato | Alta |
| **Sector Performance** | Performance por tipo de propiedad | Media |
| **Geographic Distribution** | Distribución de leads por región | Media |
| **User Retention** | % de leads que regresan | Media |

---

## 🔄 Ciclo de Vida del Lead

### Etapas Principales

#### Etapa 1: Discovery (L0-L1)
- Usuario llega a plataforma
- Explora espacios disponibles
- Filtra por sector, ubicación, precio

#### Etapa 2: Consideration (L2)
- Guarda favoritos
- Lee detalles de espacios
- Compara opciones
- Revisa fotos y características

#### Etapa 3: Interest (L3)
- Solicita visita
- Completa perfil
- Proporciona información de contacto

#### Etapa 4: Action (L4)
- Realiza visita
- Negocia términos
- Emite LOI (Carta de Intención)
- Firma contrato

#### Etapa 5: Completion (Won)
- Transacción cerrada
- Lead clasificado como "ganado"

---

## 📈 Métricas por Sector

### Office (Oficinas)
- **Ticket promedio**: Más alto
- **Ciclo de venta**: Más largo
- **Compradores típicos**: Empresas establecidas

### Retail (Locales Comerciales)
- **Ticket promedio**: Medio
- **Ciclo de venta**: Medio
- **Compradores típicos**: Emprendedores, pequeños negocios

### Industrial (Naves)
- **Ticket promedio**: Muy alto
- **Ciclo de venta**: Más largo
- **Compradores típicos**: Empresas manufactureras, logística

---

## 🎯 Objetivos de Negocio

### Corto Plazo (0-6 meses)
- Aumentar volumen de leads
- Mejorar tasa de conversión en etapas tempranas
- Expandir cobertura geográfica

### Mediano Plazo (6-12 meses)
- Aumentar ticket promedio
- Mejorar retention de leads
- Optimizar ciclo de venta

### Largo Plazo (1+ años)
- Expansión a otros países LATAM
- Verticales adicionales (residencial)
- Servicios financieros integrados

---

## 🔍 Filtros y Exclusiones Estándar

### Datos a Excluir Siempre
```sql
-- Dominio interno de test
WHERE lead_domain NOT IN ('spot2.mx')

-- Leads eliminados (soft delete)
AND lead_deleted_at IS NULL

-- Datos incompletos del día actual
AND date_field <= CURRENT_DATE - INTERVAL '1 day'
```

### Datos a Validar
- Leads de test o desarrollo
- Transacciones incompletas
- Período de análisis correcto

---

## 💡 Contexto para Análisis

### Preguntas Frecuentes del Negocio

**Del CEO/Fundadores:**
- ¿Cómo está el funnel general?
- ¿Qué sector tiene mejor performance?
- ¿Cuáles son nuestros bottlenecks?

**Del Sales Team:**
- ¿Cuántos leads nuevos cada semana?
- ¿Cuál es la tasa de conversión a LOI?
- ¿Cuánto tarda en cerrar un deal?

**Del Marketing Team:**
- ¿De dónde vienen nuestros leads?
- ¿Qué sectores atraen más usuarios?
- ¿Cuál es la retention de usuarios?

**Del Product Team:**
- ¿Dónde se atascan los usuarios?
- ¿Qué features usan más?
- ¿Cuál es el user journey típico?

---

## 📊 Benchmarks Internos

### Performance Esperado (Basado en startup SaaS inmobiliario)

- Los okr's son mensuales y se cargan en la tabla lk_okrs. Ahí estan todos los datos actualizados de 2026 de las principales métricas que son
1. Cantidad de Leads
2. Cantidad de Proyectos
3. Cantidad de visitas agendadas
4. Cantidad de visitas confirmadas
5. Cantidad de visitas completadas
6. Cantidad de LOIs 

---

## 🌍 Competencia y Mercado

### Diferenciadores de Spot2
1. **100% enfocado en comercial** - No mezcla residencial
2. **LATAM-first** - Mexico
3. **User-friendly** - Plataforma moderna y transparente
4. **Marketplace modelo** - Conecta comprador-vendedor directo

### Oportunidades
- Expansión a otros países
- Servicios financieros
- Datos y analytics
- Automatización

---

## 📱 Tecnología y Plataforma

### Stack Actual
- **Database**: PostgreSQL (Spot2)
- **Analytics**: Metabase
- **Lenguajes**: Python
- **Infrastructure**: AWS

### Datos Disponibles
- **Históricos**: Desde enero 2025 
- **Granularidad**: Lead y Project level
- **Actualizaciones**: Real-time o batch

---

## 🎓 Diccionario de Términos Spot2

| Término | Definición |
|---------|-----------|
| **Lead** | Usuario que se registra en la plataforma |
| **Spot** | Propiedad/espacio comercial disponible |
| **Project** | Búsqueda activa de un lead por un spot |
| **LOI** | Carta de Intención - Documento legal preliminar |
| **Won** | Transacción cerrada - Lead y vendedor llegaron a acuerdo |
| **Sector** | Tipo de propiedad (Office, Retail, Industrial, Land) |
| **L0-L4** | Niveles de engagement del lead |
| **Rolling** | Actividad en período específico |
| **Cohort** | Segmento de Leads agrupados por fecha de creación o por la cración de un nuevo proyecto tras 30 días de su fecha de creación. |

---

## 🔗 Relación con Datos

Toda esta información de negocio se refleja en estas tablas:

- **lk_leads**: Usuarios con sus L0-L4 states. Reflejan la forma en que se adquirió ese lead.
      L0 → Creación de cuenta - El lead se registró
      L1 → Landing profiling, consultoría o click en modal 100 segundos - Completó formulario de requerimientos
      L2 → Spot Interested - Expresó interés en un espacio. Ejemplo: https://spot2.mx/spots/oficina-en-renta-granada-miguel-hidalgo-423/32673
      L3 → Outbound contact - Lead contactado proactivamente por ventas
      L4 → Treble source - Lead originado desde Trebble
- **lk_projects**: Un proyecto es cuando un Lead en base a listado de requerimientos, ya tiene posibles spots para visitar. La información de los proyectos se pueden dar seguimiento en el CRM. Ejemplo:  https://platform.spot2.mx/admin/v1/proyectos
- **lk_users**: Listado de Usuarios. Los usuarios pueden ser tanto de demanda, por ejemplo un lead que se creó una cuenta (L0), internos @spot2, @crex (agentes afiliados), @next (franquicias) ó supply (usuarios que suben sus spots a la plataforma) 
- **lk_spots**: Son todos los spots publicados en la plataforma.
- **bt_lds_lead_spots**: Esta tabla guarda los eventos de un leads con los spots que va a agregando a su proyecto. Desde que se crea un lead, hasta cuando crea un proyecto, agrega un spot, lo visita y termina generando un LOI.
- **bt_conv_conversations**: El servicio al cliente tiene un chatbot. En esta tabla se guardan las principales métricas correspondiente a esas conversaciones.
- **Dimensiones**: sector, lead_max_type, fecha

---

## 🔀 Supply vs Demand - Funnels Diferentes

### Tipo 1: DEMAND (Buscadores)
**Definición**: Leads que buscan rentar/comprar espacios comerciales
**Funnel**: Lead → Project → Visit → LOI → Contract → Won
**Duración ciclo**: Multiple weeks to months (varies by deal)
**Conversión global**: Calculate with Query 8 (Data Validation)
**Clave**: Estos leads SÍ generan revenue

### Tipo 2: SUPPLY (Publicadores)
**Definición**: Dueños/agentes que quieren publicar sus propiedades
**Funnel**: Lead (Sign-up) → User Account → Publicar Spots → Manage Active Listings
**Duración ciclo**: Ongoing (indefinido)
**Conversión**: N/A (no es deal cerrado)
**Clave**: Estos leads NO generan transacciones, generan inventory

**IMPORTANTE**: Nunca mezcles Supply + Demand en análisis de conversión. Filtrar siempre por `lead_supply = 0` para demanda.

---

## 📞 Contactos Clave

- **Data Platform Engineer**: Estefanía Louzau, José Castellanos y Luciano Paccione
- **Documentación**: Skills específicos de Spot2

---

**Última actualización**: 2026-02-24
**Versión**: 1.1.0
**Cambios**: Lead Level definitions actualizado (L0-L4) con definiciones exactas de negocio
