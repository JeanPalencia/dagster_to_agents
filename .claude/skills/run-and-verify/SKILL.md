---
name: run-and-verify
description: >
  Launches a Dagster job on Railway, polls until completion, then verifies the
  output table in GeoSpot staging PostgreSQL has data. Reports success or failure.
  Trigger: "run the flow", "launch and verify", "disparar el flujo", "ejecutar y verificar",
  "run [job-name] and check", "lanzá el job".
---

## When to Use

When the user wants to:
- Launch a Dagster job and confirm it wrote data to the target table
- Test end-to-end that a flow is working in Railway
- Verify a recent run completed successfully

---

## Configuration (dagster_to_agents project)

- **Dagster UI**: `https://dagster-to-agents-production.up.railway.app`
- **GraphQL endpoint**: `https://dagster-to-agents-production.up.railway.app/graphql`
- **Repository location**: `dagster_pipeline.definitions`
- **Repository name**: `__repository__`
- **GeoSpot staging DB**:
  - host: read from `GEOSPOT_DB_HOST` Railway var (geospot-staging-read-replica.*)
  - user: read from `GEOSPOT_DB_USER` Railway var
  - password: read from `GEOSPOT_DB_PASSWORD` Railway var
  - database: `geospot`
  - port: `5432`

---

## Workflow

### Step 1 — Check code location is LOADED

```bash
curl -s https://dagster-to-agents-production.up.railway.app/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ locationStatusesOrError { ... on WorkspaceLocationStatusEntries { entries { loadStatus } } } }"}'
```

If `loadStatus != "LOADED"`, wait 15s and retry (max 3 times). If still not loaded, abort and report.

### Step 2 — Launch the job

```bash
curl -s https://dagster-to-agents-production.up.railway.app/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation LaunchRun($params: ExecutionParams!) { launchRun(executionParams: $params) { __typename ... on LaunchRunSuccess { run { runId status } } ... on PythonError { message } } }",
    "variables": {
      "params": {
        "selector": {
          "repositoryLocationName": "dagster_pipeline.definitions",
          "repositoryName": "__repository__",
          "jobName": "<JOB_NAME>"
        },
        "executionMetadata": { "tags": [] }
      }
    }
  }'
```

Capture `runId`. If `__typename != "LaunchRunSuccess"`, report the error and stop.

### Step 3 — Poll until terminal status

Poll every 20s (max 60 attempts = 20 min):

```bash
curl -s https://dagster-to-agents-production.up.railway.app/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ runOrError(runId: \"<RUN_ID>\") { ... on Run { status stats { ... on RunStatsSnapshot { stepsSucceeded stepsFailed } } stepStats { stepKey status } } } }"}'
```

Terminal statuses: `SUCCESS`, `FAILURE`, `CANCELED`.

Report each poll result concisely: `⏳ STARTED — N succeeded, M failed`.

### Step 4 — Verify output table in GeoSpot staging

On `SUCCESS`, connect to GeoSpot staging and verify the target table has rows:

```python
import psycopg2, os

conn = psycopg2.connect(
    host=os.environ["GEOSPOT_DB_HOST"],
    user=os.environ["GEOSPOT_DB_USER"],
    password=os.environ["GEOSPOT_DB_PASSWORD"],
    dbname="geospot",
    port=5432,
    connect_timeout=10,
)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM "<TABLE_NAME>"')
count = cur.fetchone()[0]
cur.execute('SELECT * FROM "<TABLE_NAME>" LIMIT 3')
sample = cur.fetchall()
conn.close()
print(count, sample)
```

Run via `railway run -- python3 -c "<script>"` to inject Railway env vars, OR
call directly with the known credentials from the Railway variables panel.

### Step 5 — Report

```
✅ Run <runId> SUCCESS
   Steps: N succeeded, 0 failed
   Table: <TABLE_NAME> → <COUNT> rows
   Sample row: <first row>
```

or

```
❌ Run <runId> FAILURE
   Failed steps: <list>
   Check logs: https://dagster-to-agents-production.up.railway.app/runs/<runId>
```

---

## Flow → Table mapping

| Job | Target table |
|---|---|
| `amenity_desc_consistency_job` | `dagster_agent_rpt_amenity_description_consistency` |

Add new entries here as flows are onboarded.
