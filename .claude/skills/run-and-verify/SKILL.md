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

- **Dagster UI**: `https://dagstertoagents-production.up.railway.app`
- **GraphQL endpoint**: `https://dagstertoagents-production.up.railway.app/graphql`
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
curl -s https://dagstertoagents-production.up.railway.app/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ locationStatusesOrError { ... on WorkspaceLocationStatusEntries { entries { loadStatus } } } }"}'
```

If `loadStatus != "LOADED"`, wait 15s and retry (max 3 times). If still not loaded, abort and report.

### Step 2 — Launch the job

```bash
curl -s https://dagstertoagents-production.up.railway.app/graphql \
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
curl -s https://dagstertoagents-production.up.railway.app/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ runOrError(runId: \"<RUN_ID>\") { ... on Run { status stats { ... on RunStatsSnapshot { stepsSucceeded stepsFailed } } stepStats { stepKey status } } } }"}'
```

Terminal statuses: `SUCCESS`, `FAILURE`, `CANCELED`.

Report each poll result concisely: `⏳ STARTED — N succeeded, M failed`.

### Step 4 — Verify S3 output

On `SUCCESS`, confirm the CSV was written to S3:

```bash
aws s3 ls s3://dagster-assets-production/<S3_KEY_PREFIX>/ --recursive
```

Expected: file exists with recent timestamp.

### Step 5 — Call GeoSpot API directly and verify response

The GeoSpot API is **async** — it accepts the request and loads in background.
Call it directly to confirm it accepted and get the response:

```python
import requests

resp = requests.post(
    "https://geospot.spot2.mx/data-lake-house/dagster/",
    headers={
        "Authorization": "Api-Key <GEOSPOT_API_KEY>",
        "Content-Type": "application/json",
    },
    json={
        "bucket_name": "dagster-assets-production",
        "s3_key": "<S3_KEY>",
        "table_name": "<TABLE_NAME>",
        "mode": "replace",
    },
    timeout=30,
)
print(resp.status_code, resp.text)
```

Expected response: `{"message": "Inserting data from S3 to Postgres in progress."}`
This confirms the API accepted the load request.

> **Note:** The GeoSpot API writes asynchronously. The table may take several minutes
> to appear in staging due to the async job + replication lag from primary to read replica.
> A 200 response with "in progress" message is the success signal from the API side.

### Step 6 — Verify table in GeoSpot staging (with retry)

Poll the staging DB for the table, waiting up to 5 min for the async load:

```python
import psycopg2, time

for attempt in range(10):
    conn = psycopg2.connect(
        host="geospot-staging-read-replica.cqt3kzfdcy2o.us-east-1.rds.amazonaws.com",
        user="jean.palencia",
        password="<GEOSPOT_DB_PASSWORD>",
        dbname="geospot", port=5432, connect_timeout=10,
    )
    cur = conn.cursor()
    cur.execute("""SELECT EXISTS(SELECT 1 FROM information_schema.tables
                   WHERE table_name = '<TABLE_NAME>')""")
    if cur.fetchone()[0]:
        cur.execute('SELECT COUNT(*) FROM "<TABLE_NAME>"')
        count = cur.fetchone()[0]
        cur.execute('SELECT * FROM "<TABLE_NAME>" LIMIT 1')
        sample = cur.fetchone()
        conn.close()
        print(f"✅ {count} rows — sample: {sample}")
        break
    conn.close()
    print(f"[{attempt+1}] table not yet available, waiting 30s...")
    time.sleep(30)
else:
    print("⚠️  Table not found after 5 min — API may write to a different DB or job is still running")
```

### Step 7 — Report

```
✅ Run <runId> SUCCESS
   Steps: N succeeded, 0 failed
   S3: s3://dagster-assets-production/<S3_KEY> ✅
   GeoSpot API: 200 "in progress" → load accepted ✅
   Table: <TABLE_NAME> → <COUNT> rows  (or ⚠️ still pending)
   Sample row: <first row>
```

or

```
❌ Run <runId> FAILURE
   Failed steps: <list>
   Check logs: https://dagstertoagents-production.up.railway.app/runs/<runId>
```

---

## Flow → Table mapping

| Job | Target table |
|---|---|
| `amenity_desc_consistency_job` | `dagster_agent_rpt_amenity_description_consistency` |

Add new entries here as flows are onboarded.
