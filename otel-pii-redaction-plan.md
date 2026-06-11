# Reference Solution: PII Redaction for OpenTelemetry Traces on Databricks

## Overview

This document describes a reference architecture for redacting PII from OpenTelemetry (OTel) spans stored in Unity Catalog on Databricks. It covers two complementary flows — server-side batch processing and view-based on-read redaction — using Databricks AI Functions and Spark Declarative Pipelines.

---

## Parameters

All components in this solution are parameterized for reuse across environments.

### Table Parameters

| Parameter | Description | Example |
|---|---|---|
| `source_catalog` | UC catalog containing raw OTel tables | `ml_observability` |
| `source_schema` | UC schema containing raw OTel tables | `traces_raw` |
| `table_prefix` | Prefix used when configuring OTel trace storage | `mlflow` |
| `target_catalog` | UC catalog for redacted output tables | `ml_observability` |
| `target_schema` | UC schema for redacted output tables | `traces_redacted` |
| `retention_days` | TTL for unredacted data (GDPR compliance). Set to `0` to disable. | `90` |

Derived source table names:
- `{source_catalog}.{source_schema}.{table_prefix}_otel_spans`
- `{source_catalog}.{source_schema}.{table_prefix}_otel_logs`
- `{source_catalog}.{source_schema}.{table_prefix}_otel_annotations`

### PII Redaction Rules

| Parameter | Description | Example |
|---|---|---|
| `pii_categories` | List of PII types to redact | `["email", "phone", "ssn", "credit_card", "ip_address", "name", "address"]` |
| `redaction_mode` | How to mask PII: `mask`, `hash`, or `remove` | `mask` |
| `mask_character` | Character used for masking | `*` |
| `fields_to_redact` | OTel fields to apply redaction to | `["attributes", "resource.attributes", "events"]` |
| `allowlisted_keys` | Attribute keys to skip redaction on (e.g. `service.name`) | `["service.name", "http.method", "http.status_code"]` |
| `custom_patterns` | Regex patterns for domain-specific PII | `{"employee_id": "EMP-\\d{6}", "internal_account": "ACCT-[A-Z0-9]+"}` |

---

## Flow 1: Server-Side Batch Processing with Spark Declarative Pipelines (Recommended)

### Why Spark Declarative Pipelines (SDP)

| Consideration | SDP (Streaming Table) | Notebook + Scheduled Job |
|---|---|---|
| Incremental processing | Built-in — streaming tables only process new rows | Manual checkpoint management with Structured Streaming |
| AI function support | Native in SQL | Native in SQL |
| Monitoring & alerting | Built-in pipeline UI, event log | Must configure separately |
| Retry / failure handling | Automatic | Manual |
| Lineage | Automatic UC lineage tracking | Manual |
| Serverless compute | Yes | Yes (with serverless jobs) |
| Ops overhead | Low — fully managed | Medium — manage state, schedules, alerting |

**Verdict**: SDP with streaming tables is the best fit. OTel spans are append-only, making them ideal for incremental streaming table ingestion. AI functions (`ai_mask`) are SQL-native, so an SDP SQL pipeline is the simplest implementation.

### Architecture

```
OTel Traces (full fidelity)
    │
    ▼
┌─────────────────────────────────────────┐
│  Raw UC Tables (restricted access)      │
│  {source_catalog}.{source_schema}       │
│  ├── {prefix}_otel_spans                │
│  ├── {prefix}_otel_logs                 │
│  └── {prefix}_otel_annotations          │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│  Spark Declarative Pipeline (SDP)       │
│  ├── Streaming Table: redacted_spans    │
│  ├── Streaming Table: redacted_logs     │
│  └── Passthrough: annotations           │
│                                         │
│  Redaction via:                         │
│  ├── ai_mask() for free-text fields     │
│  ├── regexp_replace() for known formats │
│  └── Allowlist skip for safe keys       │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│  Redacted UC Tables (broader access)    │
│  {target_catalog}.{target_schema}       │
│  ├── {prefix}_otel_spans                │
│  ├── {prefix}_otel_logs                 │
│  └── {prefix}_otel_annotations          │
│  └── {prefix}_trace_unified (view)      │
└─────────────────────────────────────────┘
```

### Implementation

#### Step 1: Lock down raw tables

Grant access to the raw tables only to the pipeline service principal and admin users.

```sql
-- Restrict raw table access
GRANT USE CATALOG ON CATALOG ${source_catalog} TO `pii_pipeline_sp`;
GRANT USE SCHEMA ON SCHEMA ${source_catalog}.${source_schema} TO `pii_pipeline_sp`;
GRANT SELECT ON TABLE ${source_catalog}.${source_schema}.${table_prefix}_otel_spans TO `pii_pipeline_sp`;
GRANT SELECT ON TABLE ${source_catalog}.${source_schema}.${table_prefix}_otel_logs TO `pii_pipeline_sp`;

-- Revoke broader access
REVOKE SELECT ON TABLE ${source_catalog}.${source_schema}.${table_prefix}_otel_spans FROM `data_team`;
```

#### Step 2: Create the SDP pipeline (SQL file)

File: `pii_redaction_pipeline.sql`

```sql
-- =============================================================
-- Streaming Table: Redacted Spans
-- =============================================================
CREATE OR REFRESH STREAMING TABLE redacted_spans
COMMENT 'PII-redacted OTel spans'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.zOrderCols' = 'trace_id,date'
)
AS
SELECT
  trace_id,
  span_id,
  parent_span_id,
  name,
  kind,
  start_time,
  end_time,
  status,
  date,
  record_id,
  service_name,
  time,
  instrumentation_scope,

  -- Redact span attributes (VARIANT field)
  -- ai_mask replaces PII with masked values in free-text content
  CASE
    WHEN attributes IS NOT NULL THEN
      ai_mask(
        CAST(attributes AS STRING),
        array(${pii_categories})  -- e.g. array('email','phone','ssn','name','address')
      )
    ELSE attributes
  END AS attributes,

  -- Redact resource attributes
  CASE
    WHEN resource:attributes IS NOT NULL THEN
      named_struct(
        'attributes',
        ai_mask(
          CAST(resource:attributes AS STRING),
          array(${pii_categories})
        ),
        'dropped_attributes_count',
        resource:dropped_attributes_count
      )
    ELSE resource
  END AS resource,

  -- Redact events (may contain exception messages with PII)
  CASE
    WHEN events IS NOT NULL THEN
      ai_mask(
        CAST(events AS STRING),
        array(${pii_categories})
      )
    ELSE events
  END AS events,

  -- Pass through links unchanged (typically just trace/span IDs)
  links

FROM STREAM(${source_catalog}.${source_schema}.${table_prefix}_otel_spans);


-- =============================================================
-- Streaming Table: Redacted Logs
-- =============================================================
CREATE OR REFRESH STREAMING TABLE redacted_logs
COMMENT 'PII-redacted OTel logs'
AS
SELECT
  trace_id,
  span_id,
  severity_number,
  severity_text,
  date,
  record_id,
  service_name,
  time,
  instrumentation_scope,

  -- Redact log body
  CASE
    WHEN body IS NOT NULL THEN
      ai_mask(
        CAST(body AS STRING),
        array(${pii_categories})
      )
    ELSE body
  END AS body,

  -- Redact log attributes
  CASE
    WHEN attributes IS NOT NULL THEN
      ai_mask(
        CAST(attributes AS STRING),
        array(${pii_categories})
      )
    ELSE attributes
  END AS attributes,

  -- Redact resource attributes
  CASE
    WHEN resource:attributes IS NOT NULL THEN
      named_struct(
        'attributes',
        ai_mask(
          CAST(resource:attributes AS STRING),
          array(${pii_categories})
        ),
        'dropped_attributes_count',
        resource:dropped_attributes_count
      )
    ELSE resource
  END AS resource

FROM STREAM(${source_catalog}.${source_schema}.${table_prefix}_otel_logs);


-- =============================================================
-- Streaming Table: Annotations (passthrough — no PII expected)
-- =============================================================
CREATE OR REFRESH STREAMING TABLE redacted_annotations
COMMENT 'OTel annotations (passthrough, no PII redaction applied)'
AS
SELECT *
FROM STREAM(${source_catalog}.${source_schema}.${table_prefix}_otel_annotations);
```

#### Step 3: Create the pipeline resource

```json
{
  "name": "otel-pii-redaction",
  "catalog": "${target_catalog}",
  "schema": "${target_schema}",
  "serverless": true,
  "continuous": false,
  "channel": "CURRENT",
  "configuration": {
    "source_catalog": "<value>",
    "source_schema": "<value>",
    "table_prefix": "<value>",
    "pii_categories": "'email','phone','ssn','credit_card','name','address'"
  },
  "libraries": [
    { "file": { "path": "/Workspace/path/to/pii_redaction_pipeline.sql" } }
  ]
}
```

**Schedule**: Run triggered (e.g., every 15 minutes or hourly) depending on latency requirements. Continuous mode is also an option but increases cost.

#### Step 4: Create unified view on redacted tables

```sql
-- Recreate the trace_unified view pointing at redacted tables
CREATE OR REPLACE VIEW ${target_catalog}.${target_schema}.${table_prefix}_trace_unified AS
SELECT
  s.trace_id,
  s.date,
  min(s.start_time) AS request_time,
  max(s.end_time) - min(s.start_time) AS execution_duration,
  collect_list(
    named_struct(
      'span_id', s.span_id,
      'parent_span_id', s.parent_span_id,
      'name', s.name,
      'kind', s.kind,
      'start_time', s.start_time,
      'end_time', s.end_time,
      'status', s.status,
      'attributes', s.attributes,
      'events', s.events
    )
  ) AS spans,
  a.tags,
  a.assessments
FROM ${target_catalog}.${target_schema}.redacted_spans s
LEFT JOIN ${target_catalog}.${target_schema}.redacted_annotations a
  ON s.trace_id = a.target_id
GROUP BY s.trace_id, s.date, a.tags, a.assessments;
```

#### Step 5: Grant broader access to redacted tables

```sql
GRANT USE CATALOG ON CATALOG ${target_catalog} TO `data_team`;
GRANT USE SCHEMA ON SCHEMA ${target_catalog}.${target_schema} TO `data_team`;
GRANT SELECT ON SCHEMA ${target_catalog}.${target_schema} TO `data_team`;
```

#### Step 6: Set up retention cleanup on raw tables (optional, for GDPR compliance)

If `retention_days` is configured (> 0), create a scheduled SQL job to delete expired rows and reclaim storage. The OTel ingestion path (Zerobus Ingest) writes to plain Delta tables, which do not support built-in row-level TTL — a scheduled cleanup job is required.

```sql
-- Delete rows older than the retention window
DELETE FROM ${source_catalog}.${source_schema}.${table_prefix}_otel_spans
WHERE date < current_date() - INTERVAL ${retention_days} DAYS;

DELETE FROM ${source_catalog}.${source_schema}.${table_prefix}_otel_logs
WHERE date < current_date() - INTERVAL ${retention_days} DAYS;

-- Reclaim storage after deletion
VACUUM ${source_catalog}.${source_schema}.${table_prefix}_otel_spans;
VACUUM ${source_catalog}.${source_schema}.${table_prefix}_otel_logs;
```

Schedule this as a **Databricks SQL job** running daily or weekly, depending on data volume and compliance requirements.

> **Note**: The `date` column is used as the retention filter since it is the partition column on the OTel tables. `VACUUM` removes files backing deleted rows after the Delta retention threshold (default 7 days). If Predictive Optimization is enabled on the workspace, `VACUUM` may run automatically.

---

## Flow 2: View-Based Redaction with AI Functions (No Data Duplication)

### When to Use

- Storage cost is a primary concern
- Redacted data is queried infrequently
- Acceptable to pay compute cost on every query

### Architecture

```
Raw UC Tables (restricted access)
    │
    ▼
UC View with ai_mask() (broader access)
    │
    ▼
Users query the view — redaction applied at read time
```

### Implementation

```sql
CREATE OR REPLACE VIEW ${target_catalog}.${target_schema}.${table_prefix}_otel_spans_redacted
AS
SELECT
  trace_id,
  span_id,
  parent_span_id,
  name,
  kind,
  start_time,
  end_time,
  status,
  date,
  service_name,
  time,
  instrumentation_scope,
  links,

  ai_mask(
    CAST(attributes AS STRING),
    array(${pii_categories})
  ) AS attributes,

  ai_mask(
    CAST(events AS STRING),
    array(${pii_categories})
  ) AS events,

  named_struct(
    'attributes',
    ai_mask(CAST(resource:attributes AS STRING), array(${pii_categories})),
    'dropped_attributes_count',
    resource:dropped_attributes_count
  ) AS resource

FROM ${source_catalog}.${source_schema}.${table_prefix}_otel_spans;
```

### Trade-offs

| Aspect | Pros | Cons |
|---|---|---|
| Storage | No duplication | — |
| Compute | — | `ai_mask()` called on every query; expensive at scale |
| Latency | Immediately reflects new data | Slower query response |
| Flexibility | Redaction rules update instantly | — |

---

## Flow Comparison Summary

| Dimension | Flow 1: SDP Batch | Flow 2: View-Based |
|---|---|---|
| **Data fidelity preserved** | Yes (raw table retained) | Yes (raw table retained) |
| **Storage cost** | 2x (time-windowed) | 1x |
| **Compute cost** | One-time per record | Per query |
| **Query performance** | Fast (pre-materialized) | Slow (recomputes) |
| **Latency to availability** | Minutes (pipeline interval) | Immediate |
| **Rule change rollout** | Pipeline refresh | Instant |
| **GDPR compliance** | Scheduled cleanup on raw + redacted copy | Scheduled cleanup on raw + view |
| **Best for** | Primary production use | Low-query-volume use |
| **Databricks feature** | Spark Declarative Pipeline | UC View + AI Functions |

---

## Recommended Approach

**Use Flow 1 (SDP) as the primary solution** for most enterprise deployments:
- Preserves full-fidelity data for authorized debugging
- Optimized query performance via materialization
- GDPR-compliant with scheduled retention cleanup on raw data
- Handles both PII redaction and trace filtering in one pipeline
- Fully managed with built-in monitoring and alerting

**Use Flow 2 (view-based)** as a lightweight option for low-query-volume scenarios or as a quick interim solution while setting up Flow 1.

---

## Prerequisites

1. **AI Functions enabled** — requires a SQL warehouse or serverless compute with AI Functions access
2. **Unity Catalog** — OTel traces must be stored in UC tables (MLflow trace-to-UC binding configured)
3. **Service principal** — for pipeline execution with appropriate grants on source tables
4. **Foundation model endpoint** — `ai_mask` uses a foundation model; ensure the endpoint is available and sized for throughput

## Next Steps

- [ ] Validate `ai_mask()` behavior on VARIANT columns with sample OTel span data
- [ ] Benchmark `ai_mask()` throughput to size the pipeline schedule interval
- [ ] Define the allowlisted attribute keys that should skip redaction
- [ ] Set up access control groups (raw-access vs redacted-access)
- [ ] Configure scheduled DELETE + VACUUM job for raw table retention
- [ ] Build monitoring dashboard for pipeline health and redaction coverage
