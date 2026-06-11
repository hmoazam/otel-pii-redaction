# OTel PII Redaction for Databricks

Redact PII from OpenTelemetry traces stored in Unity Catalog using Databricks AI Functions and Spark Declarative Pipelines.

Works with any OTel traces in UC — including [MLflow traces logged to Unity Catalog](https://docs.databricks.com/aws/en/mlflow3/genai/tracing/trace-unity-catalog).

## How it works

```
Raw OTel Tables (restricted access)          Redacted Tables (broader access)
┌──────────────────────────────┐            ┌──────────────────────────────┐
│ {prefix}_otel_spans          │            │ redacted_spans               │
│ {prefix}_otel_logs           │──pipeline──│ redacted_logs                │
│ {prefix}_otel_annotations    │            │ redacted_annotations         │
└──────────────────────────────┘            │ {prefix}_trace_unified (view)│
                                            └──────────────────────────────┘
```

A **Spark Declarative Pipeline** (SDP) incrementally reads new OTel spans, applies [`ai_mask()`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_mask) to redact PII (emails, phones, SSNs, credit cards, names, addresses), and writes to redacted tables. A scheduled job handles optional TTL cleanup on the raw tables.

## Prerequisites

- Databricks workspace with **Unity Catalog** enabled
- **AI Functions** available (serverless SQL warehouse or serverless pipeline)
- **Databricks CLI** authenticated to your workspace
- OTel trace data in UC tables (via MLflow, OTel exporter, or any OTLP client)

## Quick start

### One-command deployment

```bash
./deploy.sh <WORKSPACE_HOST> <CATALOG> <SOURCE_SCHEMA> <TARGET_SCHEMA> <TABLE_PREFIX>
```

Example:

```bash
./deploy.sh https://my-workspace.cloud.databricks.com my_catalog traces_raw traces_redacted my_app
```

This will:
1. Upload the pipeline SQL and retention notebook to your workspace
2. Create the target schema
3. Create and trigger the SDP pipeline
4. Create a daily retention cleanup job

After the pipeline completes, run `unified_view.sql` to create the unified trace view (replace `${...}` variables with your values).

### Manual deployment

If you prefer to set things up step by step:

1. **Create the target schema** — run statements in `setup_schema_and_grants.sql`
2. **Upload pipeline SQL** — import `pii_redaction_pipeline.sql` to your workspace
3. **Create the pipeline** — use `pipeline_config.json` as a template (replace `<PLACEHOLDER>` values)
4. **Trigger a pipeline run** — via UI or `databricks pipelines start-update <PIPELINE_ID>`
5. **Create the unified view** — run `unified_view.sql` after the first pipeline run
6. **Upload retention notebook** — import `otel_retention_cleanup.py` to your workspace
7. **Create the retention job** — use `retention_job_config.json` as a template

## Parameters

### Pipeline configuration

| Parameter | Description | Example |
|---|---|---|
| `source_catalog` | UC catalog containing raw OTel tables | `my_catalog` |
| `source_schema` | UC schema containing raw OTel tables | `traces_raw` |
| `table_prefix` | Prefix used for OTel table names | `my_app` |
| `pii_categories` | PII types to redact | `'email','phone','ssn','credit_card','name','address'` |

Source tables are derived as `{source_catalog}.{source_schema}.{table_prefix}_otel_spans` (and `_otel_logs`, `_otel_annotations`).

### Retention job parameters

| Parameter | Description | Default |
|---|---|---|
| `retention_days` | Days to retain raw data before deletion | `90` |
| `source_catalog` | Same as pipeline | — |
| `source_schema` | Same as pipeline | — |
| `table_prefix` | Same as pipeline | — |

## What gets redacted

The pipeline applies `ai_mask()` to these fields:

| Table | Fields redacted |
|---|---|
| **Spans** | `attributes`, `events`, `resource.attributes` |
| **Logs** | `body`, `attributes`, `resource.attributes` |
| **Annotations** | Passthrough (no PII expected) |

Non-PII fields (trace IDs, span IDs, timestamps, service names, status codes) are preserved unchanged.

### Supported PII categories

`ai_mask()` is LLM-backed and recognizes standard PII types well:
- `email`, `phone`, `name`, `address` — reliable
- `ssn`, `credit_card` — reliable
- `ip_address`, `date_of_birth` — works in practice

For **custom patterns** (e.g., employee IDs like `EMP-XXXXXX`), use `regexp_replace()` before `ai_mask()` in the pipeline SQL. See the [plan document](otel-pii-redaction-plan.md) for details.

## Testing

### Send test PII data

Generate test spans with known PII to validate redaction:

```bash
pip install opentelemetry-exporter-otlp-proto-http

python send_pii_traces.py <WORKSPACE_HOST> <CATALOG.SCHEMA.PREFIX_otel_spans>
```

This sends 50 test traces containing emails, phones, SSNs, credit cards, names, and addresses.

### Validate redaction

After running the pipeline, compare raw vs redacted:

```sql
SELECT
  s.span_id,
  CAST(s.attributes AS STRING) AS raw,
  CAST(r.attributes AS STRING) AS redacted
FROM <source_catalog>.<source_schema>.<prefix>_otel_spans s
JOIN <target_catalog>.<target_schema>.redacted_spans r
  ON s.trace_id = r.trace_id AND s.span_id = r.span_id
WHERE s.name = 'pii-test-interaction'
LIMIT 5;
```

## Files

| File | Description |
|---|---|
| `deploy.sh` | One-command deployment script |
| `pii_redaction_pipeline.sql` | SDP pipeline — streaming tables with `ai_mask()` |
| `otel_retention_cleanup.py` | Databricks notebook for raw table TTL cleanup |
| `unified_view.sql` | Unified trace view joining spans + annotations |
| `setup_schema_and_grants.sql` | Schema creation and access control grants |
| `pipeline_config.json` | Example pipeline config (reference) |
| `retention_job_config.json` | Example job config (reference) |
| `send_pii_traces.py` | Test utility — sends PII test data as OTel spans |
| `pii_test_data.jsonl` | 50 lines of synthetic PII test data |

## Architecture

For the full architecture document including design decisions, trade-offs (batch vs view-based redaction), and GDPR considerations, see [`otel-pii-redaction-plan.md`](otel-pii-redaction-plan.md).
