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
./deploy.sh <WORKSPACE_HOST> <CATALOG> <SOURCE_SCHEMA> <TARGET_SCHEMA> <TABLE_PREFIX> [REDACTION_CONFIG]
```

Examples:

```bash
# With defaults (standard PII categories, no custom patterns)
./deploy.sh https://my-workspace.cloud.databricks.com my_catalog traces_raw traces_redacted my_app

# With custom redaction config (PII categories + regex patterns)
./deploy.sh https://my-workspace.cloud.databricks.com my_catalog traces_raw traces_redacted my_app redaction_config.json
```

The optional `REDACTION_CONFIG` is a JSON file that controls both `ai_mask` PII categories and custom regex patterns. See [`redaction_config.example.json`](redaction_config.example.json) for the format.

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

## Redaction configuration

All redaction settings — both `ai_mask` PII categories and custom regex patterns — are controlled via a single JSON config file passed as the 6th argument to `deploy.sh`. See [`redaction_config.example.json`](redaction_config.example.json) for a complete example.

```json
{
  "pii_categories": "'email','phone','ssn','credit_card','name','address'",
  "custom_patterns": [
    "EMP-[0-9]{6}",
    "ACCT-[A-Z0-9]+"
  ]
}
```

If no config file is provided, the pipeline uses the default PII categories and no custom patterns.

### `pii_categories` — AI-based redaction

Controls what `ai_mask()` looks for. Comma-separated list of quoted category names:

```json
"pii_categories": "'email','phone','ssn','credit_card','name','address'"
```

Supported categories (LLM-backed, interprets labels semantically):
- `email`, `phone`, `name`, `address` — reliable
- `ssn`, `credit_card` — reliable
- `ip_address`, `date_of_birth` — works in practice

### `custom_patterns` — regex-based redaction

A list of Java-compatible regex patterns for structured formats that `ai_mask` doesn't recognize. Any number of patterns can be specified. All matches are replaced with `[MASKED]`.

```json
"custom_patterns": [
  "EMP-[0-9]{6}",
  "ACCT-[A-Z0-9]+",
  "INT-[0-9]{4}-[0-9]{4}",
  "sk_live_[A-Za-z0-9]{32}"
]
```

The deploy script combines all patterns into a single regex with alternation (`|`), so the pipeline applies them in one `regexp_replace()` call before `ai_mask()`.

**Important:** Use `[0-9]` instead of `\d` for digit matching. The `\d` shorthand gets mangled by pipeline parameter substitution.

### When to use which

| Use case | Approach |
|---|---|
| Known, structured formats (employee IDs, account numbers, API keys) | `custom_patterns` — deterministic, fast, no LLM cost |
| Free-text PII (names, addresses, written descriptions) | `pii_categories` — LLM-backed, handles natural language |

### Example patterns

| What to redact | Pattern |
|---|---|
| Employee IDs (`EMP-123456`) | `EMP-[0-9]{6}` |
| Internal accounts (`ACCT-AB12CD34`) | `ACCT-[A-Z0-9]+` |
| Ticket refs (`INT-2026-1234`) | `INT-[0-9]{4}-[0-9]{4}` |
| API keys (`sk_live_...`) | `sk_live_[A-Za-z0-9]{32}` |

## Testing

### Send test PII data

Generate test spans with known PII to validate redaction:

```bash
pip install opentelemetry-exporter-otlp-proto-http

python send_pii_traces.py <WORKSPACE_HOST> <CATALOG.SCHEMA.PREFIX_otel_spans>
```

This sends 60 test traces containing emails, phones, SSNs, credit cards, names, addresses, employee IDs, and internal account numbers.

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
| `redaction_config.example.json` | Example redaction config (PII categories + regex patterns) |
| `pii_redaction_pipeline.sql` | SDP pipeline — streaming tables with regex + `ai_mask()` |
| `otel_retention_cleanup.py` | Databricks notebook for raw table TTL cleanup |
| `unified_view.sql` | Unified trace view joining spans + annotations |
| `setup_schema_and_grants.sql` | Schema creation and access control grants |
| `pipeline_config.json` | Example pipeline config (reference) |
| `retention_job_config.json` | Example job config (reference) |
| `send_pii_traces.py` | Test utility — sends PII test data as OTel spans |
| `pii_test_data.jsonl` | 60 lines of synthetic PII test data (including regex patterns) |

## Architecture

For the full architecture document including design decisions, trade-offs (batch vs view-based redaction), and GDPR considerations, see [`otel-pii-redaction-plan.md`](otel-pii-redaction-plan.md).
