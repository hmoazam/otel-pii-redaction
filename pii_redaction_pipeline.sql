-- =============================================================
-- Spark Declarative Pipeline: PII Redaction for OTel Traces
-- =============================================================
-- Redaction is applied in two stages:
--   1. regexp_replace() for custom patterns (${custom_patterns_regex})
--   2. ai_mask() for standard PII categories (${pii_categories})
--
-- If no custom patterns are configured, the regex is set to an
-- unmatchable pattern ($^) so it's a no-op.
-- =============================================================


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
  record_id,
  time,
  date,
  service_name,
  trace_id,
  span_id,
  trace_state,
  parent_span_id,
  flags,
  name,
  kind,
  start_time_unix_nano,
  end_time_unix_nano,

  -- Redact span attributes: custom regex first, then ai_mask
  CASE
    WHEN attributes IS NOT NULL THEN
      ai_mask(
        regexp_replace(CAST(attributes AS STRING), '${custom_patterns_regex}', '[MASKED]'),
        array(${pii_categories})
      )
    ELSE NULL
  END AS attributes,

  dropped_attributes_count,

  -- Redact events
  CASE
    WHEN events IS NOT NULL THEN
      ai_mask(
        regexp_replace(CAST(events AS STRING), '${custom_patterns_regex}', '[MASKED]'),
        array(${pii_categories})
      )
    ELSE NULL
  END AS events,

  dropped_events_count,

  links,
  dropped_links_count,
  status,

  -- Redact resource attributes
  CASE
    WHEN resource.attributes IS NOT NULL THEN
      named_struct(
        'attributes',
        ai_mask(
          regexp_replace(CAST(resource.attributes AS STRING), '${custom_patterns_regex}', '[MASKED]'),
          array(${pii_categories})
        ),
        'dropped_attributes_count',
        resource.dropped_attributes_count
      )
    ELSE resource
  END AS resource,

  resource_schema_url,
  instrumentation_scope,
  span_schema_url

FROM STREAM(${source_catalog}.${source_schema}.${table_prefix}_otel_spans);


-- =============================================================
-- Streaming Table: Redacted Logs
-- =============================================================
CREATE OR REFRESH STREAMING TABLE redacted_logs
COMMENT 'PII-redacted OTel logs'
AS
SELECT
  record_id,
  time,
  date,
  service_name,
  event_name,
  trace_id,
  span_id,
  time_unix_nano,
  observed_time_unix_nano,
  severity_number,
  severity_text,

  -- Redact log body
  CASE
    WHEN body IS NOT NULL THEN
      ai_mask(
        regexp_replace(CAST(body AS STRING), '${custom_patterns_regex}', '[MASKED]'),
        array(${pii_categories})
      )
    ELSE NULL
  END AS body,

  -- Redact log attributes
  CASE
    WHEN attributes IS NOT NULL THEN
      ai_mask(
        regexp_replace(CAST(attributes AS STRING), '${custom_patterns_regex}', '[MASKED]'),
        array(${pii_categories})
      )
    ELSE NULL
  END AS attributes,

  dropped_attributes_count,
  flags,

  -- Redact resource attributes
  CASE
    WHEN resource.attributes IS NOT NULL THEN
      named_struct(
        'attributes',
        ai_mask(
          regexp_replace(CAST(resource.attributes AS STRING), '${custom_patterns_regex}', '[MASKED]'),
          array(${pii_categories})
        ),
        'dropped_attributes_count',
        resource.dropped_attributes_count
      )
    ELSE resource
  END AS resource,

  resource_schema_url,
  instrumentation_scope,
  log_schema_url

FROM STREAM(${source_catalog}.${source_schema}.${table_prefix}_otel_logs);


-- =============================================================
-- Streaming Table: Annotations (passthrough -- no PII expected)
-- =============================================================
CREATE OR REFRESH STREAMING TABLE redacted_annotations
COMMENT 'OTel annotations (passthrough, no PII redaction applied)'
AS
SELECT *
FROM STREAM(${source_catalog}.${source_schema}.${table_prefix}_otel_annotations);
