-- Unified trace view on redacted tables
-- Joins redacted spans with annotations for convenient trace querying

CREATE OR REPLACE VIEW ${target_catalog}.${target_schema}.${table_prefix}_trace_unified (
  trace_id,
  date,
  request_time_nano,
  execution_duration_nano,
  spans,
  annotations
)
AS SELECT
  s.trace_id,
  s.date,
  min(s.start_time_unix_nano) AS request_time_nano,
  max(s.end_time_unix_nano) - min(s.start_time_unix_nano) AS execution_duration_nano,
  collect_list(named_struct(
    'span_id', s.span_id,
    'parent_span_id', s.parent_span_id,
    'name', s.name,
    'kind', s.kind,
    'start_time_unix_nano', s.start_time_unix_nano,
    'end_time_unix_nano', s.end_time_unix_nano,
    'status', s.status,
    'attributes', s.attributes,
    'events', s.events
  )) AS spans,
  first(a.annotations) AS annotations
FROM ${target_catalog}.${target_schema}.redacted_spans s
LEFT JOIN (
  SELECT
    target_id,
    collect_list(named_struct(
      'annotation_id', annotation_id,
      'annotation_type', annotation_type,
      'name', name,
      'value', cast(value AS string),
      'comment', comment,
      'created_at', created_at,
      'created_by', created_by
    )) AS annotations
  FROM ${target_catalog}.${target_schema}.redacted_annotations
  GROUP BY target_id
) a ON s.trace_id = a.target_id
GROUP BY s.trace_id, s.date, a.annotations;
