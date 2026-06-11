# Databricks notebook source
# MAGIC %md
# MAGIC # OTel PII Redaction — Guided Deployment
# MAGIC
# MAGIC This notebook deploys the full OTel PII redaction pipeline step by step.
# MAGIC It creates an SDP pipeline that uses `ai_mask()` to redact PII from OTel traces,
# MAGIC a unified view for querying, and a retention cleanup job.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Unity Catalog enabled workspace
# MAGIC - AI Functions available (serverless)
# MAGIC - OTel trace data in UC tables (via MLflow, OTel exporter, or any OTLP client)
# MAGIC
# MAGIC **Usage:** Fill in the parameters above, then **Run All**.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "1. Catalog")
dbutils.widgets.text("source_schema", "", "2. Source Schema (raw OTel tables)")
dbutils.widgets.text("target_schema", "", "3. Target Schema (redacted output)")
dbutils.widgets.text("table_prefix", "", "4. Table Prefix")
dbutils.widgets.text("pii_categories", "'email','phone','ssn','credit_card','name','address'", "5. PII Categories")
dbutils.widgets.text("pipeline_name", "otel-pii-redaction", "6. Pipeline Name")

# COMMAND ----------

import re

catalog = dbutils.widgets.get("catalog").strip()
source_schema = dbutils.widgets.get("source_schema").strip()
target_schema = dbutils.widgets.get("target_schema").strip()
table_prefix = dbutils.widgets.get("table_prefix").strip()
pii_categories = dbutils.widgets.get("pii_categories").strip()
pipeline_name = dbutils.widgets.get("pipeline_name").strip()

# Validate identifiers (catalog, schema, prefix)
_ID_RE = re.compile(r"^[a-zA-Z0-9_]+$")
for name, label in [
    (catalog, "Catalog"),
    (source_schema, "Source Schema"),
    (target_schema, "Target Schema"),
    (table_prefix, "Table Prefix"),
]:
    if not name:
        dbutils.notebook.exit(f"FAILED: {label} is required")
    if not _ID_RE.match(name):
        dbutils.notebook.exit(f"FAILED: Invalid {label} '{name}'. Only alphanumeric and underscores allowed.")

# Validate pipeline name (allows hyphens)
_PIPELINE_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
if not pipeline_name:
    dbutils.notebook.exit("FAILED: Pipeline Name is required")
if not _PIPELINE_NAME_RE.match(pipeline_name):
    dbutils.notebook.exit(f"FAILED: Invalid Pipeline Name '{pipeline_name}'. Only alphanumeric, underscores, and hyphens allowed.")

# Validate pii_categories format: comma-separated single-quoted identifiers
_PII_RE = re.compile(r"^('[a-zA-Z_]+'(,'[a-zA-Z_]+')*)?$")
if pii_categories and not _PII_RE.match(pii_categories):
    dbutils.notebook.exit(
        f"FAILED: Invalid PII categories format: '{pii_categories}'. "
        "Expected comma-separated quoted names like: 'email','phone','ssn'"
    )

spans_table = f"{catalog}.{source_schema}.{table_prefix}_otel_spans"
logs_table = f"{catalog}.{source_schema}.{table_prefix}_otel_logs"
annotations_table = f"{catalog}.{source_schema}.{table_prefix}_otel_annotations"

print("=== Configuration ===")
print(f"  Catalog:        {catalog}")
print(f"  Source schema:  {catalog}.{source_schema}")
print(f"  Target schema:  {catalog}.{target_schema}")
print(f"  Table prefix:   {table_prefix}")
print(f"  PII categories: {pii_categories}")
print(f"  Pipeline name:  {pipeline_name}")
print(f"  Source tables:  {spans_table}, {logs_table}, {annotations_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize SDK Client

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
current_user = w.current_user.me()
print(f"Authenticated as: {current_user.user_name}")

workspace_path = f"/Users/{current_user.user_name}/otel-pii-redaction"
print(f"Workspace path:  {workspace_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Verify Source Tables

# COMMAND ----------

from pyspark.sql.utils import AnalysisException

for table_fqn in [spans_table, logs_table, annotations_table]:
    try:
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table_fqn}").first()["cnt"]
        print(f"  {table_fqn} — {count:,} rows")
    except AnalysisException as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "does not exist" in str(e):
            print(f"  {table_fqn} — not found (will be created when data arrives)")
        else:
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Target Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{target_schema}")
print(f"Target schema ready: {catalog}.{target_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Upload Pipeline SQL to Workspace

# COMMAND ----------

import os
import base64

# Read pipeline SQL from the repo (assumes notebook is in a Databricks Git folder)
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_dir = os.path.dirname(notebook_path)
sql_file_path = f"/Workspace{notebook_dir}/pii_redaction_pipeline.sql"

try:
    with open(sql_file_path, "r") as f:
        pipeline_sql_content = f.read()
    print(f"Read pipeline SQL ({len(pipeline_sql_content)} chars) from: {sql_file_path}")
except FileNotFoundError:
    dbutils.notebook.exit(
        f"FAILED: Could not find pii_redaction_pipeline.sql at {sql_file_path}. "
        "Make sure you are running this notebook from a Databricks Git folder (Repos) "
        "that contains the otel-pii-redaction repository."
    )

# Upload to workspace
from databricks.sdk.service.workspace import ImportFormat, Language

w.workspace.mkdirs(workspace_path)
w.workspace.import_(
    path=f"{workspace_path}/pii_redaction_pipeline.sql",
    content=base64.b64encode(pipeline_sql_content.encode()).decode(),
    format=ImportFormat.SOURCE,
    language=Language.SQL,
    overwrite=True,
)
pipeline_sql_workspace_path = f"{workspace_path}/pii_redaction_pipeline.sql"
print(f"Uploaded pipeline SQL to: {pipeline_sql_workspace_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create or Update SDP Pipeline

# COMMAND ----------

from databricks.sdk.service.pipelines import PipelineLibrary, FileLibrary

# Check for existing pipeline with the same name
existing = [
    p for p in w.pipelines.list_pipelines(filter=f"name LIKE '{pipeline_name}'")
    if p.name == pipeline_name
]

pipeline_spec = dict(
    name=pipeline_name,
    catalog=catalog,
    schema=target_schema,
    serverless=True,
    continuous=False,
    channel="CURRENT",
    configuration={
        "source_catalog": catalog,
        "source_schema": source_schema,
        "table_prefix": table_prefix,
        "pii_categories": pii_categories,
    },
    libraries=[PipelineLibrary(file=FileLibrary(path=pipeline_sql_workspace_path))],
    tags={"created_by": "otel-pii-redaction-notebook"},
)

if existing:
    pipeline_id = existing[0].pipeline_id
    print(f"Pipeline '{pipeline_name}' already exists (ID: {pipeline_id}). Updating...")
    w.pipelines.update(pipeline_id=pipeline_id, **pipeline_spec)
    print("Pipeline updated.")
else:
    result = w.pipelines.create(**pipeline_spec)
    pipeline_id = result.pipeline_id
    print(f"Pipeline created with ID: {pipeline_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Trigger Pipeline Run

# COMMAND ----------

update_response = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=True)
if not update_response or not update_response.update_id:
    dbutils.notebook.exit("FAILED: Pipeline update was not triggered. Check pipeline status in the UI.")

update_id = update_response.update_id
print(f"Pipeline update triggered. Update ID: {update_id}")

host = w.config.host.rstrip("/") if w.config.host else spark.conf.get("spark.databricks.workspaceUrl", "")
if host:
    print(f"\nMonitor progress: {host}/pipelines/{pipeline_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Wait for Pipeline Completion
# MAGIC
# MAGIC This cell polls until the pipeline finishes. Skip it if you prefer to check the UI manually.

# COMMAND ----------

import time

MAX_WAIT_SECONDS = 3600
POLL_INTERVAL = 30
elapsed = 0
state = "UNKNOWN"

print(f"Waiting for pipeline to complete (timeout: {MAX_WAIT_SECONDS // 60}min, checking every {POLL_INTERVAL}s)...")
while elapsed < MAX_WAIT_SECONDS:
    pipeline_detail = w.pipelines.get(pipeline_id=pipeline_id)
    latest = pipeline_detail.latest_updates or []
    # Find the specific update we triggered
    matching = [u for u in latest if u.update_id == update_id]
    if matching:
        state = matching[0].state.value if matching[0].state else "UNKNOWN"
    elif latest:
        state = latest[0].state.value if latest[0].state else "UNKNOWN"
    else:
        state = "STARTING"
    print(f"  [{elapsed}s] State: {state}")
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(POLL_INTERVAL)
    elapsed += POLL_INTERVAL

if elapsed >= MAX_WAIT_SECONDS and state not in ("COMPLETED", "FAILED", "CANCELED"):
    dbutils.notebook.exit(f"FAILED: Pipeline timed out after {MAX_WAIT_SECONDS}s in state {state}")

if state != "COMPLETED":
    print(f"\nPipeline ended with state: {state}. Check the UI for details.")
    dbutils.notebook.exit(f"FAILED: Pipeline ended with state {state}")

print("\nPipeline completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Create Unified View

# COMMAND ----------

unified_view_path = f"/Workspace{notebook_dir}/unified_view.sql"

try:
    with open(unified_view_path, "r") as f:
        unified_view_sql = f.read()
except FileNotFoundError:
    dbutils.notebook.exit(
        f"FAILED: Could not find unified_view.sql at {unified_view_path}. "
        "Make sure the file exists in the same Git folder."
    )

# Substitute parameters
unified_view_sql = (
    unified_view_sql
    .replace("${target_catalog}", catalog)
    .replace("${target_schema}", target_schema)
    .replace("${table_prefix}", table_prefix)
)

spark.sql(unified_view_sql)
print(f"Unified view created: {catalog}.{target_schema}.{table_prefix}_trace_unified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Create Retention Cleanup Job

# COMMAND ----------

from databricks.sdk.service.jobs import (
    JobSettings,
    Task,
    NotebookTask,
    CronSchedule,
    PauseStatus,
    Source,
    QueueSettings,
)

# Upload retention notebook
retention_source_path = f"/Workspace{notebook_dir}/otel_retention_cleanup.py"
try:
    with open(retention_source_path, "r") as f:
        retention_content = f.read()
except FileNotFoundError:
    dbutils.notebook.exit(
        f"FAILED: Could not find otel_retention_cleanup.py at {retention_source_path}."
    )

w.workspace.import_(
    path=f"{workspace_path}/otel_retention_cleanup",
    content=base64.b64encode(retention_content.encode()).decode(),
    format=ImportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True,
)
print(f"Uploaded retention notebook to: {workspace_path}/otel_retention_cleanup")

# Build desired job settings
job_name = "otel-raw-retention-cleanup"
desired_settings = JobSettings(
    name=job_name,
    schedule=CronSchedule(
        quartz_cron_expression="0 0 2 * * ?",
        timezone_id="America/Los_Angeles",
        pause_status=PauseStatus.UNPAUSED,
    ),
    max_concurrent_runs=1,
    tasks=[
        Task(
            task_key="retention_cleanup",
            notebook_task=NotebookTask(
                notebook_path=f"{workspace_path}/otel_retention_cleanup",
                base_parameters={
                    "retention_days": "90",
                    "source_catalog": catalog,
                    "source_schema": source_schema,
                    "table_prefix": table_prefix,
                },
                source=Source.WORKSPACE,
            ),
            description="Delete rows older than retention_days and vacuum raw OTel tables",
        )
    ],
    tags={"project": "otel-pii-redaction", "purpose": "retention-cleanup"},
    queue=QueueSettings(enabled=True),
)

# Check for existing job with same name
existing_jobs = [j for j in w.jobs.list(name=job_name) if j.settings and j.settings.name == job_name]

if existing_jobs:
    job_id = existing_jobs[0].job_id
    print(f"Job '{job_name}' already exists (ID: {job_id}). Updating...")
    w.jobs.update(job_id=job_id, new_settings=desired_settings)
    print("Job updated.")
else:
    job_result = w.jobs.create(
        name=desired_settings.name,
        schedule=desired_settings.schedule,
        max_concurrent_runs=desired_settings.max_concurrent_runs,
        tasks=desired_settings.tasks,
        tags=desired_settings.tags,
        queue=desired_settings.queue,
    )
    job_id = job_result.job_id
    print(f"Retention job created with ID: {job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Validate Redaction

# COMMAND ----------

print("Querying redacted spans to verify PII was masked...\n")

try:
    result_df = spark.sql(f"""
        SELECT
            s.span_id,
            CAST(s.attributes AS STRING) AS raw_attrs,
            CAST(r.attributes AS STRING) AS redacted_attrs
        FROM {catalog}.{source_schema}.{table_prefix}_otel_spans s
        JOIN {catalog}.{target_schema}.redacted_spans r
          ON s.trace_id = r.trace_id AND s.span_id = r.span_id
        WHERE s.attributes IS NOT NULL
        LIMIT 5
    """)
    display(result_df)
    print("\nCompare 'raw_attrs' vs 'redacted_attrs' above to verify PII was masked.")
except Exception as e:
    print(f"Validation query failed: {e}")
    print("If the pipeline just completed, the tables may still be finalizing. Try re-running this cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deployment Complete
# MAGIC
# MAGIC **Resources created:**

# COMMAND ----------

print("=== Deployment Summary ===")
print(f"  Pipeline ID:     {pipeline_id}")
print(f"  Retention Job:   {job_id}")
print(f"  Target schema:   {catalog}.{target_schema}")
print(f"  Redacted spans:  {catalog}.{target_schema}.redacted_spans")
print(f"  Redacted logs:   {catalog}.{target_schema}.redacted_logs")
print(f"  Unified view:    {catalog}.{target_schema}.{table_prefix}_trace_unified")
if host:
    print(f"\n  Pipeline UI:     https://{host}/pipelines/{pipeline_id}")
    print(f"  Job UI:          https://{host}/jobs/{job_id}")
