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
dbutils.widgets.text("table_prefix", "", "4. Otel Traces Table Prefix")
dbutils.widgets.text("pii_categories", "'email','phone','ssn','credit_card','name','address'", "5. PII Categories")
dbutils.widgets.text("pipeline_name", "otel-pii-redaction", "6. Pipeline Name")
dbutils.widgets.dropdown("redaction_pipeline_mode", "triggered", ["triggered", "continuous"], "8. Pipeline Mode")
dbutils.widgets.dropdown("redaction_trigger_frequency", "daily", ["hourly", "every 6 hours", "daily", "weekly"], "9. Trigger Frequency (triggered mode)")
dbutils.widgets.text("retention_days", "90", "7. Retention Days (blank or 0 = no deletion)")

# COMMAND ----------

import re

catalog = dbutils.widgets.get("catalog").strip()
source_schema = dbutils.widgets.get("source_schema").strip()
target_schema = dbutils.widgets.get("target_schema").strip()
table_prefix = dbutils.widgets.get("table_prefix").strip()
pii_categories = dbutils.widgets.get("pii_categories").strip()
pipeline_name = dbutils.widgets.get("pipeline_name").strip()
retention_days = dbutils.widgets.get("retention_days").strip()
pipeline_mode = dbutils.widgets.get("redaction_pipeline_mode").strip().lower()
trigger_frequency = dbutils.widgets.get("redaction_trigger_frequency").strip().lower()

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

# Validate retention_days; blank, "0", "none", or "never" disables raw-table deletion
_RETENTION_DISABLED = ("", "0", "none", "never")
retention_enabled = retention_days.lower() not in _RETENTION_DISABLED
if retention_enabled and (not retention_days.isdigit() or int(retention_days) <= 0):
    dbutils.notebook.exit(f"FAILED: Invalid Retention Days '{retention_days}'. Use a positive integer, or leave blank / 0 / none to disable deletion.")

# Validate pipeline_mode and map trigger frequency to a Quartz cron expression
_VALID_MODES = ("triggered", "continuous")
if pipeline_mode not in _VALID_MODES:
    dbutils.notebook.exit(f"FAILED: Invalid Pipeline Mode '{pipeline_mode}'. Must be one of: {', '.join(_VALID_MODES)}")

_FREQUENCY_CRON = {
    "hourly": "0 0 * * * ?",
    "every 6 hours": "0 0 0/6 * * ?",
    "daily": "0 0 0 * * ?",
    "weekly": "0 0 0 ? * MON",
}
if pipeline_mode == "triggered" and trigger_frequency not in _FREQUENCY_CRON:
    dbutils.notebook.exit(f"FAILED: Invalid Trigger Frequency '{trigger_frequency}'. Must be one of: {', '.join(_FREQUENCY_CRON)}")
trigger_cron = _FREQUENCY_CRON.get(trigger_frequency)

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
print(f"  Retention days: {retention_days if retention_enabled else 'disabled (no raw table deletion)'}")
print(f"  Pipeline mode:  {pipeline_mode}")
if pipeline_mode == "triggered":
    print(f"  Trigger freq:   {trigger_frequency} (cron: {trigger_cron})")
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
from databricks.sdk.service.workspace import ExportFormat

# Locate the pipeline SQL in the repo (assumes notebook is in a Databricks Git folder)
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_dir = os.path.dirname(notebook_path)
sql_source_path = f"{notebook_dir}/pii_redaction_pipeline.sql"

# Read via the Workspace API (robust; works for FILE or NOTEBOOK objects, avoids /Workspace FUSE-mount issues)
try:
    exported = w.workspace.export(path=sql_source_path, format=ExportFormat.AUTO)
    pipeline_sql_content = base64.b64decode(exported.content).decode()
    print(f"Read pipeline SQL ({len(pipeline_sql_content)} chars) from: {sql_source_path}")
except Exception as e:
    dbutils.notebook.exit(
        f"FAILED: Could not read pii_redaction_pipeline.sql at {sql_source_path}: {e}. "
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
    target=target_schema,
    serverless=True,
    continuous=(pipeline_mode == "continuous"),
    channel="CURRENT",
    configuration={
        "source_catalog": catalog,
        "source_schema": source_schema,
        "table_prefix": table_prefix,
        "pii_categories": pii_categories,
    },
    libraries=[PipelineLibrary(file=FileLibrary(path=pipeline_sql_workspace_path))],
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

# DBTITLE 1,Step 5b header
# MAGIC %md
# MAGIC ## Step 5b: Schedule Triggered Pipeline
# MAGIC
# MAGIC If the pipeline mode is **triggered**, this creates (or updates) a scheduled Job that triggers the pipeline on the chosen frequency. If the mode is **continuous**, the pipeline runs on its own and no scheduling Job is created.

# COMMAND ----------

# DBTITLE 1,Schedule triggered pipeline
# Schedule the pipeline to run on a recurring cadence (triggered mode only).
# Continuous pipelines run on their own and do not need a scheduling job.
from databricks.sdk.service.jobs import (
    JobSettings,
    Task,
    PipelineTask,
    CronSchedule,
    PauseStatus,
    QueueSettings,
)

trigger_job_name = f"{pipeline_name}-scheduled-trigger"

if pipeline_mode == "triggered":
    trigger_job_settings = JobSettings(
        name=trigger_job_name,
        schedule=CronSchedule(
            quartz_cron_expression=trigger_cron,
            timezone_id="America/Los_Angeles",
            pause_status=PauseStatus.UNPAUSED,
        ),
        max_concurrent_runs=1,
        tasks=[
            Task(
                task_key="run_redaction_pipeline",
                pipeline_task=PipelineTask(pipeline_id=pipeline_id),
                description="Trigger the OTel PII redaction SDP pipeline",
            )
        ],
        tags={"project": "otel-pii-redaction", "purpose": "pipeline-trigger"},
        queue=QueueSettings(enabled=True),
    )

    existing_trigger_jobs = [
        j for j in w.jobs.list(name=trigger_job_name)
        if j.settings and j.settings.name == trigger_job_name
    ]
    if existing_trigger_jobs:
        trigger_job_id = existing_trigger_jobs[0].job_id
        print(f"Trigger job '{trigger_job_name}' already exists (ID: {trigger_job_id}). Updating...")
        w.jobs.update(job_id=trigger_job_id, new_settings=trigger_job_settings)
        print(f"Trigger job updated (cron: {trigger_cron}).")
    else:
        tj = w.jobs.create(
            name=trigger_job_settings.name,
            schedule=trigger_job_settings.schedule,
            max_concurrent_runs=trigger_job_settings.max_concurrent_runs,
            tasks=trigger_job_settings.tasks,
            tags=trigger_job_settings.tags,
            queue=trigger_job_settings.queue,
        )
        trigger_job_id = tj.job_id
        print(f"Scheduled trigger job created with ID: {trigger_job_id} (cron: {trigger_cron})")
else:
    trigger_job_id = None
    print("Pipeline mode is 'continuous' — pipeline runs continuously, no scheduled trigger job created.")

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

# Continuous pipelines stay RUNNING and never reach COMPLETED — treat RUNNING as the healthy terminal state.
success_state = "RUNNING" if pipeline_mode == "continuous" else "COMPLETED"
terminal_states = (success_state, "FAILED", "CANCELED")
print(f"Waiting for pipeline (mode: {pipeline_mode}, timeout: {MAX_WAIT_SECONDS // 60}min, checking every {POLL_INTERVAL}s)...")
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
    if state in terminal_states:
        break
    time.sleep(POLL_INTERVAL)
    elapsed += POLL_INTERVAL

if elapsed >= MAX_WAIT_SECONDS and state not in terminal_states:
    dbutils.notebook.exit(f"FAILED: Pipeline timed out after {MAX_WAIT_SECONDS}s in state {state}")

if state != success_state:
    print(f"\nPipeline ended with state: {state}. Check the UI for details.")
    dbutils.notebook.exit(f"FAILED: Pipeline ended with state {state}")

if pipeline_mode == "continuous":
    print("\nPipeline is running continuously (will keep processing new data).")
else:
    print("\nPipeline completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Create Retention Cleanup Job

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

job_name = "otel-raw-retention-cleanup"

if not retention_enabled:
    # Retention disabled (blank / 0 / none): do NOT delete raw tables.
    # If a cleanup job exists from a previous deployment, pause it so it stops deleting.
    existing_jobs = [j for j in w.jobs.list(name=job_name) if j.settings and j.settings.name == job_name]
    if existing_jobs:
        job_id = existing_jobs[0].job_id
        current_settings = w.jobs.get(job_id=job_id).settings
        if current_settings.schedule:
            current_settings.schedule.pause_status = PauseStatus.PAUSED
            w.jobs.update(job_id=job_id, new_settings=current_settings)
        print(f"Retention disabled — paused existing cleanup job (ID: {job_id}). Raw tables will NOT be deleted.")
    else:
        job_id = None
        print("Retention disabled — no cleanup job created. Raw tables will NOT be deleted.")
else:
    # Read the retention notebook via the Workspace API (robust; avoids /Workspace FUSE-mount flakiness for Git-synced files)
    from databricks.sdk.service.workspace import ExportFormat
    retention_src = f"{notebook_dir}/otel_retention_cleanup"
    try:
        exported = w.workspace.export(path=retention_src, format=ExportFormat.SOURCE)
        retention_content = base64.b64decode(exported.content).decode()
    except Exception as e:
        dbutils.notebook.exit(
            f"FAILED: Could not export retention notebook from {retention_src}: {e}"
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
                        "retention_days": retention_days,
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
# MAGIC ## Step 9: Validate Redaction

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
print(f"  Pipeline mode:   {pipeline_mode}")
if pipeline_mode == "triggered":
    print(f"  Trigger Job:     {trigger_job_id} (cron: {trigger_cron})")
print(f"  Retention Job:   {job_id if job_id else 'disabled (no deletion)'}")
print(f"  Target schema:   {catalog}.{target_schema}")
print(f"  Redacted spans:  {catalog}.{target_schema}.redacted_spans")
print(f"  Redacted logs:   {catalog}.{target_schema}.redacted_logs")
print(f"  Unified view:    {catalog}.{target_schema}.{table_prefix}_trace_unified")
if host:
    print(f"\n  Pipeline UI:     https://{host}/pipelines/{pipeline_id}")
    if job_id:
        print(f"  Job UI:          https://{host}/jobs/{job_id}")
    if pipeline_mode == "triggered" and trigger_job_id:
        print(f"  Trigger Job UI:  https://{host}/jobs/{trigger_job_id}")
