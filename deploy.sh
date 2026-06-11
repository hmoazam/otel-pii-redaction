#!/bin/bash
# =============================================================
# OTel PII Redaction Demo — Redeployment Script
# =============================================================
# Usage: ./deploy.sh <WORKSPACE_HOST> <CATALOG> <SOURCE_SCHEMA> <TARGET_SCHEMA> <TABLE_PREFIX> [CUSTOM_PATTERNS_JSON]
#
# Arguments:
#   1. WORKSPACE_HOST         — e.g. https://my-workspace.cloud.databricks.com
#   2. CATALOG                — Unity Catalog name
#   3. SOURCE_SCHEMA          — Schema containing raw OTel tables
#   4. TARGET_SCHEMA          — Schema to write redacted tables into
#   5. TABLE_PREFIX           — OTel table name prefix
#   6. CUSTOM_PATTERNS_JSON   — (optional) Path to a JSON file defining custom regex patterns.
#                               If omitted, all custom pattern slots are set to empty strings (no-op).
#
# Example (no custom patterns):
#   ./deploy.sh https://my-workspace.cloud.databricks.com hanna_moazam claude_ingest traces_redacted claude_code
#
# Example (with custom patterns):
#   ./deploy.sh https://my-workspace.cloud.databricks.com hanna_moazam claude_ingest traces_redacted claude_code custom_patterns.json
#
# Custom patterns JSON format:
#   {
#     "custom_pattern_1": "EMP-\\d{6}",
#     "custom_pattern_1_replacement": "[REDACTED_EMP_ID]",
#     "custom_pattern_2": "ACCT-\\d{10}",
#     "custom_pattern_2_replacement": "[REDACTED_ACCT]"
#   }
#   Omitted slots default to empty string (skipped). Up to 5 pattern pairs supported.
#
# Prerequisites:
#   - Databricks CLI authenticated to the target workspace
#   - Unity Catalog enabled, AI Functions available
#   - A serverless SQL warehouse

set -euo pipefail

WORKSPACE_HOST="${1:?Usage: ./deploy.sh <WORKSPACE_HOST> <CATALOG> <SOURCE_SCHEMA> <TARGET_SCHEMA> <TABLE_PREFIX> [CUSTOM_PATTERNS_JSON]}"
CATALOG="${2:?Missing CATALOG}"
SOURCE_SCHEMA="${3:?Missing SOURCE_SCHEMA}"
TARGET_SCHEMA="${4:?Missing TARGET_SCHEMA}"
TABLE_PREFIX="${5:?Missing TABLE_PREFIX}"
CUSTOM_PATTERNS_JSON="${6:-}"

# Read custom pattern slots from optional JSON file, defaulting all to empty string
_read_pattern() {
  local key="$1"
  if [ -n "$CUSTOM_PATTERNS_JSON" ] && [ -f "$CUSTOM_PATTERNS_JSON" ]; then
    python3 -c "import sys,json; d=json.load(open('$CUSTOM_PATTERNS_JSON')); print(d.get('$key',''))" 2>/dev/null || echo ""
  else
    echo ""
  fi
}

CUSTOM_PATTERN_1="$(_read_pattern custom_pattern_1)"
CUSTOM_PATTERN_1_REPLACEMENT="$(_read_pattern custom_pattern_1_replacement)"
CUSTOM_PATTERN_2="$(_read_pattern custom_pattern_2)"
CUSTOM_PATTERN_2_REPLACEMENT="$(_read_pattern custom_pattern_2_replacement)"
CUSTOM_PATTERN_3="$(_read_pattern custom_pattern_3)"
CUSTOM_PATTERN_3_REPLACEMENT="$(_read_pattern custom_pattern_3_replacement)"
CUSTOM_PATTERN_4="$(_read_pattern custom_pattern_4)"
CUSTOM_PATTERN_4_REPLACEMENT="$(_read_pattern custom_pattern_4_replacement)"
CUSTOM_PATTERN_5="$(_read_pattern custom_pattern_5)"
CUSTOM_PATTERN_5_REPLACEMENT="$(_read_pattern custom_pattern_5_replacement)"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_PATH="/Workspace/Users/$(databricks auth describe --host "$WORKSPACE_HOST" 2>/dev/null | grep -oP 'User:\s+\K.*' || echo 'UNKNOWN_USER')/otel-pii-redaction"

echo "=== OTel PII Redaction Demo Deployment ==="
echo "Workspace:       $WORKSPACE_HOST"
echo "Catalog:         $CATALOG"
echo "Source:          $CATALOG.$SOURCE_SCHEMA"
echo "Target:          $CATALOG.$TARGET_SCHEMA"
echo "Table prefix:    $TABLE_PREFIX"
echo "Remote path:     $WORKSPACE_PATH"
echo "Custom patterns: ${CUSTOM_PATTERNS_JSON:-"(none — all pattern slots empty)"}"
echo ""

# Step 1: Upload files to workspace
echo "--- Step 1: Uploading files to workspace ---"
databricks workspace mkdirs "$WORKSPACE_PATH" --host "$WORKSPACE_HOST"
databricks workspace import "$WORKSPACE_PATH/pii_redaction_pipeline.sql" \
  --file "$SCRIPT_DIR/pii_redaction_pipeline.sql" \
  --format AUTO --language SQL --overwrite \
  --host "$WORKSPACE_HOST"
databricks workspace import "$WORKSPACE_PATH/otel_retention_cleanup" \
  --file "$SCRIPT_DIR/otel_retention_cleanup.py" \
  --format SOURCE --language PYTHON --overwrite \
  --host "$WORKSPACE_HOST"
echo "Files uploaded."

# Step 2: Create the target schema
echo ""
echo "--- Step 2: Creating target schema ---"
databricks sql execute --statement "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${TARGET_SCHEMA}" \
  --host "$WORKSPACE_HOST" 2>/dev/null || echo "Schema may already exist."

# Step 3: Create the SDP pipeline
echo ""
echo "--- Step 3: Creating SDP pipeline ---"
PIPELINE_JSON=$(cat <<EOF
{
  "name": "otel-pii-redaction",
  "catalog": "$CATALOG",
  "schema": "$TARGET_SCHEMA",
  "serverless": true,
  "continuous": false,
  "channel": "CURRENT",
  "configuration": {
    "source_catalog": "$CATALOG",
    "source_schema": "$SOURCE_SCHEMA",
    "table_prefix": "$TABLE_PREFIX",
    "pii_categories": "'email','phone','ssn','credit_card','name','address'",
    "custom_pattern_1": "$CUSTOM_PATTERN_1",
    "custom_pattern_1_replacement": "$CUSTOM_PATTERN_1_REPLACEMENT",
    "custom_pattern_2": "$CUSTOM_PATTERN_2",
    "custom_pattern_2_replacement": "$CUSTOM_PATTERN_2_REPLACEMENT",
    "custom_pattern_3": "$CUSTOM_PATTERN_3",
    "custom_pattern_3_replacement": "$CUSTOM_PATTERN_3_REPLACEMENT",
    "custom_pattern_4": "$CUSTOM_PATTERN_4",
    "custom_pattern_4_replacement": "$CUSTOM_PATTERN_4_REPLACEMENT",
    "custom_pattern_5": "$CUSTOM_PATTERN_5",
    "custom_pattern_5_replacement": "$CUSTOM_PATTERN_5_REPLACEMENT"
  },
  "libraries": [
    {"file": {"path": "$WORKSPACE_PATH/pii_redaction_pipeline.sql"}}
  ],
  "tags": {
    "created_by": "otel-pii-redaction-deploy-script"
  }
}
EOF
)
PIPELINE_RESULT=$(echo "$PIPELINE_JSON" | databricks pipelines create --json @- --host "$WORKSPACE_HOST" 2>&1) || true
echo "$PIPELINE_RESULT"
PIPELINE_ID=$(echo "$PIPELINE_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('pipeline_id','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
echo "Pipeline ID: $PIPELINE_ID"

# Step 4: Trigger pipeline run
if [ "$PIPELINE_ID" != "UNKNOWN" ]; then
  echo ""
  echo "--- Step 4: Triggering pipeline run ---"
  databricks pipelines start-update "$PIPELINE_ID" --host "$WORKSPACE_HOST" || echo "Failed to trigger pipeline. Start it manually from the UI."
fi

# Step 5: Create the retention cleanup job
echo ""
echo "--- Step 5: Creating retention cleanup job ---"
JOB_JSON=$(cat <<EOF
{
  "name": "otel-raw-retention-cleanup",
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "America/Los_Angeles",
    "pause_status": "UNPAUSED"
  },
  "max_concurrent_runs": 1,
  "tasks": [{
    "task_key": "retention_cleanup",
    "notebook_task": {
      "notebook_path": "$WORKSPACE_PATH/otel_retention_cleanup",
      "base_parameters": {"retention_days": "90", "source_catalog": "$CATALOG", "source_schema": "$SOURCE_SCHEMA", "table_prefix": "$TABLE_PREFIX"},
      "source": "WORKSPACE"
    },
    "description": "Delete rows older than retention_days and vacuum raw OTel tables",
    "environment_key": "default"
  }],
  "tags": {"project": "otel-pii-redaction", "purpose": "retention-cleanup"},
  "queue": {"enabled": true},
  "environments": [{"environment_key": "default", "spec": {"client": "1"}}]
}
EOF
)
echo "$JOB_JSON" | databricks jobs create --json @- --host "$WORKSPACE_HOST" || echo "Failed to create job."

# Step 6: Create unified view (after pipeline completes)
echo ""
echo "--- Step 6: Note ---"
echo "After the pipeline run completes, create the unified view by running:"
echo "  unified_view.sql (replace \${...} variables with: $CATALOG, $TARGET_SCHEMA, $TABLE_PREFIX)"
echo ""
echo "=== Deployment complete ==="
echo "Pipeline ID: $PIPELINE_ID"
echo "Monitor at: $WORKSPACE_HOST/#joblist/pipelines/$PIPELINE_ID"
