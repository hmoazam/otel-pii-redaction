-- =============================================================
-- Schema & Access Control Setup
-- =============================================================
-- Run these statements to set up the target schema and access controls.
-- Replace ${...} variables with your actual values.

-- Step 1: Create target schema for redacted tables
CREATE SCHEMA IF NOT EXISTS ${target_catalog}.${target_schema};

-- Step 2: Lock down raw tables (restrict to pipeline SP / admin only)
-- Adjust group names as needed for your workspace
REVOKE SELECT ON SCHEMA ${source_catalog}.${source_schema} FROM `data_team`;

-- Step 3: Grant access to redacted schema
GRANT USE CATALOG ON CATALOG ${target_catalog} TO `data_team`;
GRANT USE SCHEMA ON SCHEMA ${target_catalog}.${target_schema} TO `data_team`;
GRANT SELECT ON SCHEMA ${target_catalog}.${target_schema} TO `data_team`;
