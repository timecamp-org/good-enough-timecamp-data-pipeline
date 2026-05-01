# Project Cumulative vs Budgeted Report

## 1. Fetch Project Data

```sh
uv run --with-requirements requirements.txt dlt_fetch_timecamp.py \
   --from 2020-01-01 --to 2026-05-01 \
   --datasets entries,tasks \
   --format jsonl \
   --output ./output
```

> **Note:** For accurate all-time project totals, fetch all historical entries.
> The cumulative column will only reflect data within the fetched period.

## 2. Run Project DuckDB Query

```sql
-- Project Budget vs Tracked Time Report
-- One row per first-level project
-- Cumulative includes tracked time from the project and all subtasks recursively
-- Budgeted sums budgets from the project and all subtasks recursively

-- Helper macro for formatting seconds as decimal hours
CREATE OR REPLACE MACRO format_hours(seconds) AS
    printf('%.4f', seconds / 3600.0);

WITH RECURSIVE
-- Load tasks data
tasks AS (
    SELECT
        CAST(task_id AS VARCHAR) as task_id,
        CASE
            WHEN parent_id IS NULL OR CAST(parent_id AS VARCHAR) IN ('', '0')
            THEN NULL
            ELSE CAST(parent_id AS VARCHAR)
        END as parent_id,
        name,
        COALESCE(CAST(budgeted AS INTEGER), 0) as budgeted  -- in seconds
    FROM read_json_auto('./output/timecamp/tasks.*.jsonl')
),

-- Load entries data
entries AS (
    SELECT
        CAST(task_id AS VARCHAR) as task_id,
        CAST(duration AS INTEGER) as duration  -- in seconds
    FROM read_json_auto('./output/timecamp/entries.*.jsonl')
),

-- Recursive CTE: map each task to itself AND all its ancestors
task_hierarchy AS (
    -- Base case: each task maps to itself
    SELECT
        task_id as descendant_id,
        task_id as ancestor_id
    FROM tasks

    UNION ALL

    -- Recursive case: if task has a parent, map descendant to parent's ancestors
    SELECT
        th.descendant_id,
        t.parent_id as ancestor_id
    FROM task_hierarchy th
    JOIN tasks t ON th.ancestor_id = t.task_id
    WHERE t.parent_id IS NOT NULL
),

-- First-level tasks are projects
projects AS (
    SELECT
        task_id,
        name
    FROM tasks
    WHERE parent_id IS NULL
),

-- Map each project to itself and all descendant tasks
project_descendants AS (
    SELECT
        p.task_id as project_id,
        p.name as project_name,
        th.descendant_id
    FROM projects p
    JOIN task_hierarchy th ON th.ancestor_id = p.task_id
),

-- Calculate cumulative tracked time for each project tree
tracked_cumulative AS (
    SELECT
        pd.project_id,
        SUM(e.duration) as cumulative_seconds
    FROM project_descendants pd
    JOIN entries e ON e.task_id = pd.descendant_id
    GROUP BY pd.project_id
),

-- Calculate total budget for each project tree
budgeted_total AS (
    SELECT
        pd.project_id,
        SUM(t.budgeted) as budgeted_seconds
    FROM project_descendants pd
    JOIN tasks t ON t.task_id = pd.descendant_id
    GROUP BY pd.project_id
)

SELECT
    p.name as "Project",
    format_hours(COALESCE(tc.cumulative_seconds, 0)) as "Cumulative",
    format_hours(COALESCE(bt.budgeted_seconds, 0)) as "Budgeted"
FROM projects p
LEFT JOIN tracked_cumulative tc ON p.task_id = tc.project_id
LEFT JOIN budgeted_total bt ON p.task_id = bt.project_id
ORDER BY COALESCE(tc.cumulative_seconds, 0) DESC;
```

## 3. Run Project Query With DuckDB CLI

```sh
# Install DuckDB if needed: brew install duckdb

# Run the query from a file
duckdb < project_budget_report.sql

# Or run inline (copy the SQL above)
duckdb -c "..."
```

## Sample Output

```text
┌────────────────────────────┬────────────┬───────────┐
│ Project                    │ Cumulative │ Budgeted  │
├────────────────────────────┼────────────┼───────────┤
│ [MAR] Marketing            │ 512.5000   │ 700.0000  │
│ Jira                       │ 420.2500   │ 450.0000  │
│ [ORG] Organization         │ 164.3333   │ 40.0000   │
└────────────────────────────┴────────────┴───────────┘
```

> **Note:** "Project" means first-level tasks (`parent_id = 0` or empty).
> "Budgeted" is the sum of all budgets in that project tree, including the project itself.
