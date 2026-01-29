# Sample Reports

## Estimated Time vs Actual Time

### 1. Fetch the data

```sh
uv run --with-requirements requirements.txt dlt_fetch_timecamp.py \
   --from 2025-12-01 --to 2025-12-31 \
   --datasets entries,tasks \
   --format jsonl \
   --output ./output
```

> **Note:** For accurate "Cumulative time tracked", fetch a wider date range (or all historical data).
> The cumulative column will only reflect data within the fetched period.

### 2. Run DuckDB query

```sql
-- Budget vs Tracked Time Report
-- Replicates TimeCamp's Estimated Time vs Actual Time report
-- Includes time from all subtasks (recursive rollup)

-- Helper macro for formatting seconds as "Xh YYm"
CREATE OR REPLACE MACRO format_duration(seconds) AS 
    CONCAT(
        CAST(CAST(FLOOR(seconds / 3600) AS INTEGER) AS VARCHAR), 'h ',
        LPAD(CAST(CAST(FLOOR((seconds % 3600) / 60) AS INTEGER) AS VARCHAR), 2, '0'), 'm'
    );

-- Set variables for the reporting period (adjust as needed)
SET VARIABLE report_start = '2025-12-01';
SET VARIABLE report_end = '2025-12-31';

WITH RECURSIVE
-- Load tasks data
tasks AS (
    SELECT 
        task_id,
        parent_id,
        name,
        COALESCE(budgeted, 0) as budgeted,  -- in seconds
        task_breadcrumb
    FROM read_json_auto('./output/timecamp/tasks.*.jsonl')
),

-- Load entries data  
entries AS (
    SELECT
        task_id,
        CAST(duration AS INTEGER) as duration,  -- in seconds
        date
    FROM read_json_auto('./output/timecamp/entries.*.jsonl')
),

-- Recursive CTE: map each task to itself AND all its ancestors
-- This allows us to attribute time entries to parent tasks
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
    WHERE t.parent_id > 0
),

-- Calculate cumulative (all-time) tracked time (including subtasks)
tracked_cumulative AS (
    SELECT 
        th.ancestor_id as task_id,
        SUM(e.duration) as cumulative_seconds
    FROM entries e
    JOIN task_hierarchy th ON e.task_id = th.descendant_id
    GROUP BY th.ancestor_id
),

-- Main report - only tasks with budget > 0
report AS (
    SELECT 
        t.name,
        t.task_breadcrumb as breadcrumb,
        COALESCE(tc.cumulative_seconds, 0) as cumulative_seconds,
        t.budgeted as budgeted_seconds,
        t.budgeted - COALESCE(tc.cumulative_seconds, 0) as left_seconds
    FROM tasks t
    LEFT JOIN tracked_cumulative tc ON t.task_id = tc.task_id
    WHERE t.budgeted > 0  -- Only show tasks with a budget
)

SELECT 
    name as "Name",
    breadcrumb as "Breadcrumb",
    format_duration(cumulative_seconds) as "Cumulative",
    CASE 
        WHEN left_seconds < 0 
        THEN CONCAT('-', format_duration(ABS(left_seconds)))
        ELSE format_duration(left_seconds)
    END as "Left",
    format_duration(budgeted_seconds) as "Budgeted"
FROM report
ORDER BY cumulative_seconds DESC;
```

### 3. Run with DuckDB CLI

```sh
# Install DuckDB if needed: brew install duckdb

# Run the query from a file
duckdb < budget_report.sql

# Or run inline (copy the SQL above)
duckdb -c "..."
```

### Sample Output

```
┌────────────────────────────────┬──────────────────────────────────────────────────────────┬────────────┬───────────┬───────────┐
│ Name                           │ Breadcrumb                                               │ Cumulative │ Left      │ Budgeted  │
├────────────────────────────────┼──────────────────────────────────────────────────────────┼────────────┼───────────┼───────────┤
│ [MAR] SEO                      │ [MAR] SEO                                                │ 348h 45m   │ 151h 14m  │ 500h 00m  │
│ [ORG] Departments meetings     │ [ORG] Departments meetings                               │ 143h 05m   │ -123h 05m │ 20h 00m   │
│ [ORG] Education                │ [ORG] Education                                          │ 21h 15m    │ -1h 15m   │ 20h 00m   │
│ [TCD-8572] Zmiana design...    │ Jira / TimeCamp DEV / [TCD-2851] Architektura - Front... │ 0h 00m     │ 0h 01m    │ 0h 01m    │
└────────────────────────────────┴──────────────────────────────────────────────────────────┴────────────┴───────────┴───────────┘
```

> **Note:** "Cumulative" includes time from all subtasks recursively.
> "Left" = Budgeted - Cumulative (negative means over budget).