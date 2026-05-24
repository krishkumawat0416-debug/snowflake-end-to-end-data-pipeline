# ❄️ Snow Project — Real-Time Data Pipeline on Snowflake

> **Created by: Krish Kumawat**
> **Platform: Snowflake | Cloud: AWS S3 | Architecture: ELT Pipeline with SCD Type-2**

---

## 📌 Project Overview

**Snow Project** is an end-to-end real-time data pipeline that automatically ingests JSON files from AWS S3 and processes them inside Snowflake. The project leverages **Snowpipe**, **Streams**, and **Tasks** to build a fully automated, event-driven ELT pipeline — including a **SCD Type-2** (Slowly Changing Dimension) implementation for complete historical tracking of customer records.

The pipeline follows a **Bronze → Silver → Gold** layered architecture, where raw JSON lands first, gets flattened and typed in the silver layer, and finally gets historized into a production-ready dimension table.

---

## 🏗️ Architecture Overview

```
AWS S3 Bucket  (krishregex)
        │
        │  Auto-Ingest via SQS Event Notification
        ▼
┌──────────────────────┐
│      Snowpipe        │  ←  pro_pipe  (AUTO_INGEST = TRUE)
│    (Bronze Layer)    │
└──────────────────────┘
        │
        ▼
┌──────────────────────────────────┐
│        pipe_table (VARIANT)      │  ←  Raw JSON stored as VARIANT
│    + file_name  +  load_time     │      with source file metadata
└──────────────────────────────────┘
        │
        │   Change Data Capture
        ▼
┌──────────────────────┐
│      pipe_stream     │  ←  APPEND_ONLY Stream on pipe_table
└──────────────────────┘
        │
        ├─────────────────────────────────────────────┐
        ▼                                             ▼
┌──────────────────────┐               ┌──────────────────────────┐
│    silver_table      │               │      dim_customer         │
│   (Silver Layer)     │               │    (SCD Type-2 / Gold)    │
│    silver_task       │               │   task_scd2_customer      │
└──────────────────────┘               └──────────────────────────┘
```

---

## 🗂️ Database & Schema Structure

| Schema | Purpose |
|---|---|
| `snow_project.file_format` | Defines the JSON file format for COPY INTO |
| `snow_project.pipe_schema` | Holds the Snowpipe definition |
| `snow_project.pipe_table_schema` | Contains all Tables, Streams, and Tasks |
| `snow_project.pro_stages` | External Stage pointing to the S3 bucket |

---

## 📦 Components In Detail

### 1. 🪣 External Stage — `my_prostage`
- Connects to the AWS S3 bucket `krishregex`
- All incoming JSON files land here
- Authenticated using AWS `KEY_ID` and `SECRET_KEY`
- Can be verified using `LIST @snow_project.pro_stages.my_prostage`

---

### 2. 📥 Snowpipe — `pro_pipe`
- `AUTO_INGEST = TRUE` — triggers automatically via S3 SQS event notification
- No manual intervention needed; new files are picked up instantly
- Captures file-level metadata alongside raw data:
  - `METADATA$FILENAME` → source file name
  - `METADATA$FILE_LAST_MODIFIED` → file timestamp

---

### 3. 🗃️ Bronze Table — `pipe_table`

```sql
pipe_table (
    row_column  VARIANT,    -- Entire JSON record stored as-is
    file_name   STRING,     -- Source S3 file name
    load_time   TIMESTAMP   -- File last modified timestamp
)
```

Raw, unmodified JSON is stored here. No transformation happens at this layer — it acts as the immutable landing zone.

---

### 4. 🔄 Stream — `pipe_stream`

- Created as `APPEND_ONLY` on `pipe_table`
- Captures every new row appended to the bronze table
- Acts as the CDC (Change Data Capture) trigger for downstream tasks
- Once consumed by a task, the stream offsets advance automatically

---

### 5. 🥈 Silver Table — `silver_table`

Flattened, typed, and enriched customer records:

| Column | Source JSON Field | Type |
|---|---|---|
| `cid` | CUSTOMER_ID | STRING |
| `cname` | CUSTOMER_NAME | STRING |
| `eid` | EMAIL_ID | STRING |
| `pno` | PHONE_NUMBER | STRING |
| `city`, `state`, `country` | CITY, STATE, COUNTRY | STRING |
| `CusSeg` | CUSTOMER_SEGMENT | STRING |
| `CusSta` | CUSTOMER_STATUS | STRING |
| `CreRat` | CREDIT_RATING | STRING |
| `EffStaDT`, `EffEndDt` | EFFECTIVE_START_DT, EFFECTIVE_END_DT | STRING |
| `Iscurr` | IS_CURRENT | STRING |
| `SouSys` | SOURCE_SYSTEM | STRING |
| `LoDa`, `LoTi` | LOAD_DATE, LOAD_TIMESTAMP | STRING |
| `ReHa` | RECORD_HASH | STRING |
| `year`, `month`, `quarter`, `week_day_name` | Derived from LOAD_TIMESTAMP | INT / STRING |
| `file_name`, `load_time` | Passed from pipe_table metadata | STRING / TIMESTAMP |

---

### 6. ⚙️ Task — `silver_task`

- **Schedule**: Every 1 minute
- **Warehouse**: `compute_wh`
- Reads new records from `pipe_stream`
- Inserts flattened + typed rows into `silver_table`
- Must be explicitly resumed: `ALTER TASK silver_task RESUME`

---

### 7. 🌟 Dimension Table — `dim_customer` (SCD Type-2)

```sql
dim_customer (
    cid                 STRING,
    cname               STRING,
    eid                 STRING,
    pno                 STRING,
    city                STRING,
    state               STRING,
    country             STRING,
    CusSeg              STRING,
    CusSta              STRING,
    CreRat              STRING,
    effective_start_dt  TIMESTAMP,
    effective_end_dt    TIMESTAMP,
    is_current          STRING,     -- 'Y' = active, 'N' = expired
    record_hash         STRING
)
```

---

### 8. ⚙️ Task — `task_scd2_customer` (SCD Type-2 MERGE)

- **Schedule**: Every 1 minute
- **Condition**: Only executes when `SYSTEM$STREAM_HAS_DATA(...)` returns TRUE — no wasted compute
- **MERGE logic**:

| Scenario | Action |
|---|---|
| `cid` matches AND `record_hash` has changed | Expire old row: set `is_current = 'N'` and `effective_end_dt = CURRENT_TIMESTAMP()` |
| `cid` is new (not in dim_customer) | Insert new row with `is_current = 'Y'`, `effective_start_dt = CURRENT_TIMESTAMP()`, `effective_end_dt = NULL` |

This ensures a complete, auditable history of every customer record change over time.

---

## 🔁 End-to-End Data Flow

```
Step 1 → JSON file is uploaded to S3 bucket (krishregex)
Step 2 → S3 fires an SQS event notification
Step 3 → Snowpipe (pro_pipe) picks up the file and loads it into pipe_table
Step 4 → pipe_stream detects the newly appended rows
Step 5 → silver_task runs every minute → inserts flattened data into silver_table
Step 6 → task_scd2_customer runs every minute (only if stream has data)
          → MERGE into dim_customer applying SCD Type-2 logic
```

---

## 🚀 Setup & Deployment Instructions

### Step 1: Database & Schema Setup
```sql
CREATE OR REPLACE DATABASE snow_project;
USE snow_project;
CREATE OR REPLACE SCHEMA snow_project.file_format;
CREATE OR REPLACE SCHEMA snow_project.pipe_schema;
CREATE OR REPLACE SCHEMA snow_project.pipe_table_schema;
CREATE OR REPLACE SCHEMA snow_project.pro_stages;
```

### Step 2: File Format
```sql
CREATE OR REPLACE FILE FORMAT snow_project.file_format.json_format
TYPE = json;
```

### Step 3: External Stage (S3)
```sql
CREATE OR REPLACE STAGE snow_project.pro_stages.my_prostage
URL = 's3://krishregex'
CREDENTIALS = (
    AWS_KEY_ID     = '<your_aws_key_id>'
    AWS_SECRET_KEY = '<your_aws_secret_key>'
);
```

### Step 4: Bronze Table
```sql
CREATE OR REPLACE TABLE snow_project.pipe_table_schema.pipe_table (
    row_column  VARIANT,
    file_name   STRING,
    load_time   TIMESTAMP
);
```

### Step 5: Snowpipe
```sql
CREATE OR REPLACE PIPE snow_project.pipe_schema.pro_pipe
AUTO_INGEST = TRUE
AS
COPY INTO snow_project.pipe_table_schema.pipe_table (row_column, file_name, load_time)
FROM (
    SELECT $1, METADATA$FILENAME, METADATA$FILE_LAST_MODIFIED
    FROM @snow_project.pro_stages.my_prostage
)
FILE_FORMAT = snow_project.file_format.json_format;
```
> ⚠️ After creating the pipe, run `DESC PIPE snow_project.pipe_schema.pro_pipe` to get the **SQS ARN** and configure it in your S3 bucket's Event Notifications.

### Step 6: Silver & Dimension Tables
```sql
-- Create silver_table and dim_customer as defined in the SQL file
```

### Step 7: Stream
```sql
CREATE OR REPLACE STREAM snow_project.pipe_table_schema.pipe_stream
ON TABLE snow_project.pipe_table_schema.pipe_table
APPEND_ONLY = TRUE;
```

### Step 8: Tasks — Create & Resume
```sql
-- Silver Task
ALTER TASK snow_project.pipe_table_schema.silver_task RESUME;

-- SCD2 Task
ALTER TASK snow_project.pipe_table_schema.task_scd2_customer RESUME;
```

---

## ⚡ Challenges Faced & How They Were Solved

### Challenge 1: Snowpipe Not Auto-Ingesting Files
**Problem:** After creating the pipe with `AUTO_INGEST = TRUE`, files uploaded to S3 were not being picked up automatically.

**Root Cause:** The SQS ARN from the Snowpipe was not configured in the S3 bucket's Event Notification settings.

**Solution:**
- Ran `DESC PIPE snow_project.pipe_schema.pro_pipe` to retrieve the `notification_channel` (SQS ARN)
- Went to the S3 bucket → Properties → Event Notifications → Added the SQS ARN for `s3:ObjectCreated:*` events
- Used `ALTER PIPE pro_pipe REFRESH` to manually backfill any files that were uploaded before the notification was set up

---

### Challenge 2: Stream Consumed Before Task Could Process It
**Problem:** The `pipe_stream` was being consumed (offset advancing) even when the task failed or ran partially, causing data loss in `silver_table`.

**Root Cause:** Snowflake streams advance their offset only when a DML (INSERT/MERGE) succeeds inside a transaction. However, task failures without proper error handling left the stream in an inconsistent state during testing.

**Solution:**
- Used `SYSTEM$STREAM_HAS_DATA()` as a WHEN condition on the SCD2 task so it only fires when there is actual data — avoiding empty runs
- Added explicit checks using `SELECT * FROM pipe_stream` before and after task runs during debugging
- Ensured that each task's DML is atomic — the stream offset only advances after a successful commit

---

### Challenge 3: VARIANT Data Type Casting Issues
**Problem:** When extracting fields from the `VARIANT` column (`row_column`), some fields returned `NULL` or threw type errors — especially for `TIMESTAMP` fields like `LOAD_TIMESTAMP`.

**Root Cause:** JSON keys are case-sensitive inside Snowflake's VARIANT type. Fields stored as `LOAD_TIMESTAMP` in JSON could not be accessed using lowercase `load_timestamp`.

**Solution:**
- Ensured all JSON key references used the **exact same case** as stored in the JSON file (e.g., `row_column:LOAD_TIMESTAMP::string`)
- For timestamp parsing, used the explicit two-step cast: first to `::string`, then wrapped in `TO_TIMESTAMP()` to handle format variations safely

---

### Challenge 4: SCD2 Not Creating New Version on Record Change
**Problem:** When a customer's details changed (e.g., city updated), the MERGE was not inserting a new row — only the existing row was being updated.

**Root Cause:** The MERGE statement was matching on `cid` alone, so the `WHEN MATCHED` branch fired and updated `is_current = 'N'`, but there was no second `INSERT` for the new version of the record in the same MERGE cycle.

**Solution:**
- Restructured the MERGE so `WHEN MATCHED AND record_hash <> src.record_hash` only expires the old record
- Added logic to insert the new active version by re-running the insert for unmatched new hashes
- Validated the fix using:
```sql
SELECT cid, is_current, effective_start_dt, effective_end_dt
FROM snow_project.pipe_table_schema.dim_customer
ORDER BY cid, effective_start_dt;
```
This confirmed that changed records now appear as two rows — one expired (`is_current = 'N'`) and one active (`is_current = 'Y'`)

---

### Challenge 5: Task Running Every Minute Even With No New Data
**Problem:** `silver_task` was scheduled every minute and running warehouse compute even when `pipe_stream` had no new records, causing unnecessary credit consumption.

**Root Cause:** The task had no condition guard — it would always attempt the INSERT regardless of stream data availability.

**Solution:**
- Added `WHEN SYSTEM$STREAM_HAS_DATA('snow_project.pipe_table_schema.pipe_stream')` condition to `task_scd2_customer` so it skips execution when the stream is empty
- For `silver_task`, added monitoring via `SHOW TASKS` and `SHOW TASK HISTORY` to track actual execution cycles and identify idle runs

---

### Challenge 6: Table Already Exists Errors During Iterative Development
**Problem:** During development and schema changes, running the script repeatedly threw errors because objects already existed with different definitions.

**Solution:**
- Used `CREATE OR REPLACE` consistently for all objects (tables, stages, pipes, tasks, streams)
- Added `DROP TABLE IF EXISTS` before major table redefinitions to avoid column mismatch errors
- Maintained a clear execution order in the SQL script to avoid dependency issues (e.g., stream must exist before task that reads it)

---

## 🔍 Monitoring & Debugging Queries

```sql
-- Check Snowpipe status and pending file queue
SELECT SYSTEM$PIPE_STATUS('snow_project.pipe_schema.pro_pipe');

-- Manually refresh pipe to backfill missed files
ALTER PIPE snow_project.pipe_schema.pro_pipe REFRESH;

-- Count records landed in bronze table
SELECT COUNT(*) FROM snow_project.pipe_table_schema.pipe_table;

-- Inspect raw JSON in bronze table
SELECT row_column, file_name FROM snow_project.pipe_table_schema.pipe_table LIMIT 5;

-- Check pending records in stream
SELECT * FROM snow_project.pipe_table_schema.pipe_stream;

-- Verify silver table load
SELECT * FROM snow_project.pipe_table_schema.silver_table;

-- Validate SCD2 history
SELECT cid, is_current, effective_start_dt, effective_end_dt
FROM snow_project.pipe_table_schema.dim_customer
ORDER BY cid, effective_start_dt;

-- Task status
SHOW TASKS;
SHOW TASKS LIKE '%silver_task%';
SHOW TASKS LIKE '%scd2%';
```

---

## ⚠️ Important Notes

- **Never hardcode AWS credentials** in SQL files that go into version control — use Snowflake Secrets or IAM roles in production
- Always **SUSPEND tasks** after testing to avoid unnecessary compute charges
- The `pipe_stream` offset advances only after a successful DML commit — failed tasks do NOT consume the stream
- `RECORD_HASH` must be pre-computed and present in the source JSON for SCD2 to work correctly
- `ALTER PIPE ... REFRESH` is useful to backfill files uploaded before the SQS notification was properly configured

---

## 🧰 Tech Stack

| Technology | Role |
|---|---|
| **Snowflake** | Data Warehouse, Pipeline Orchestration Engine |
| **AWS S3** | Raw file storage and landing zone |
| **Snowpipe** | Auto-ingestion triggered by S3 events |
| **Snowflake Streams** | Change Data Capture (CDC) |
| **Snowflake Tasks** | Scheduled, condition-based automation |
| **SCD Type-2** | Full historical tracking of dimension changes |
| **JSON / VARIANT** | Semi-structured data ingestion and parsing |
| **MERGE Statement** | Upsert logic for SCD2 implementation |

---

## 👨‍💻 Author

**Krish Kumawat**
Snowflake Data Engineering Project

---

*"From raw S3 JSON to a fully historized SCD2 dimension table — automated, real-time, and production-ready."*
