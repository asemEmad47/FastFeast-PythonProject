# Automated ETL Pipeline Builder

> **Config-Driven · Near Real-Time · OLTP → OLAP · Python · Modular Architecture**

An automated ETL tool that builds and runs your entire data pipeline from a single pipeline config file — no boilerplate, no manual wiring. Define your schema, adjust the config (or let any AI tool generate it for you), and your pipeline is up and running in minutes.

**FastFeast** is the example application powering this repo — a rapidly growing food delivery platform used here to demonstrate the full capabilities of the framework across real-world batch and micro-batch data flows.

![Overview](Images/Overview%20Image.png)

---

## Table of Contents

- [Business Context](#-business-context)
- [High-Level Architecture](#-high-level-architecture)
- [Module Breakdown](#-module-breakdown)
- [Design Patterns](#-design-patterns)
- [Pipeline Flow](#-pipeline-flow)
- [Input Files & Data Model](#-input-files--data-model)
- [Validation System](#-validation-system)
- [LookUp Strategy](#-lookup-strategy)
- [PII Handling](#-pii-handling)
- [Metrics & Audit](#-metrics--audit)
- [Alerting](#-alerting)
- [Configuration](#-configuration)
- [Project Structure](#-project-structure)
- [Getting Started](#-getting-started)
- [How to Run](#-how-to-run)

---

## Business Context

FastFeast connects **Customers**, **Restaurants**, **Drivers**, and **Support Agents**. As the platform scaled across cities, operational analytics became critical for monitoring delivery SLAs, complaint rates, driver performance, and revenue impact.

### Problems the Pipeline Solves

| Business Problem | Technical Solution |
|---|---|
| No real-time SLA visibility | SLA breach fields computed at fact-load time |
| Duplicate records corrupting metrics | Idempotent LookUp dedup before every load |
| Orphan FK references breaking reports | FK check with quarantine before every load |
| PII leaking into analytics layer | `PIIMask` component hashes sensitive fields |
| Pipeline halting on one bad file | Fault-tolerant flow — skip, log, alert, continue |
| No pipeline observability | `Audit` writes quality metrics to DWH after every file |
---

## High-Level Architecture
![High Level Architecture](Images/High%20Level%20Architecture.png)
---

## Module Breakdown

| Module | Classes | Responsibility |
|---|---|---|
| `ETLModule` | `Task`, `EmailTask`, `DataFlowTask`, `FactLoadTask`, `DataFlowComponent`, `WorkFlow` | Orchestration, component chain execution |
| `ValidationModule` | `Validator`, `SchemaValidator`, `RowsValidator`, `ValidatorContext` | Two-layer validation with Strategy pattern |
| `BatchModule` | `Batch`, `MicroBatch`, `BatchReader`, `FileTracker` | Thread management, file discovery, idempotency |
| `AppManager` | `AppManager` | Startup, dependency injection, thread launch |
| `RegistryModule` | `DataRegistry`, `ConfFileParser` | Central mapping hub, config parsing |
| `DBLayer` | `DatabaseManager` | Singleton connection pool |
| `RepositoryLayer` | `BaseRepository<T>` + 7 concrete repos | All DB access abstracted behind typed repos |
| `AuditModule` | `Audit`, `QualityReport`, `PipelineRunLog` | Metrics accumulation, DWH persistence |
| `Models` | `BaseModel` + 17 entity models | SQLAlchemy ORM entities |

---

## Design Patterns

### 1. Factory — `ReadFromSourceFactory`

**Problem:** Files arrive in both CSV and JSON formats. The rest of the pipeline must not care about format.

**Solution:** A factory inspects the filename and returns the correct reader. Both readers implement the same `DataFlowComponent` interface so downstream components call `do_task()` identically.

```
ReadFromSourceFactory
  + create_source(file_name, DataRegistry, ConfFileParser): ReadFromSource
        │
        ├── ".csv"  ──► ReadFromCSV
        └── ".json" ──► ReadFromJSON
```

Adding a new format (e.g., Parquet) requires one new class and one line in the factory — nothing else changes.

---

### 2. Repository — `BaseRepository<T>`

**Problem:** The pipeline needs to read from and write to many different tables. SQL must not be scattered across business logic.

**Solution:** Every entity has its own typed repository that inherits from a generic base. All database interactions go through repositories only — `LoadToTarget`, `LookUp`, and `Audit` never touch raw SQL or ORM sessions directly.

```
BaseRepository<T>
  - database_manager: DatabaseManager      ← injected, not created
  + add(T): bool
  + add_many(List<T>): bool
  + get_by_id(entity_id): T
  + get_all(): List<T>
  + get_by_attribute(attr, value): List<T>
  + get_existing_ids(id_col, ids: set): set ← bulk lookup — ONE query
  + delete_by_id(entity_id): bool
  + update(entity_id, **kwargs): bool
```

**Concrete repositories:**
`DimCustomerRepository`,`DimDriverRepository`, `DimRestaurantRepository`, `DimAgentRepository`, `FactRepository`, `OrphaneRepository`, `RejectedRepository`

---

### 3. Singleton — `DatabaseManager`

**Problem:** The database connection pool must be created exactly once and shared across all threads and all repositories. Creating a new pool per file would exhaust DB connections immediately.

**Solution:** `DatabaseManager` uses the Singleton pattern — the first call creates the instance; every subsequent call returns the same object.

```python
class DatabaseManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize_pool()
        return cls._instance

    def get_session(self) -> Session: ...
    def session_scope(self) -> ContextManager: ...  # handles commit/rollback
```

`AppManager.initialize_variables()` calls `DatabaseManager()` once. Both Batch and MicroBatch threads share this single pool. Thread safety is handled by SQLAlchemy's scoped session.

---

### 4. Registry — `DataRegistry`

**Problem:** `WorkFlow`, `ReadFromSource`, `LookUp`, and `LoadToTarget` all need to know: *"For this file, which repository do I use? Which ORM model? Which config section?"* This mapping must live in one place.

**Solution:** `DataRegistry` is the single source of truth that maps a filename to its repository, model, config key, lookup strategy, and in-memory cache. It also owns the LookUp cache for small/stable dimension tables.

```
DataRegistry
  + get_repository_for_file(file_name): BaseRepository
  + get_model_for_file(file_name): BaseModel
  + get_table_conf(file_name): string
```
---

### 5. Strategy — `ValidatorContext`

**Problem:** The pipeline needs two fundamentally different validation modes — schema-level (reject whole file) and row-level (reject individual rows). These must be swappable without changing orchestration code.

**Solution:** `ValidatorContext` holds a `Validator` reference and delegates to it. `WorkFlow` swaps the strategy between schema and row validation.

```
ValidatorContext
  - validator: Validator
  + set_validator(validator: Validator): void
  + validate(df, BaseModel): (bool, List<string>, DataFrame)
       │
       ├── SchemaValidator  → whole-file check → fail = skip entire file
       └── RowsValidator    → per-row check   → fail = quarantine bad rows
```

```python
# Inside WorkFlow — clean swap, zero branching
context.set_validator(SchemaValidator())
ok, errors, df = context.validate(df, model)
if not ok:
    quarantine_writer.write_rejected_rows(df, errors)
    return  # skip file

context.set_validator(RowsValidator())
ok, errors, clean_df = context.validate(df, model)
# bad rows already separated internally, clean_df continues
```

---

### 6. Composite Chain — `DataFlowTask` + `DataFlowComponent`

**Problem:** Each table's pipeline is a different ordered sequence of operations. Writing a bespoke function for each table is not maintainable.

**Solution:** `DataFlowTask` holds a dictionary the key is the source name and the value is the list of components `Dict<DataFlowComponent>` and calls `do_task()` on each in sequence. The task doesn't know what components do — it just chains them. Adding a step to any table's pipeline is one line of config.

```
DataFlowTask
  - components: Dict<DataFlowComponent>
  + do_task(): (bool, List<string>)
      │
      ├── ReadFromSource.do_task()      → raw DataFrame
      ├── ValidatorComponent.do_task()  → clean DataFrame
      ├── PIIMask.do_task()             → masked DataFrame
      ├── LookUp.do_task()  (dedup)     → deduped DataFrame
      ├── LookUp.do_task()  (orphan)    → valid DataFrame
      ├── Join.do_task()                → enriched DataFrame  [optional]
      └── LoadToTarget.do_task()        → written to DWH
```

**`FactLoadTask`** is a specialised `Task` that enforces dimension-first loading:

```
FactLoadTask
  - dim_task:  DataFlowTask   → runs dimension load first
  - fact_task: DataFlowTask   → runs fact load only after dims confirmed
  + do_task(): (bool, List<string>)
```

---

### 7. Facade — `WorkFlow`

**Problem:** `Batch` and `MicroBatch` should not need to know about validators, lookups, registries, audits, or alerting. They just process files.

**Solution:** `WorkFlow.orchestrate()` is the facade that hides all per-file complexity behind one method call. Threads call it; everything else happens inside.

```
WorkFlow
  - files: List<string>
  - validator: ValidatorContext
  - registry: DataRegistry
  - parser: ConfFileParser
  - tasks: List<Task>
  - audit: Audit
  - alerter: EmailTask
  - file_tracker: FileTracker
  + orchestrate(): void
  - _trigger_alert(message: string): void  ← async, non-blocking
```

---


## Pipeline Flow

The pipeline execution is organized into **four deterministic phases** orchestrated by the `WorkFlow` facade.
Both **Batch** and **MicroBatch** threads send files to the same workflow engine.

### Workflow Orchestration

WORKFLOW.orchestrate()

  audit.file_start_time = now()
  result_dfs = do_before_join_action()
  joined_dfs = do_join_action()
  final_dfs  = do_after_action()

  ok = load_to_fact.do_task(final_dfs)

  IF NOT ok
      trigger_alert("Load phase failed")
      RETURN

  FOR file IN files
      file_tracker.mark_processed(file)

  file_tracker.move_files_to_archive(files)

  audit.persist_to_dwh(run_log_repo)
  audit.LogSuccess("Pipeline completed successfully")

---

### Phase 1 — Before Join Tasks

Components executed:

- ReadFromSource
- ValidatorComponent
- PIIMask
- QuarantineWriter

---

### Phase 2 — Join Phase

Fact tables are enriched using dimension tables cached in `DataRegistry`.

FOR each fact table:
    fact_df = registry.get_df(fact_table)

    FOR each dimension:
        right_df = registry.get_df(dim_table)

        fact_df = Join.do_task(
            fact_df,
            right_df,
            left_on,
            right_on,
            join_type
        )

---

### Phase 3 — After Join Tasks

Components executed:

- Transformer
- OrphansHandler
- DuplicatesLookUp
- OrphanesLookUp

FOR task IN after_join_tasks:
    task.do_task()

---

### Phase 4 — Load Phase

Load order:

Dimensions → Facts

FACTLOADTASK.do_task():

FOR dim_component:
    repo.add_many(df)

FOR fact_component:
    repo.add_many(df)

---

### Audit

audit.persist_to_dwh(run_log_repo)
audit.LogSuccess()

Metrics stored:

- total_records
- passed_records
- failed_records
- duplicate_rate
- orphan_rate
- quarantined_count
- null_rate

---

### File Completion

file_tracker.mark_processed(file)
file_tracker.move_files_to_archive(file)

---

### Fault Tolerance

Errors never stop the pipeline.

Error → Audit.LogFailure() → trigger_alert() → continue processing


## Input Files & Data Model

### Source Files

**Batch — daily, full snapshot**

| File | Format | Loads Into |
|---|---|---|
| `customers.csv` | CSV | `dim_customer` |
| `drivers.csv` | CSV | `dim_driver` |
| `restaurants.json` | JSON | `dim_restaurant` |
| `agents.csv` | CSV | `dim_agent` |
| `cities.json` | JSON | `dim_city` |
| `regions.csv` | CSV | `dim_region` |
| `reasons.csv` | CSV | `dim_reason` |
| `categories.csv` | CSV | `dim_resturant` |
| `segments.csv` | CSV | `dim_customer` |
| `teams.csv` | CSV | `dim_agent` |
| `channels.csv` | CSV | `dim_channel` |
| `priorities.csv` | CSV | `Not Loaded` |
| `reason_categories.csv` | CSV | `dim_reason` |

**Micro-Batch — incremental, irregular intervals**

| File | Format | Loads Into |
|---|---|---|
| `orders.json` | JSON | `fact_ticket` |
| `tickets.csv` | CSV | `fact_ticket` |
| `ticket_events.json` | JSON | `fact_ticket` |

### OLAP Star Schema

![dimensional model](Images/dimendsionalModel.drawio.png)

### SLA Fields Computed at Load Time

These fields are not in the source — they are calculated during `fact_ticket` load:

| Computed Field | Formula |
|---|---|
| `sla_first_response_breached` | `first_response_at > sla_first_due_at` |
| `sla_resolve_breached` | `resolved_at > sla_resolve_due_at` |
| `first_response_time_min` | `(first_response_at − created_at)` in minutes |
| `resolution_time_min` | `(resolved_at − created_at)` in minutes |
| `is_reopened` | detected from ticket_event status transitions |

---

## Validation System

### Schema Validation — file-level gate

Entire file is quarantined and skipped if any of these fail:

| Check | Example Failure |
|---|---|
| Required columns present | `agent_id` column missing entirely |
| Column data types correct | `order_amount` column arrives as string |
| Mandatory column-level nulls | All values in `customer_id` are null |

### Row-Level Validation — per-record filter

Bad rows are quarantined; the clean DataFrame continues. Pipeline never stops.

| Check | Example Failure |
|---|---|
| Email format | `"notanemail"`, `"@no-domain"` |
| Phone format | `"abc1234"`, empty string |
| Date parseability | `"N/A"`, `"invalid-date"` |
| Numeric range | Negative `order_amount`, `rating > 5` |
| Null on required field | `customer_id` is null on a row |
| Categorical value | `status` not in `{open, pending, resolved, closed}` |

### Quarantine

Every rejected row is written to `rejected table` with:
- Batch Source
- Rejection reason
- Rejection timestamp
- Original row data

---

## 🔍 LookUp Strategy

`LookUp` is used for two purposes: **duplicate detection** and **orphan FK checks**. The golden rule is: **never query the DB row by row**. One query per batch, always.

### Two-Tier Strategy

**Tier 1 — In-Memory Cache** (small, stable, batch-only tables)

Refreshed at the start of each new batch day. Zero DB calls during the pipeline run for these tables.

```
Cached at startup:
  priority, channel, reason, reason_category,
  category, team, segment, city, region,
  restaurants, agents
```

**Tier 2 — Bulk IN Query** (large, continuously growing tables)

One query per micro-batch, regardless of DataFrame size:

```python
# Collect all FKs from the incoming DataFrame
incoming_ids = set(df["customer_id"].dropna().unique())

# ONE round trip to DB — not one per row
existing_ids = repo.get_existing_ids("customer_id", incoming_ids)
# SQL: SELECT customer_id FROM dim_customer WHERE customer_id IN (1, 2, ...)

# Pure pandas set operation — no loop
orphan_mask = ~df["customer_id"].isin(existing_ids)
orphan_df   = df[orphan_mask]
clean_df    = df[~orphan_mask]
```

```
Bulk query tables:
  customers, drivers, orders, tickets, ticket_events
```

### Per-Table Strategy Config

```yaml
files:
  agents:
    file_name: agents.csv
    file_type: dynamic
    model_class: Agent
    required_fields:
      - agent_id
      - agent_name
      - agent_email
      - agent_phone
      - hire_date
    pii_fields:
      - agent_email
      - agent_phone

tables:
  RegionsDim:
    source: [regions]
    schema: FASTFEASTDWH
    target_table: RegionsDim
    table_type: static_dimension
    repository: RegionRepository
    primary_key: region_id
    foreign_keys:
      city_id: 
        dim_table: CitiesDim
        pk_column: city_id
    keep_columns:
      - region_id
      - region_name
      - city_id
      - delivery_base_fee
    required_fields:
      - region_id
      - region_name
      - delivery_base_fee
```

---


## PII Handling

No raw PII is ever stored in the analytics layer. `PIIMask` runs after row validation and before any data reaches the DWH. Masking uses **SHA-256** — values are deterministic (same input → same hash) so joins still work, but the original value is unrecoverable from the warehouse.

| Entity | Masked Fields |
|---|---|
| Customer | `email`, `phone` |
| Agent | `agent_email`, `agent_phone` |

---

## Metrics & Audit

`Audit` accumulates metrics per file and writes one row to `pipeline_run_log` in the DWH after every file completes — success or failure.

### Quality Metrics Tracked

| Metric | Description |
|---|---|
| `total_records` | All records in the incoming file |
| `null_counts` | Per-column null count |
| `duplicate_count` | Records filtered by dedup LookUp |
| `orphan_count` | Records quarantined by FK check |
| `quarantined_count` | Total rows rejected across all steps |
| `processing_latency_ms` | Time from file arrival to load complete |
| `file_success` | True if file loaded without critical error |

### Business Analytics Views (DWH — not pipeline code)

All business reporting lives as SQL views on the fact tables. The pipeline does not compute business KPIs — it only loads clean data and computes the physical SLA fields.

```sql
-- SLA breach rate by city
CREATE VIEW v_sla_breach_rate AS
SELECT
    c.city_name,
    ROUND(
        COUNT(*) FILTER (WHERE f.sla_resolve_breached) * 100.0 / COUNT(*), 2
    ) AS breach_rate_pct
FROM fact_ticket f
JOIN dim_region r ON f.region_id = r.region_id
JOIN dim_city   c ON r.city_id   = c.city_id
GROUP BY c.city_name;

-- Top complaint reasons
CREATE VIEW v_top_reasons AS
SELECT
    r.reason_name,
    rc.category_name,
    COUNT(*) AS ticket_count
FROM fact_ticket f
JOIN dim_reason          r  ON f.reason_id   = r.reason_id
JOIN dim_reason_category rc ON r.reason_category_id = rc.reason_category_id
GROUP BY r.reason_name, rc.category_name
ORDER BY ticket_count DESC;
```

---

## Alerting

Alerts fire **asynchronously in a background thread** — they never block pipeline execution.

**Alert triggers:**
- File read failure
- Schema validation failure
- Database write failure
- Orphan rate exceeds configured threshold
- Any unhandled critical exception

**No alert is sent for successful processing.**

```python
# Non-blocking — pipeline continues immediately
def _trigger_alert(self, message: str) -> None:
    thread = threading.Thread(
        target=self.alerter.do_task,
        args=(message,),
        daemon=True
    )
    thread.start()
```

---

## Configuration

All behavior is driven by `config/settings.py`. No values are hardcoded in the codebase.

```python

DATABASE_URL = os.getenv("FF_DATABASE_URL", "postgresql://user:pass@localhost/fastfeast")

ALERT_SMTP_HOST      = os.getenv("ALERT_SMTP_HOST",      "smtp.gmail.com")
ALERT_SMTP_PORT      = int(os.getenv("ALERT_SMTP_PORT",  "587"))
ALERT_FROM_EMAIL     = os.getenv("ALERT_FROM_EMAIL",     "asememad984@gmail.com")
ALERT_TO_EMAIL       = os.getenv("ALERT_TO_EMAIL",       "asememad590@gmail.com")
ALERT_EMAIL_PASSWORD = os.getenv("ALERT_EMAIL_PASSWORD", "tpfsajouaawlqmzw")

# Snow flake part
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE",  "FASTFEAST")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA",    "FASTFEASTDWH")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "FASTFEAST_WH")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE",      "FASTFEAST_ADMIN")
```

---

## Project Structure

```
fastfeast-pipeline/
│
├── main.py
├── config/
│   └── pipeline.yaml
│
├── app/
│   ├── app_manager.py
│   │
│   ├── etl/
│   │   ├── workflow.py
│   │   ├── task.py
│   │   ├── email_task.py
│   │   ├── data_flow_task.py
│   │   ├── fact_load_task.py
│   │   └── components/
│   │       ├── data_flow_component.py
│   │       ├── read_from_source.py
│   │       ├── read_from_csv.py
│   │       ├── read_from_json.py
│   │       ├── read_from_source_factory.py
│   │       ├── validator_component.py
│   │       ├── pii_mask.py
│   │       ├── lookup.py
│   │       ├── join.py
│   │       ├── filter.py
│   │       ├── load_to_target.py
│   │       └── quarantine_writer.py
│   │
│   ├── validation/
│   │   ├── validator.py
│   │   ├── validator_context.py
│   │   ├── schema_validator.py
│   │   └── rows_validator.py
│   │
│   ├── batch/
│   │   ├── batch.py
│   │   ├── micro_batch.py
│   │   ├── batch_reader.py
│   │   └── file_tracker.py
│   │
│   ├── registry/
│   │   ├── data_registry.py
│   │   └── conf_file_parser.py
│   │
│   ├── db/
│   │   └── database_manager.py       # Singleton
│   │
│   ├── repositories/
│   │   ├── base_repository.py
│   │   ├── customer_repository.py
│   │   ├── fact_repository.py
│   │   ├── driver_repository.py
│   │   ├── restaurant_repository.py
│   │   ├── agent_repository.py
│   │
│   ├── models/
│   │   ├── base_model.py
│   │   ├── customer.py    ├── order.py       ├── ticket.py
│   │   ├── ticket_event.py├── driver.py      ├── restaurant.py
│   │   ├── agent.py       ├── region.py      ├── city.py
│   │   ├── priority.py    ├── channel.py     ├── reason.py
│   │   ├── reason_category.py ├── category.py├── team.py
│   │   └── segment.py
│   │
│   └── audit/
│       ├── audit.py
│       ├── quality_report.py
│       └── pipeline_run_log.py
│
├── data/
│   ├── input/
│   │   ├── batch/YYYY-MM-DD/        # 13 dimension source files
│   │   └── stream/YYYY-MM-DD/HH/    # orders, tickets, ticket_events
│   └── quarantine/                  # rejected rows land here
│
├── logs/                            # structured, rotated logs
│
├── scripts/
│   ├── generate_master_data.py
│   ├── generate_batch_data.py
│   ├── generate_stream_data.py
│   ├── add_new_customers.py
│   └── add_new_drivers.py
│
└── tests/
    ├── test_validators.py
    ├── test_lookup.py
    ├── test_repositories.py
    └── test_pipeline.py
```

---

## Getting Started

This is an **automated ETL pipeline builder** — define your schema in a pipeline config file and the pipeline is constructed and executed for you in minutes. No boilerplate, no manual wiring.

---

### Step 1 — Set Up a Virtual Environment (Windows)

It is strongly recommended to run the project inside a virtual environment to isolate dependencies.

```bash
# 1. Create the virtual environment
python -m venv venv

# 2. Activate it
venv\Scripts\activate

# 3. Confirm it is active — your terminal prompt should show (venv)
```

> To deactivate the environment at any time, simply run `deactivate`.

---

### Step 2 — Install Dependencies

With the virtual environment active, install all required packages:

```bash
pip install -r requirements.txt
```

---

### Step 3 — Configure Your Credentials & Pipeline

The project ships with example files inside the **`FilesExamples/`** directory. These files are **git-ignored** from the main project and serve as templates you copy and fill in.

```
FilesExamples/
├── .env.example          ← credentials template
└── conf.example/         ← pipeline & data configuration templates
    ├── batch.yaml
    └── micro_batch.yaml
```

#### 3a — Set Up Your `.env`

Copy the example env file to the project root and fill in your own credentials:

```bash
copy FilesExamples\.env.example .env
```

Then open `.env` and replace the placeholder values with your actual credentials:

```env
# Database
FF_DATABASE_URL=postgresql://YOUR_USER:YOUR_PASSWORD@YOUR_HOST/YOUR_DB

# Email Alerts
ALERT_SMTP_HOST=smtp.gmail.com
ALERT_SMTP_PORT=587
ALERT_FROM_EMAIL=your_alert_sender@gmail.com
ALERT_TO_EMAIL=your_inbox@gmail.com
ALERT_EMAIL_PASSWORD=your_app_password

# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=YOUR_DATABASE
SNOWFLAKE_SCHEMA=YOUR_SCHEMA
SNOWFLAKE_WAREHOUSE=YOUR_WAREHOUSE
SNOWFLAKE_ROLE=YOUR_ROLE
```

#### 3b — Set Up Your Pipeline Config

Copy the example conf files into the `conf/` directory:

```bash
# Copy the entire example conf folder into the conf directory
xcopy FilesExamples\conf.example conf /E /I
```

Then open the copied files in `conf/` and adjust them to match your batch and micro-batch data sources, paths, and pipeline definitions.

> 💡 **Pro tip — AI-powered pipeline generation:** The pipeline is built entirely from the config files in `conf/`. You can describe your schema to any AI tool (ChatGPT, Claude, etc.), paste in the example config structure, and ask it to generate the pipeline config for your tables. Your full ETL pipeline will be up and running in minutes — no code changes needed.

---

## How to Run

### Prerequisites

```bash
pip install -r requirements.txt
# Python 3.10+, SQLAlchemy, pandas, psycopg2, PyYAML
```

### 1. Generate Master Data (once per environment setup)

```bash
python scripts/generate_master_data.py
```

### 2. Generate a Daily Batch

```bash
python scripts/generate_batch_data.py --date 2026-02-20
```

### 3. Add New Users Mid-Day (simulates real growth)

```bash
python scripts/add_new_customers.py --count 5
python scripts/add_new_drivers.py   --count 3
```

### 4. Generate Micro-Batch Stream Events

```bash
python scripts/generate_stream_data.py --date 2026-02-20 --hour 09
python scripts/generate_stream_data.py --date 2026-02-20 --hour 14
```

### 5. Run the Pipeline

```bash
python main.py
```

Both threads start automatically. **Batch** processes the daily files once. **MicroBatch** polls the stream directory every `poll_interval_sec` seconds continuously.

---

## Fault Tolerance Guarantee

```
On ANY error at ANY step:
  1. Error logged immediately (file, step, full error message, timestamp)
  2. Problematic record or file is skipped
  3. Alert fired asynchronously — pipeline does not wait for it
  4. All remaining files continue processing normally
  5. Pipeline never halts
```

---

*FastFeast Data Engineering .*
