# Data Engineering Project (Spark)

This project implements a **Batch + Streaming data pipeline** using **Apache Spark**, fully containerized with Docker.

The pipeline ingests user and event data, performs cleansing, deduplication, enrichment, and produces **daily aggregated analytics**.


## 🚀 How to Run the Pipeline

### Prerequisites
- Docker installed
- No local Spark installation required


### Build & Run

```bash
docker build -t unloq-spark .
docker run --rm -it \
  -v "$(pwd)/output:/app/output" \
  -v "$(pwd)/processed:/app/processed" \
  unloq
```

### Processed files & Output location


- processed/
    - users/
    - events/
- output/
    - daily_aggregations/


## Batch & Streaming Modes

### Batch Mode
- Reads full historical files:
  - `users.json`
  - `events.csv`
- Used for:
  - Backfills, Reprocessing, Recovery scenarios


### Streaming Mode (Simulated)
- Reads incremental events from `events_stream.json`
- Processed in **micro-batches**
- Mimics real-time ingestion without external systems (e.g., Kafka)


## Deduplication & Watermarking Strategy

### Deduplication
- **Primary key:** `event_id`
- Applied at every ingestion step
- Cross-source deduplication ensures:
  - No double counting between batch and stream
  - Safe re-runs

### Watermarking
- Logical event time: `event_ts`
- Late events are allowed within a defined window
- Ensures correctness for out-of-order data


## Idempotency Guarantees

The pipeline is **idempotent by design**.

| Scenario | Guarantee |
|--------|----------|
| Re-running batch jobs | No duplicate data |
| Restarting pipeline | Safe |
| Overlapping batch & stream | Deduplicated |
| Partial failures | Recoverable |


### How Idempotency Is Achieved
- Deterministic keys (`event_id`)
- Append-only writes
- Deduplication before every write
- Stateless Spark jobs



## Design Assumptions & Trade-offs

### Assumptions
- `event_id` is globally unique
- User dimension is treated as **SCD Type-1**
- Late data is bounded

### Trade-offs

| Decision | Reason |
|--------|--------|
| No partitioning for raw events | Avoid small-file problem |
| Partitioned aggregates only | Query efficiency |
| No Delta Lake | Keep dependencies minimal |


## Scaling to Larger Data Volumes

This design scales naturally with minimal changes.

### Storage
- Replace local filesystem with:
  - S3 / GCS / ADLS
- Switch output to **Delta Lake** for:
  - Compaction, Time travel, Upserts

### Compute
- Run Spark on:
  - EMR / Databricks
- Increase executor count and memory

### Streaming
- Replace file-based simulation with:
  - Kafka / Event Hub
- Enable true Structured Streaming with checkpoints

### Optimizations
- Broadcast joins for small dimensions
- Adaptive Query Execution (AQE)
- Periodic compaction jobs
- OTIMIZE and Z-ORDER 

