# StreamForge AI - High-Level Architecture (v0.1)

This document describes the intended end-to-end architecture of StreamForge AI, organized into:

- ingestion
- streaming
- storage
- the prefetch layer
- module boundaries and integration contracts
- key design decisions

Note: in the current repository snapshot, only `prefetch-engine/` is implemented as executable code. The other layers are documented as architectural placeholders and interfaces you can implement next.

## Ingestion layer

**Purpose:** capture row-level changes from operational databases (CDC) and publish them as events.

**Primary components (intended):**
- `ingestion-service/`: Debezium-based connector(s).

**Event backbone:**
- Events are published to Kafka topics.

**Inputs:**
- Source database connections (e.g., MySQL / Postgres)

**Outputs:**
- CDC event streams (Kafka topics) containing “what changed” plus enough metadata for downstream processors (e.g., database/table identity, operation type, ordering/offsets).

## Streaming layer

**Purpose:** consume CDC events and produce downstream artifacts for analytics/ML.

**Primary components (intended):**
- `stream-processor/`: Apache Flink job(s).

**Responsibilities:**
- Consume CDC events from Kafka.
- Perform cleaning and transformation.
- Compute feature aggregations / derived signals.

**Outputs (intended):**
1. **Processed data for training/inference**
   - Written to object storage via `storage-sink/`.
2. **Access-manifest for prefetch**
   - A “what the ML job will likely read next” manifest produced by streaming logic and consumed by the prefetch layer.

**Manifest shape (documented, consistent with `prefetch-engine/`):**
- Each manifest entry corresponds to an object identified by `uri` plus a small set of access signals.
- In the current executable demo, the signals map directly to:
  - `recent_access_count` (int)
  - `last_access_epoch` (float)
- In `prefetch-engine/prefetch.py`, this becomes `FileStat` with:
  - `uri: str`
  - `recent_access_count: int`
  - `last_access_epoch: float`

## Storage layer

**Purpose:** provide a durable object-storage substrate for processed artifacts and (optionally) manifests.

**Primary components (intended):**
- `storage-sink/`: writes objects to object storage (S3-compatible API).

**Current storage choice (intended MVP):**
- MinIO as the initial object storage target.

**Future storage choice (documented):**
- Potential Iceberg table sinks for incremental analytics.

## Prefetch layer

**Purpose:** reduce ML cold-start latency by staging a predicted “hot set” of objects into a local cache before the ML job begins.

**Primary components (implemented in this repo):**
- `prefetch-engine/`
  - `prefetch.py`: a runnable, executable architecture sketch.

### Prefetch pipeline (as implemented)
1. **Job planning**
   - An upstream component provides an access manifest.
   - In the demo, `_build_demo_manifest()` creates synthetic `FileStat` entries backed by local files.
2. **Hot set selection**
   - `select_hot_files(candidates, top_n)` ranks candidates by a simple score and returns the top N.
   - The demo scoring function is:
     - `score = recent_access_count + 0.000001 * last_access_epoch`
3. **Prefetch & staging**
   - `prefetch_files(hot_files, cache_dir)` stages each selected object into a local cache directory.
   - The demo simulates remote IO by copying local files and sleeping (`simulate_latency_s`).
4. **ML job consumption**
   - `run_simulated_ml_job(cache_dir, hot_files)` checks cache hits vs misses (demo behavior).
   - `build_processed_records(...)` produces NDJSON records including `cache_hit`.
   - `upload_processed_records_to_minio(records)` optionally uploads those records to MinIO if MinIO env vars are set.

### Cache and object identity
- Objects are identified by their `uri`.
- In the demo, URIs are `file://...` and staging resolves the path by stripping the `file://` prefix.
- In production, this same mechanism is intended to generalize to S3/MinIO-style URIs and remote fetch clients.

### Prefetch engine output contract
- Processed records are written as NDJSON (one JSON object per line).
- The NDJSON object key format in MinIO is:
  - `{prefix}/processed/{run_id}/part-{part_id:05d}.jsonl`
- These parameters come from:
  - `MINIO_PREFIX` (default: `streamforge`)
  - `MINIO_BUCKET` (default: `processed`)
  - `MINIO_PART_ID` (default: `0`)
  - `STREAMFORGE_JOB_ID` (optional override; otherwise generated as a UTC timestamp run id)

## Module boundaries

The intended module boundaries are best described as “integration contracts” that connect layers:

1. **Ingestion -> Streaming**
   - Contract: Kafka topic(s) and event schema for CDC changes.
   - Boundary principle: streaming must not depend on source-DB connector internals; it should only depend on the event format.

2. **Streaming -> Storage**
   - Contract: object-storage layout conventions (prefixes, partitioning, file formats) for processed artifacts.
   - Boundary principle: streaming emits data records; storage-sink owns the writing details.

3. **Streaming -> Prefetch layer (manifest)**
   - Contract: access-manifest schema and its location (object storage path or shared filesystem).
   - Boundary principle: prefetch should only require a manifest, not a direct coupling to Flink execution state.

4. **Prefetch -> ML job consumption**
   - Contract: local cache directory location and the mapping between manifest URIs and cached objects.
   - Boundary principle: ML training should read from cache first, but still be able to fall back to remote storage on cache miss.

5. **Prefetch engine -> Storage sink (optional output)**
   - Contract: processed-record output format and (optional) MinIO upload behavior.
   - In the demo, MinIO upload is gated by required env vars:
     - `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`

## Design decisions

1. **Kafka as the event backbone**
   - Works naturally with Debezium (CDC) and Flink (stream processing).
2. **Flink for streaming semantics**
   - Provides checkpointing and flexible event processing for aggregations.
3. **MinIO first**
   - S3-compatible and simple for local demos; avoids higher operational complexity in early iterations.
4. **Prefetch optimization strategy**
   - Prefetch uses a predicted access manifest and stages a “hot set” into a local cache before ML starts.
5. **MVP favors clarity over sophistication**
   - The current prefetch demo uses:
     - a simple ranking score
     - file-copy-based staging
     - cache hit/miss reporting
   - Future evolution is expected to replace these with real remote fetch clients, better ranking signals, and observability metrics.

