# SyncFlow: Real-Time Data Sync Between PostgreSQL Databases

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](build.gradle)
[![Java Version](https://img.shields.io/badge/Java-17%2B-blue)](build.gradle)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-blue)](docker-compose.yml)
[![Tests](https://img.shields.io/badge/Tests-121%20Passed-success)](build/reports/tests/test/index.html)

> **Course:** CSE3103 — Applied Databases and Systems Thinking  
> **Institution:** BML Munjal University  
> **Team Members:** Disha, Falak  
> **Track:** Research Track  
> **Core Research Question:** *How do batch size and write load affect sync lag and throughput, compared with native logical replication or Debezium?*

---

## 1. Project Overview

**SyncFlow** is a high-performance, real-time database synchronization engine designed to continuously stream changes (`INSERT`, `UPDATE`, `DELETE`) from a source PostgreSQL database to a target PostgreSQL database with **sub-second replication lag** and **exactly-once delivery guarantees**.

### Key Capabilities
- **Continuous Change Data Capture (CDC):** Uses PostgreSQL's native logical decoding (`pgoutput` plugin) to stream changes directly from the Write-Ahead Log (WAL).
- **Initial Snapshot Bootstrap:** Automatically captures and copies pre-existing source tables using PostgreSQL exported snapshots (`EXPORT_SNAPSHOT`) before seamlessly transitioning into live streaming.
- **Transaction-Level Atomicity:** Replicates complete source transactions as single atomic target transactions, preserving ACID guarantees and relational foreign-key consistency.
- **Durable Checkpointing & Crash Recovery:** Atomic dual-write of target data and commit Log Sequence Numbers (LSN) in `syncflow_metadata`, preventing duplicate replay upon restart.
- **Micro-Batching:** High-throughput JDBC batch execution (500 rows/batch during snapshot, configurable transaction batching during CDC).
- **Comprehensive Monitoring & CLI:** Built-in Picocli CLI and REST API exposing real-time throughput, event buffers, replication progress, and a 5-minute lag alert threshold.
- **100K+ Rows Verified:** Benchmarked at **~20,323 rows/second** with 100% data integrity across 100,000+ records.

---

## 2. System Architecture

```
             SOURCE DATABASE (PostgreSQL 18)
                            │ (WAL / Write-Ahead Log)
                            ▼
            ┌───────────────────────────────┐
            │ Logical Replication (pgoutput)│
            └───────────────┬───────────────┘
                            │ Replication Stream
                            ▼
                     ┌─────────────┐
                     │  CdcReader  │
                     └──────┬──────┘
                            │ Raw Byte Buffers
                            ▼
                    ┌────────────────┐
                    │ LogicalDecoder │
                    └───────┬────────┘
                            │ ChangeEvents (BEGIN, INSERT, UPDATE, DELETE, COMMIT)
                            ▼
                     ┌─────────────┐
                     │ SyncEngine  │ ◄─────── CheckpointManager (LSN Check)
                     └──────┬──────┘
                            │ SourceTransaction
                            ▼
                    ┌────────────────┐
                    │ BatchProcessor │
                    └───────┬────────┘
                            │ Prepared SQL Batches
                            ▼
                    ┌────────────────┐
                    │  TargetWriter  │
                    └───────┬────────┘
                            │ Atomic Commit (Target Data + syncflow_metadata LSN)
                            ▼
                    TARGET DATABASE (PostgreSQL 18)
```

### Initial Snapshot Bootstrap Flow
```
1. BootstrapPreflightValidator: Pre-flight checks (wal_level, slot capacity, target emptiness)
2. SnapshotManager:             CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT (Lock consistent point)
3. SnapshotWorker:              Imports snapshot (SET TRANSACTION SNAPSHOT) -> 500-row JDBC batch copy
4. SnapshotStateManager:        State lifecycle: NEW -> COPYING -> CATCHING_UP -> STREAMING
5. Seamless Handoff:            SyncEngine starts CDC streaming strictly from snapshot LSN
```

---

## 3. Technology Stack

- **Language & Runtime:** Java 17+, Spring Boot 3.5.6
- **Build Tool:** Gradle 9.6.0
- **Databases:** PostgreSQL 18 (Source port: `5434`, Target port: `5435`)
- **Protocols & Drivers:** PostgreSQL JDBC Driver (`org.postgresql:postgresql:42.7.7`) with `pgoutput` logical replication stream
- **CLI Framework:** Picocli 4.7.7
- **Testing:** JUnit 5 (Jupiter), Mockito 5.18, Testcontainers-ready integration harnesses

---

## 4. 5-Step Quickstart Guide (Evaluator Reproducibility)

Follow these 5 steps to run the complete SyncFlow demo from scratch:

### Step 1: Start Database Containers
```bash
docker-compose up -d
```
*Starts `syncflow-source` (port 5434) and `syncflow-target` (port 5435) with `wal_level=logical`.*

### Step 2: Start SyncFlow Server
In **Terminal 1**, start the Spring Boot engine:
```bash
./gradlew bootRun
# On Windows PowerShell:
.\gradlew bootRun
```
*Wait until `Started SyncFlowApplication in ... seconds` appears.*

### Step 3: Run Initial Snapshot Bootstrap
In **Terminal 2**, trigger the full snapshot + CDC handoff via the CLI:
```bash
./gradlew cli --args="bootstrap"
# On Windows PowerShell:
.\gradlew cli --args="bootstrap"
```
*SyncFlow will copy all existing source rows (`customers`, `products`, `orders`, `order_items`, `payments`) to the target and automatically transition into live CDC mode.*

### Step 4: Verify Initial Snapshot in Target
Check the target database to confirm all source records were copied:
```bash
docker exec -i syncflow-target psql -U syncflow -d syncflow_target -c "SELECT count(*) FROM customers;"
```

### Step 5: Test Real-Time CDC Streaming
Insert a new record directly into the **Source Database**:
```bash
docker exec -i syncflow-source psql -U syncflow -d syncflow_source -c "
INSERT INTO customers (name, email, phone) 
VALUES ('Live Demo User', 'demo@syncflow.io', '9876543210');
"
```
Within milliseconds, verify that the new record was automatically streamed to the **Target Database**:
```bash
docker exec -i syncflow-target psql -U syncflow -d syncflow_target -c "
SELECT * FROM customers WHERE email = 'demo@syncflow.io';
"
```

---

## 5. CLI Command Reference

The SyncFlow CLI provides full control and telemetry over the synchronization pipeline:

```bash
./gradlew cli --args="<command>"
```

| Command | Description | Example |
|---|---|---|
| `bootstrap` | Executes full table snapshot from source to target, then auto-starts live CDC | `./gradlew cli --args="bootstrap"` |
| `start` | Starts continuous CDC replication engine (for ongoing updates) | `./gradlew cli --args="start"` |
| `status` | Checks engine status (`RUNNING`/`STOPPED`) and replication connection | `./gradlew cli --args="status"` |
| `progress` | Displays total synced events, successful events, failed events, and success rate | `./gradlew cli --args="progress"` |
| `lag` | Reports replication lag in milliseconds and indicates if 5-minute threshold is exceeded | `./gradlew cli --args="lag"` |
| `events` | Displays recent in-memory captured CDC events (`INSERT`, `UPDATE`, `DELETE`) | `./gradlew cli --args="events"` |
| `stop` | Gracefully stops the CDC streaming engine | `./gradlew cli --args="stop"` |
| `config` | Displays or updates configured database URLs | `./gradlew cli --args="config"` |
| `--help` | Prints help and usage instructions | `./gradlew cli --args="--help"` |

---

## 6. REST API Contract

Base URL: `http://localhost:8080/api/sync`

| Method | Endpoint | Description | Sample Response |
|---|---|---|---|
| `GET` | `/status` | Service and connection status | `{"service":"SyncFlow","status":"RUNNING","connectionStatus":"CONNECTED"}` |
| `POST` | `/bootstrap` | Initiates snapshot copy & CDC handoff | `{"status":"STARTED","message":"Bootstrap snapshot initiated."}` |
| `POST` | `/start` | Starts CDC streaming engine | `{"message":"CDC engine start requested"}` |
| `POST` | `/stop` | Stops CDC streaming engine | `{"message":"CDC engine stop requested"}` |
| `GET` | `/progress` | Event counters & success rate | `{"totalEvents":100,"successfulEvents":100,"failedEvents":0,"successRatePercent":100.0}` |
| `GET` | `/lag` | Replication delay & alert status | `{"lagMillis":142,"lagSeconds":0.142,"alertRequired":false,"alertThresholdMillis":300000}` |
| `GET` | `/events` | Recent in-memory captured events | `{"count":1,"events":[{"tableName":"customers","operation":"INSERT",...}]}` |

---

## 7. Performance & 100K+ Rows Benchmark

SyncFlow includes an automated large-scale benchmark test ([`LargeScaleSyncTest.java`](src/test/java/com/syncflow/benchmark/LargeScaleSyncTest.java)) validating bulk data ingestion, throughput, and memory stability:

```bash
./gradlew test --tests LargeScaleSyncTest
```

### Benchmark Results
- **Dataset Size:** 100,052 customer records
- **Total Rows Copied:** 100,052
- **Elapsed Time:** 4.92 seconds
- **Throughput:** **~20,323 rows/second**
- **Batch Size:** 500 rows/batch (JDBC `executeBatch`)
- **Data Integrity:** 100.0% match between source and target (0 lost, 0 duplicated)
- **Full Test Suite:** 121 tests passed (`BUILD SUCCESSFUL in 1m 3s`)

---

## 8. Running Automated Tests

To run the entire test suite (unit tests, integration tests, and reliability recovery tests):

```bash
./gradlew test
```
HTML test reports are automatically generated at:
```
build/reports/tests/test/index.html
```

---

## 9. Deliverables Matrix (V1 Scope)

| Deliverable | Requirement | Status | Implementation Details |
|---|---|:---:|---|
| **CDC Extractor** | Read PostgreSQL WAL via logical replication | ✅ Complete | `CdcConnection.java`, `CdcReader.java` |
| **Parse Change Events** | Extract INSERT, UPDATE, DELETE & column tuples | ✅ Complete | `LogicalDecoder.java`, `ChangeEvent.java` |
| **Stream to Target** | Target JDBC execution with schema mapping | ✅ Complete | `TargetWriter.java`, `SyncEngine.java` |
| **Batch Writes** | High-throughput batching | ✅ Complete | 500 rows/batch (`SnapshotWorker`), transaction-batch (`BatchProcessor`) |
| **Exactly-Once Delivery** | Crash recovery without duplicate writes | ✅ Complete | `CheckpointManager.java`, atomic dual-commit in `syncflow_metadata` |
| **Deduplication Logic** | Filter duplicate / out-of-order LSN replays | ✅ Complete | `CheckpointManager.java`, `DeduplicationManager.java` |
| **Progress Tracking** | Metrics tracking & 5-minute lag alert | ✅ Complete | `ProgressTracker.java`, `LagMonitor.java` |
| **CLI Tool** | Terminal interface for operations & telemetry | ✅ Complete | `Main.java` (Picocli subcommands) |
| **Initial Snapshot** | Bulk bootstrap + seamless CDC catchup | ✅ Complete | `BootstrapManager.java`, `SnapshotWorker.java` |
| **100K+ Rows Scalability** | Verified under high data volume | ✅ Complete | `LargeScaleSyncTest.java` (20,323 rows/sec) |

---

## 10. Repository & Tagged Release

- **Branch:** `dev`
- **Release Tag:** `v1.0-report`
- **License:** Educational / Open Source for CSE3103 Coursework
