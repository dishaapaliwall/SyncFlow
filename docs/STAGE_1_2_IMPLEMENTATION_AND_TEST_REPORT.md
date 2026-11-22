# SyncFlow — Stage 1 & Stage 2 Implementation Report

## 1. Project Overview

SyncFlow is a database synchronization system that captures changes from a PostgreSQL source database and applies them to a PostgreSQL target database.

The system uses PostgreSQL Logical Replication with the `pgoutput` plugin to capture database changes.

**Current technology stack**
- Java
- Spring Boot 3.5.6
- Gradle
- PostgreSQL 18
- PostgreSQL JDBC Driver
- Docker Compose
- PostgreSQL logical replication (`pgoutput`)

**Database configuration**
- Source database: `syncflow_source`
- Target database: `syncflow_target`
- Source port: `5434`
- Target port: `5435`
- Replication slot: `syncflow_slot`
- Publication: `syncflow_publication`

## 2. Stage 1 — Core Change Data Capture (CDC)

### Objective

Implement continuous change capture from the source PostgreSQL database and apply those changes to the target database.

### Architecture

The synchronization flow is:

Source PostgreSQL → Logical Replication Stream → CdcReader → LogicalDecoder → SyncEngine → BatchProcessor → TargetWriter → Target PostgreSQL

### Main components

**CdcConnection**
- Establishes the PostgreSQL replication connection.
- Connects to the existing logical replication slot.
- Configures the `pgoutput` protocol and publication.

**CdcReader**
- Reads pending logical replication messages.
- Provides access to received LSN information.
- Acknowledges a specified LSN using flushed and applied positions.

**LogicalDecoder**
- Decodes PostgreSQL logical replication messages.
- Handles transaction BEGIN, RELATION, INSERT, UPDATE, DELETE and COMMIT messages.
- Extracts transaction commit LSN, end LSN and commit timestamp.

**ChangeEvent**
- Represents an individual database change.

**SourceTransaction**
- Groups all changes belonging to one committed source transaction.
- Stores commit LSN, end LSN, commit timestamp and transaction events.

**SyncEngine**
- Manages the CDC lifecycle.
- Reads and decodes replication messages.
- Groups events into source transactions.
- Sends completed transactions for target processing.
- Acknowledges the source only after successful target processing.
- Handles reconnection attempts.

**BatchProcessor**
- Processes all changes belonging to a source transaction as one target transaction.

**TargetWriter**
- Applies INSERT, UPDATE and DELETE operations to the target database.

**TransactionManager**
- Controls target database transactions through commit and rollback.

### Transaction processing flow

1. Read a logical replication message.
2. Decode the message.
3. Collect row changes belonging to the current source transaction.
4. Detect the COMMIT message.
5. Build a SourceTransaction.
6. Apply all changes to the target in one database transaction.
7. Commit the target transaction.
8. Acknowledge the source transaction's exact end LSN.

If target processing fails, the target transaction is rolled back and the source transaction is not acknowledged.

### Stage 1 testing

The following scenarios were tested:

| Test | Result |
|---|---|
| Single INSERT replication | Passed |
| Multiple INSERTs in one transaction | Passed |
| UPDATE replication | Passed |
| DELETE replication | Passed |
| Transaction containing 11 events | Passed |
| Target transaction rollback on failure | Passed |
| Replay after resolving a target conflict | Passed |
| Explicit transaction end-LSN acknowledgement | Passed |

### Important implementation detail

The system processes complete source transactions rather than independently committing each row event.

This preserves transaction-level atomicity on the target.

## 3. Stage 2 — Checkpointing and Recovery

### Objective

Introduce durable checkpoint tracking so that SyncFlow can recover from failures without applying an already-committed source transaction twice.

### Problem addressed

Consider the following failure:

1. A source transaction is received.
2. Its changes are applied to the target.
3. The target commits successfully.
4. The application crashes before acknowledging the source transaction.

PostgreSQL can replay the unacknowledged transaction after reconnection.

Without additional protection, the target could attempt to apply the same changes again.

### Checkpoint storage

A `syncflow_metadata` table was introduced in the target database.

It contains:

| Column | Purpose |
|---|---|
| id | Metadata row identifier |
| last_processed_lsn | Last successfully committed source transaction end LSN |
| last_sync_time | Time of the last checkpoint update |
| total_events | Total tracked events |
| successful_events | Successfully processed events |
| failed_events | Failed processing attempts |

The metadata row uses `id = 1`.

### Main components

**CheckpointManager**
- Initializes the metadata row if it does not exist.
- Reads the last processed LSN.
- Updates the checkpoint and synchronization timestamp.
- Uses the same target database connection as transaction processing.

**BatchProcessor**
- Applies the transaction's events.
- Saves the transaction end LSN through CheckpointManager.
- Commits target changes and checkpoint together.
- Rolls back both if processing fails.

**RecoveryManager**
- Provides checkpoint-based recovery helper operations.
- Reads the saved checkpoint and determines whether one is available.

**SyncEngine**
- Reads the saved checkpoint when it receives a committed source transaction.
- Compares the incoming transaction's end LSN with the saved checkpoint.
- Skips transactions whose end LSN is less than or equal to the saved checkpoint.
- Acknowledges the skipped transaction after identifying it as already applied.
- Processes new transactions normally.

### Atomic processing flow

For a new transaction:

1. Begin target database transaction.
2. Apply all row changes.
3. Save the transaction end LSN in `syncflow_metadata`.
4. Commit the target transaction.
5. Acknowledge the source transaction.

The target data and checkpoint are committed atomically.

### Replay handling

When a transaction is replayed:

1. Read the saved target checkpoint.
2. Compare the replayed transaction's end LSN with the checkpoint.
3. If the transaction is already covered by the checkpoint, skip its target writes.
4. Acknowledge its end LSN.
5. Continue processing subsequent transactions.

This protects against duplicate application in the tested crash-recovery scenario.

## 4. Stage 2 — Recovery Experiment

### Objective

Verify that SyncFlow can recover when the target transaction commits successfully but the source acknowledgement does not happen.

### Test setup

A temporary crash-test hook was introduced.

It deliberately terminated the Java process immediately after:

`batchProcessor.process(sourceTransaction)`

and before:

`reader.acknowledge(...)`

The hook was configured through a temporary application property:

`syncflow.test.crash-after-target-commit`

The temporary hook and its configuration were removed after the experiment.

### Test procedure

1. Stop the CDC engine.
2. Enable the temporary crash-test property.
3. Restart Spring Boot.
4. Start CDC.
5. Insert a test customer into the source database.
6. Allow the target transaction to commit.
7. Deliberately terminate the Java process before source acknowledgement.
8. Verify that the target record and checkpoint persisted.
9. Verify that the replication slot had not acknowledged the test transaction.
10. Restart Spring Boot and CDC.
11. Observe the replayed transaction.
12. Verify that SyncEngine skipped the already-applied transaction and acknowledged its end LSN.

### Actual test values

Test record:

- Customer ID: `64`
- Name: `Crash Recovery Test`
- Email: `syncflow.crash.test@gmail.com`

Transaction positions:

- Commit LSN: `0/1CAC058`
- End LSN: `0/1CAC088`
- Saved target checkpoint: `0/1CAC088`

The process terminated with exit code `137`.

### Observed recovery logs

The restarted application produced:

`[SyncFlow] COMMIT received - commitLsn: 0/1CAC058, endLsn: 0/1CAC088`

`[SyncFlow] Skipping already-applied transaction. End LSN: 0/1CAC088`

`[SyncFlow] Acknowledged source transaction end LSN: 0/1CAC088`

### Final verification

- Customer 64 existed in the target database after the crash.
- The checkpoint remained at `0/1CAC088`.
- PostgreSQL replayed the unacknowledged transaction.
- SyncEngine skipped applying the transaction again.
- The transaction was acknowledged after recovery.
- The final target query returned exactly one row for customer 64.

**Result: The controlled crash-recovery experiment passed.**

## 5. Automated Testing

The project test suite was executed using:

`./gradlew clean test`

Final result after removing the temporary crash hook:

`BUILD SUCCESSFUL`

The unit tests include mocked dependencies where appropriate.

For example, `SyncEngineTest` uses a Mockito mock of CheckpointManager. This mock does not persist data to PostgreSQL.

Database-backed checkpoint persistence and the controlled crash-recovery experiment were verified separately using the actual PostgreSQL containers.

## 6. Known Limitations and Pending Work

The following work is not yet part of the completed Stage 1 and Stage 2 implementation:

1. Initial snapshot of existing source data.
2. Coordination of snapshot creation and logical replication using an exported PostgreSQL snapshot.
3. Capturing and applying changes occurring during snapshot execution.
4. Snapshot-to-CDC catch-up and transition.
5. Broader failure testing and PostgreSQL edge-case validation.
6. Additional logical replication message support and production-hardening work.

### Existing data observation

During testing, the source database contained 28 customer records while the target contained 17.

The original customer records with IDs 1–11 were absent from the target. The later CDC test records were present.

This is consistent with the fact that initial snapshot functionality has not yet been implemented. The databases have not been declared fully synchronized.

## 7. Current Project Status

| Stage | Description | Status |
|---|---|---|
| Stage 1 | Core CDC and transaction processing | Implemented and tested |
| Stage 2 | Persistent checkpoints and crash recovery | Implemented and tested in the documented scenarios |
| Stage 3 | Initial snapshot and snapshot-to-CDC coordination | Pending |

## 8. Important Design Principle

SyncFlow applies complete source transactions to the target and acknowledges them only after successful target processing.

Stage 2 extends this design by storing the checkpoint in the same target transaction as the row changes.

This provides a durable recovery boundary for the tested failure scenario: a committed target transaction whose source acknowledgement was interrupted.

The initial snapshot and its coordination with ongoing CDC remain the next major development milestone.