# SyncFlow API Contract

## 1. Overview

- **Project:** SyncFlow
- **Purpose:** Real-time change data capture (CDC) and synchronization between PostgreSQL databases.
- **Base URL:** `http://localhost:8080`
- **API prefix:** `/api/sync`
- **Protocol:** HTTP
- **Data format:** JSON

## 2. Sync Status

### GET `/api/sync/status`

**Purpose:** Return the current service and connection status.

**Request body:** None

**Success response — 200 OK**

```json
{
  "service": "SyncFlow",
  "status": "RUNNING",
  "connectionStatus": "CONNECTED"
}
```

**Response fields:**

- `service`: Service name.
- `status`: `RUNNING` or `STOPPED`.
- `connectionStatus`: `CONNECTED` or `DISCONNECTED`.

## 3. Start Synchronization

### POST `/api/sync/start`

**Purpose:** Request the CDC engine to start synchronization.

**Request body:** None

**Current response**

```json
{
  "message": "CDC engine start requested"
}
```

If the engine is already running:

```json
{
  "message": "SyncFlow is already running"
}
```

**Note:** A start-request response does not guarantee that replication has successfully started.

## 4. Stop Synchronization

### POST `/api/sync/stop`

**Purpose:** Request the CDC engine to stop.

**Request body:** None

**Current response**

```json
{
  "message": "CDC engine stop requested"
}
```

## 5. Synchronization Progress

### GET `/api/sync/progress`

**Purpose:** Return event-processing statistics recorded by the progress tracker.

**Request body:** None

**Response fields:**

| Field | Type | Description |
|---|---|---|
| `totalEvents` | Integer (`long`) | Total recorded events |
| `successfulEvents` | Integer (`long`) | Successfully recorded events |
| `failedEvents` | Integer (`long`) | Failed events |
| `successRatePercent` | Number (`double`) | Percentage of successful events |

**Success response example:**

```json
{
  "totalEvents": 100,
  "successfulEvents": 95,
  "failedEvents": 5,
  "successRatePercent": 95.0
}
```

**Calculation:**

`successRatePercent = successfulEvents / totalEvents × 100`

If `totalEvents` is zero, the current implementation returns `0.0`.

**Note:** The actual meaning of a successful or failed event depends on where the progress tracker methods are called in the synchronization engine.


## 6. Replication Lag

### GET `/api/sync/lag`

**Purpose:** Return the current replication lag and alert-threshold information.

**Request body:** None

**Response fields:**

| Field | Type | Description |
|---|---|---|
| `lagMillis` | Integer (`long`) | Current lag in milliseconds |
| `lagSeconds` | Number (`double`) | Current lag converted to seconds |
| `alertRequired` | Boolean | True when current lag exceeds the configured threshold |
| `alertThresholdMillis` | Integer (`long`) | Configured alert threshold in milliseconds |

**Success response example:**

```json
{
  "lagMillis": 350000,
  "lagSeconds": 350.0,
  "alertRequired": true,
  "alertThresholdMillis": 300000
}
```

**Current configuration:** The service initializes the threshold to 300000 milliseconds (5 minutes).

**Alert rule:** `alertRequired = currentLagMillis > alertThresholdMillis`

**Validation:** Negative thresholds and negative lag values are rejected by `LagMonitor` with `IllegalArgumentException`.

**Note:** The HTTP response produced when an exception occurs must be verified separately. The monitoring class alone does not define the HTTP error response.

## 7. Recent CDC Events

### GET `/api/sync/events`

**Purpose:** Return the recent CDC events currently stored in the in-memory event buffer.

**Request body:** None

**Response fields:**

| Field | Type | Description |
|---|---|---|
| `count` | Integer | Number of events currently stored in the buffer |
| `events` | Array | List of recent CDC events |

Each event contains:

| Field | Type | Description |
|---|---|---|
| `tableName` | String | Name of the affected table |
| `operation` | Enum | Database operation: `INSERT`, `UPDATE`, or `DELETE` |
| `data` | Object or null | New/current row data represented by the event |
| `oldData` | Object or null | Previous row data, when available |
| `lsn` | String | Log Sequence Number associated with the event |
| `timestamp` | Date-time or null | Timestamp associated with the event |

**Buffer behavior:**
- The buffer capacity is 100 events.
- When the buffer is full, adding a new event removes the oldest event.
- Events are returned in oldest-to-newest order among the events currently stored.
- The buffer is in memory; this endpoint returns buffered events, not a permanent event history.
- The returned `count` is the number of events in the response.

**Example response:**

```json
{
  "count": 1,
  "events": [
    {
      "tableName": "customers",
      "operation": "INSERT",
      "data": {
        "customer_id": 1,
        "name": "Example Customer"
      },
      "oldData": null,
      "lsn": "0/16B6C50",
      "timestamp": "2026-10-20T10:30:00"
    }
  ]
}
```

*Note: The example is illustrative. Actual event values and timestamp serialization depend on the runtime data and JSON configuration.*

## 8. Pending Documentation

The following details must be verified against the implementation:

- Exact HTTP status codes for all responses.
- Error-response format.
- Exact response schemas for progress, lag and events.
- Behaviour when the database connection fails.
- Whether starting and stopping synchronization is synchronous or asynchronous.
