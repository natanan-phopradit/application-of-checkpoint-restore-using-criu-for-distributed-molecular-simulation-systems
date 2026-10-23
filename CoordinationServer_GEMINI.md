# WorkManager

WorkManager is a C++20 server application designed to coordinate GROMACS molecular dynamics simulations. It manages work queues via ActiveMQ and handles large data files (topologies, snapshots, results) using MinIO object storage.

## Core Architecture

### Message Queuing (ActiveMQ)
The application utilizes a `QueueProducer` class to manage two primary queues:
- **WorkQueue:** For new simulation tasks (topologies). Messages are persistent.
- **SnapshotQueue:** For simulation snapshots. These utilize **Last Value Queue (LVQ)** logic (via the `_AMQ_LVQ_NAME` property) to ensure that only the most recent snapshot for a specific simulation is prioritized.

### Object Storage (MinIO)
Data is organized into three buckets:
- `gromacs-topology`: Stores `.tpr` files for initial work.
- `snapshot`: Stores intermediate simulation states.
- `results`: Stores final simulation outputs.

### Reliability Features
- **MinIO Retries:** Implements `retry_minio_op` with exponential backoff (starting at 1s, doubling up to 5 attempts) to handle "SlowDown" (429/503) errors from S3-compatible storage.
- **Deduplication:** The `/send_result` endpoint checks for existing objects in the `results` bucket before uploading to prevent duplicate data submission (returns `409 Conflict` on match).

## API Reference

The server listens on port **65096**.

### `POST /upload_work`
Uploads a topology from the local `gromacs_topology/` directory to MinIO and enqueues it.
- **Query Param:** `?name=[benchName]` (default: `benchMEM`).
- **Body (JSON):** Optional `{ "Name": "..." }` or `{ "Bench": "..." }`.
- **Returns:** Path of the uploaded object.

### `GET /get_work` (Actually `POST /get_work` logic in code)
Downloads a work item and serves it as a file attachment.
- **Body (JSON):** Requires `Name`, `Path`, and `Type` ("topology" or "snapshot").
- **Behavior:** If type is `snapshot`, the object is **deleted** from MinIO after a successful download.
- **Response Headers:** Includes `X-File-Type`.

### `POST /send_snapshot`
Receives a snapshot and enqueues a notification.
- **Headers Required:**
    - `X-Filename`: Name of the snapshot file.
    - `X-Device-ID`: Identifier for the client device (used for path namespacing).
    - `X-AutoCheckpoint`: "true" or "false".
- **Logic:** Enqueued to `SnapshotQueue` with priority 6. If `X-AutoCheckpoint` is not "true", the message is set to expire quickly (1ms TTL).

### `POST /send_result`
Receives a result file and stores it in the `results` bucket.
- **Headers Required:**
    - `X-Filename`: Name of the result file.
    - `X-Device-ID`: Identifier for the client device.
- **Logic:** Performs deduplication check via `StatObject` before processing.

## Technical Details

### Dependencies
- **Boost:** Beast (HTTP), Asio (Network), JSON, Log, UUID, Lexical Cast.
- **ActiveMQ-CPP:** CMS API for ActiveMQ interaction.
- **MinIO C++ SDK:** S3-compatible object storage client.

### Environment Variables
| Variable | Description |
| :--- | :--- |
| `WORKQUEUE_URL` | ActiveMQ broker URI (e.g., `tcp://localhost:61616`) |
| `WORKSERVER_URL` | MinIO server URL for topologies |
| `WORKSERVER_ACCESSKEY` | MinIO Access Key (Topology) |
| `WORKSERVER_SECRETKEY` | MinIO Secret Key (Topology) |
| `SNAPSHOTSERVER_URL` | MinIO server URL for snapshots |
| `SNAPSHOTSERVER_ACCESSKEY` | MinIO Access Key (Snapshot) |
| `SNAPSHOTSERVER_SECRETKEY` | MinIO Secret Key (Snapshot) |
| `RESULTSERVER_URL` | MinIO server URL for results |
| `RESULTSERVER_ACCESSKEY` | MinIO Access Key (Results) |
| `RESULTSERVER_SECRETKEY` | MinIO Secret Key (Results) |

### Build System
- **Compiler:** Requires C++20 (uses `std::format`, `std::filesystem`, etc.).
- **Linker:** Specifically configured to use `mold` (`-fuse-ld=mold`) for high-performance linking.
- **CMake:** Minimum version 4.1.

## Development Notes
- **Concurrency:** Uses a thread-per-connection model (`std::thread` detached in `main`).
- **Storage:** Files are temporarily buffered in `/tmp/` before being uploaded to MinIO or served to clients.
- **LVQ:** Snapshot messages use the filename stem (without extensions) as the LVQ key.
