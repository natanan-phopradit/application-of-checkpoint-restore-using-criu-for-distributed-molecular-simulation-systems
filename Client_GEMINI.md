# Project2: High-Performance C++20 Molecular Dynamics Orchestrator

Project2 is a specialized worker application designed to orchestrate GROMACS simulations with transparent, state-preserving **Checkpoint/Restore** capabilities. By leveraging **CRIU (Checkpoint/Restore In Userspace)** and a robust messaging/REST architecture, it enables high-throughput, fault-tolerant molecular dynamics workloads.

---

## 🏗️ Architectural Overview

The application operates as a stateful worker in a distributed system, following a **Fetch-Execute-Checkpoint-Upload** lifecycle.

### Core Components
- **Task Management (`QueueProducer`):** A wrapper around ActiveMQ-CPP that manages persistent connections to a broker. It handles task retrieval from `WorkQueue` and status updates/snapshot notifications via `SnapshotQueue`.
- **Orchestration Loop:** A monolithic main loop that manages the state machine:
    1.  **Fetch:** Dequeues a task from ActiveMQ.
    2.  **Provision:** Downloads the required topology (`.tpr`) or memory snapshot (`.tar.gz`) from the Coordination Server.
    3.  **Execute/Restore:** Either starts a new `gmx mdrun` process or restores a previous state using `libcriu`.
    4.  **Monitor:** Polls the running simulation, handling periodic checkpoints and system signals.
    5.  **Finalize:** Upon completion, gathers GROMACS results (`.cpt`, `.edr`, `.gro`, `.log`), compresses them, and uploads them.
- **CRIU Wrapper:** Provides a high-level C++ interface to `libcriu` for dumping and restoring process trees.
- **REST Client (Boost.Beast):** Handles all binary and metadata transfers with the Coordination Server, including hardware identification headers for node tracking.

---

## 🔧 Technical Deep Dive

### CRIU Integration
Project2 uses `libcriu` for transparent process migration.
- **Checkpointing:** The `criu_checkpoint` function triggers a `criu_dump`. Images are stored in `/dev/shm` (RAM disk) to minimize I/O overhead.
- **Restoration:** The `criu_restore` function uses `criu_restore_child` to resume a process tree from images.
- **Optimization:** Uses `criu_set_leave_running(true)` for periodic checkpoints to avoid process interruption.

### Messaging & Task Protocol
The system uses **ActiveMQ** for task distribution and **Artemis Jolokia** for monitoring.
- **WorkQueue:** Contains simulation task metadata (Name, Path, CreatedAt).
- **SnapshotQueue:** Used for "Last Value Queuing" (LVQ) of snapshots, ensuring the system only keeps the most recent state of a specific simulation.
- **Task Types:** 
    - `standard`: A fresh simulation start.
    - `snapshot`: A resumption from a previous CRIU dump.
    - `finished_snapshot`: A notification that a simulation has concluded.

### Coordination Server REST API
Communication is performed via HTTP/1.1 (Boost.Beast):
- `GET /jolokia/...`: Used to poll message counts on the broker.
- `POST /get_work`: Downloads topologies or snapshots. Includes `X-Device-ID`, `X-CPU-Logical-Cores`, and `X-OS-Version` headers.
- `POST /send_snapshot`: Uploads CRIU images compressed with `pigz`.
- `POST /send_result`: Uploads final GROMACS output files.

---

## 🛠️ Build & Development

### Dependencies
Managed via `vcpkg`. Key libraries include:
- **Boost 1.90.0:** Asio, Beast, Process, Json, Log, Chrono.
- **ActiveMQ-CPP:** Messaging.
- **libcriu:** Checkpoint/Restore.
- **lfreist-hwinfo:** Hardware discovery.
- **ayush272002-dotenv:** Environment management.

### Build System Configuration
- **C++20:** Required for `std::format`, `std::filesystem`, and improved RAII.
- **Linker:** Configured to use `mold` for significantly faster link times.
- **Build Command:**
  ```bash
  cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=[vcpkg_path]/scripts/buildsystems/vcpkg.cmake
  cmake --build build
  ```

---

## ⚙️ Configuration (.env)

| Variable | Description |
| :--- | :--- |
| `WORKQUEUE_URL` | ActiveMQ broker URI (e.g., `tcp://localhost:61616`). |
| `COORDINATIONSERVER_URL` | Hostname of the REST API server. |
| `COORDINATIONSERVER_PORT` | Port for the REST API. |
| `GMX_PATH` | Full path to the `gmx` executable. |
| `ARTEMIS_PORT` | Port for Jolokia management API (default `8161`). |
| `ARTEMIS_AUTH` | Basic auth for Jolokia (e.g., `admin:admin`). |
| `ARTEMIS_GETCOUNT_TARGET` | Jolokia MBean path for queue count monitoring. |

---

## ⚠️ Critical Notes

- **Signal Handling:** The application traps `SIGINT` and `SIGTERM`. Upon receipt, it attempts an immediate checkpoint before exiting to prevent work loss.
- **Permissions:** CRIU requires either `root` privileges or the `CAP_CHECKPOINT_RESTORE` capability.
- **Temporary Storage:** `/dev/shm` is used for high-speed image storage. Ensure the system has sufficient RAM allocated to `tmpfs`.
- **Waitpid Logic:** The main loop uses non-blocking `waitpid` with `WNOHANG` to monitor GROMACS while remaining responsive to signals and timers.
