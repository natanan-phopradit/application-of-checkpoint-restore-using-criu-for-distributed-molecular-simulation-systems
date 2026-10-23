# Coordination Server Setup Guide

This guide covers the installation and configuration of the two primary components of the WorkManager coordination server: the **Apache Artemis** message broker and the **WorkManager** C++ application.

---

## Part 1: Apache Artemis 2.54.0 Setup

Apache Artemis handles message queuing for simulation tasks and snapshots.

### 1. Installation (Ubuntu 24.04)

Prepare the environment and initialize the broker:

```bash
# Install dependencies
sudo apt update
sudo apt install nano openjdk-21-jdk -y

# Download Artemis
wget -O apache-artemis-2.54.0-bin.tar.gz "https://www.apache.org/dyn/closer.lua?filename=artemis/artemis/2.54.0/apache-artemis-2.54.0-bin.tar.gz&action=download"

# Extract
tar -xzf apache-artemis-2.54.0-bin.tar.gz

# Create Broker Instance
sudo apache-artemis-2.54.0/bin/artemis create /var/lib/mybroker
```

### 2. Configuration

Edit the configuration files in `/var/lib/mybroker/etc/`:

#### `broker.xml`
- **External Bindings:** In the `<acceptors>` block, change IP addresses to `tcp://0.0.0.0:<port>` for all protocols (`artemis`, `amqp`, `stomp`, `hornetq`, `mqtt`) to allow external connections.
- **Auto-delete:** Set `auto-delete-queues` and `auto-delete-addresses` to `false` in the `<address-setting match="#">` block.
- **Address Settings:** Add or modify the following:
  - `<address-setting match="SnapshotQueue">`: Configure expiry between 0-240,000ms (4 mins) with `<expiry-address>WorkQueue</expiry-address>`.
- **Queues:** Define `WorkQueue` and `SnapshotQueue` under `<addresses>`, both with `last-value="true"`.

#### `bootstrap.xml`
- **Web Console Binding:** Change the binding URI to `http://0.0.0.0:8161` to make the management console accessible externally.

#### `jolokia-access.xml`
- **CORS:** Add `<allow-origin>*://0.0.0.0*</allow-origin>` and `<allow-origin>*</allow-origin>`.
- **Ignore Scheme:** Uncomment `<ignore-scheme/>`.

### 3. Service Management

```bash
# Start the broker
sudo /var/lib/mybroker/bin/artemis-service start

# Enable on boot (Recommended)
sudo systemctl enable /var/lib/mybroker/bin/artemis-service
```

---

## Part 2: WorkManager Build & Configuration

The WorkManager application coordinates simulations via the Artemis broker and MinIO storage.

### 1. Build Prerequisites

- **C++20 Compiler:** GCC 10+, Clang 12+, or MSVC 2022+.
- **CMake:** [Official Installation Guide](https://cmake.org/download/).
- **vcpkg:** [Getting Started with vcpkg](https://learn.microsoft.com/en-us/vcpkg/get_started/get-started).
- **mold Linker:**
  ```bash
  sudo apt update && sudo apt install mold -y
  ```

### 2. Build Instructions

Run from the project root directory:

```bash
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=<vcpkg_root_path>/scripts/buildsystems/vcpkg.cmake
cmake --build build --target WorkManager --clean-first -j
```
*Replace `<vcpkg_root_path>` with your actual vcpkg installation path.*

### 3. Environment Variables

Configure these variables before running the application:

| Variable | Description |
|---|---|
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

### 4. Important Paths

- **GROMACS Topologies:** Place `.tpr` files in `/home/ubuntu/WorkManager/gromacs_topology/` for the `/upload_work` endpoint.
- **Temporary Storage:** The system uses `/tmp/` for buffering during transfers.
