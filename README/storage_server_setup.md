# Storage Server Setup Guide (MinIO)

This guide documents the setup and configuration of the MinIO storage infrastructure used by WorkManager. The system utilizes three distinct MinIO instances (or buckets) to manage different types of simulation data.

---

## 1. System Architecture

The coordination server interacts with three logical storage servers:

| Server Type | Bucket Name | Purpose |
| :--- | :--- | :--- |
| **Work Server** | `gromacs-topology` | Stores initial `.tpr` topology files. |
| **Snapshot Server** | `snapshot` | Stores intermediate simulation states. |
| **Result Server** | `results` | Stores final simulation outputs. |

### Reliability Features
- **Exponential Backoff:** The WorkManager client implements a retry mechanism (`retry_minio_op`) for all MinIO operations. It starts with a 1s delay, doubling up to 5 attempts, specifically to handle "SlowDown" (429/503) errors.
- **Deduplication:** The Result Server performs a `StatObject` check before uploading. If a result file with the same name already exists, the server returns a `409 Conflict` to prevent duplicate submissions.

---

## 2. Installation (Ubuntu/Debian)

Run these commands on each server designated as a storage node.

### 1. Download & Install

```bash
# Download MinIO
curl --progress-bar -L dl.min.io/aistor/minio/release/linux-amd64/minio.deb -o minio.deb

# Install
sudo dpkg -i minio.deb

# Verify
minio --version
```

### 2. License & Configuration

MinIO requires a license for enterprise features or specific deployments.

```bash
# Create license directory
sudo mkdir -p /opt/minio
sudo nano /opt/minio/minio.license
# [Paste license from https://subnet.min.io/]

# Configure environment
sudo nano /etc/default/minio
```

Add the following configuration (adjust `MINIO_VOLUMES` to your mount point):

```bash
MINIO_LICENSE="/opt/minio/minio.license"
MINIO_VOLUMES="/mnt/drive-1/minio"
```

### 3. Permissions & Service

```bash
# Set ownership
sudo chown -R minio-user:minio-user /opt/minio
sudo chown -R minio-user:minio-user /mnt/drive-1/minio

# Enable and start service
sudo systemctl enable minio.service
sudo systemctl start minio.service
```

---

## 3. Bucket & Access Configuration

Repeat these steps for the Work, Snapshot, and Result logical roles.

### 1. Create Buckets
Access the MinIO Console at `http://[Server-IP]:9001` and create the following buckets:
- `gromacs-topology`
- `snapshot`
- `results`

### 2. Generate Access Keys
For each role, navigate to **Access Keys** in the console and create a new key pair.

### 3. Credential Tracking
These credentials must be provided to the Coordination Server via environment variables:

| Role | Variable Prefix | URL | Access Key | Secret Key |
| :--- | :--- | :--- | :--- | :--- |
| Work | `WORKSERVER_` | | | |
| Snapshot | `SNAPSHOTSERVER_` | | | |
| Result | `RESULTSERVER_` | | | |

---

## 4. Operational Notes
- **Firewall:** Ensure ports `9000` (API) and `9001` (Console) are open.
- **Cleanup:** For the `snapshot` bucket, the coordination server automatically deletes objects after a successful download via the `/get_work` endpoint.
- **Storage Path:** Always verify that your `MINIO_VOLUMES` path is correctly mounted before starting the service.
