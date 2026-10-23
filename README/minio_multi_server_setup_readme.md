# MinIO Multi-Server Setup Guide

This guide documents the setup process for three MinIO servers:

- Snapshot Server
- Result Server
- Work Server

Each server will run MinIO and expose its own bucket and access credentials.

---

# Prerequisites

- Ubuntu/Debian-based Linux server
- `sudo` privileges
- Mounted storage path available at `/mnt/drive-1/minio`
- Internet access
- MinIO account for obtaining a license

Create a MinIO account and retrieve the license from:

https://subnet.min.io/?jmp=docs

Go to the Deployments view and select the **License** button.

---

# Install MinIO

Run the following commands on each server.

## 1. Download MinIO

```bash
curl --progress-bar -L dl.min.io/aistor/minio/release/linux-amd64/minio.deb -o minio.deb
```

## 2. Install MinIO

```bash
sudo dpkg -i minio.deb
```

## 3. Verify Installation

```bash
minio --version
```

## 4. Create License Directory

```bash
sudo mkdir /opt/minio
```

## 5. Install Nano

```bash
sudo apt install nano
```

## 6. Add MinIO License

```bash
sudo nano /opt/minio/minio.license
```

Paste the license content obtained from:

https://subnet.min.io/?jmp=docs

---

# Configure Permissions

```bash
sudo chown -R minio-user:minio-user /opt/minio
```

---

# Configure MinIO Environment

Edit the MinIO configuration file:

```bash
sudo nano /etc/default/minio
```

Add the following:

```bash
MINIO_LICENSE="/opt/minio/minio.license"
MINIO_VOLUMES="/mnt/drive-1/minio"
```

---

# Apply Ownership Again

```bash
sudo chown -R minio-user:minio-user /opt/minio/
```

```bash
sudo chown -R minio-user:minio-user /mnt/drive-1/minio
```

---

# Enable MinIO Service

```bash
sudo systemctl enable minio.service
```

---

# Repeat for All Servers

Repeat the full installation and configuration process for:

1. Snapshot Server
2. Result Server
3. Work Server

---

# MinIO Console Setup

Access the MinIO console using:

```text
http://x.x.x.x:9001
```

Replace `x.x.x.x` with the server IP address.

---

# Bucket Creation

## 1. Work Server

Create bucket:

```text
gromacs-topology
```

Purpose:

- Stores initial work items

---

## 2. Snapshot Server

Create bucket:

```text
snapshot
```

Purpose:

- Stores intermediate simulation states

---

## 3. Result Server

Create bucket:

```text
results
```

Purpose:

- Stores final simulation outputs

---

# Access Key Setup

For each server:

1. Open the MinIO Console
2. Navigate to Access Keys
3. Create a new Access Key
4. Save the following information:

```text
Server URL
Access Key
Secret Key
```

These credentials will be required by the coordination server.

---

# Recommended Credential Tracking

| Server Type     | URL | Access Key | Secret Key |
|----------------|-----|-------------|-------------|
| Work Server    |     |             |             |
| Snapshot Server|     |             |             |
| Result Server  |     |             |             |

---

# Notes

- Ensure firewall rules allow access to ports `9000` and `9001`
- Verify mounted storage exists before starting MinIO
- Keep all access keys and secret keys secure
- Test bucket access after setup is complete

