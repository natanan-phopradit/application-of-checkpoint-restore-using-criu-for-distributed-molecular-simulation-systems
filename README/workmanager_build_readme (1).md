# WorkManager Build Guide

## Prerequisites

Before building the project, make sure the following dependencies are installed:

### 1. C++20 Compiler

You need a compiler with C++20 support.

Examples:

- GCC 10+
- Clang 12+
- MSVC Visual Studio 2022+

---

### 2. CMake

Install CMake by following the official guide:

- https://cmake.org/cmake/help/latest/guide/tutorial/Before%20You%20Begin.html#getting-cmake

Verify installation:

```bash
cmake --version
```

---

### 3. vcpkg

Install and configure vcpkg:

- https://learn.microsoft.com/en-us/vcpkg/get_started/get-started

After installation, note the path to your `vcpkg` directory.

Example:

```bash
/home/user/vcpkg
```

or on Windows:

```powershell
C:\vcpkg
```

---

# Build Instructions

Run the following command from the project root directory:

```bash
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=<vcpkg_root_path>/scripts/buildsystems/vcpkg.cmake && cmake --build build --target WorkManager --clean-first -j
```

Replace:

```text
<vcpkg_root_path>
```

with the actual path to your `vcpkg` installation.

Example:

```bash
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=/home/user/vcpkg/scripts/buildsystems/vcpkg.cmake && cmake --build build --target WorkManager --clean-first -j
```

---

# Build Output

After a successful build, the generated binaries will be available inside the `build` directory.


## Environment Variables

Configure the following environment variables before running the application.

| Variable | Description |
|---|---|
| `WORKQUEUE_URL` | ActiveMQ broker URI (e.g., `tcp://localhost:61616`). |
| `WORKSERVER_URL` | MinIO server URL for topologies (e.g., `http://localhost:9000`). |
| `WORKSERVER_ACCESSKEY` | MinIO Access Key for the topology bucket. |
| `WORKSERVER_SECRETKEY` | MinIO Secret Key for the topology bucket. |
| `SNAPSHOTSERVER_URL` | MinIO server URL for snapshots. |
| `SNAPSHOTSERVER_ACCESSKEY` | MinIO Access Key for the snapshot bucket. |
| `SNAPSHOTSERVER_SECRETKEY` | MinIO Secret Key for the snapshot bucket. |
| `RESULTSERVER_URL` | MinIO server URL for results. |
| `RESULTSERVER_ACCESSKEY` | MinIO Access Key for the results bucket. |
| `RESULTSERVER_SECRETKEY` | MinIO Secret Key for the results bucket. |

---

## Linker Requirements

The project is configured to use the `mold` linker for improved build performance.

Ensure `mold` is installed on your system before building the project.

Example installation on Ubuntu:

```bash
sudo apt update
sudo apt install mold
```

---

## Important Paths

### GROMACS Topology Files

The `/upload_work` endpoint expects GROMACS topology files (`.tpr`) to be located at:

```text
/home/ubuntu/WorkManager/gromacs_topology/
```

### Temporary Buffer Storage

The system uses `/tmp/` for temporary file buffering during uploads and downloads.

