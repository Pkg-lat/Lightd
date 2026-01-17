# Port Management API

## Overview

Lightd uses an **explicit port pool** managed via an array of available ports. The panel can add or remove ports from this pool as needed.

### Key Concepts

- **Available Ports**: An explicit array of ports that can be allocated to containers
- **Reserved Ports**: System ports that should never be added to the pool (e.g., 22, 80, 443)
- **Allocated Ports**: Ports currently assigned to containers

## Configuration

The `network.json` file has this structure:

```json
{
  "network": {
    "name": "lightd-network",
    "subnet": "172.20.0.0/16",
    "gateway": "172.20.0.1"
  },
  "ports": {
    "available_ports": [9001, 9002, 9003, ...],
    "default_host_ip": "0.0.0.0",
    "reserved_ports": [22, 80, 443, 3000, 8080]
  }
}
```

### How It Works

- The panel manages which ports are available by adding/removing them from the `available_ports` array
- When a container needs a port, Lightd picks from this array
- Ports can only be removed if they're not currently allocated to a container
- Reserved ports cannot be added to the pool

## API Endpoints

### Get Port Pool Information

Get detailed information about the port pool.

```bash
GET /network/ports/pool
```

**Response:**
```json
{
  "success": true,
  "data": {
    "total_available": 50,
    "reserved_ports": [22, 80, 443, 3000, 8080],
    "allocated_ports": 15,
    "unallocated_ports": 35,
    "available_ports_sample": [9001, 9002, 9003, ...]
  }
}
```

### Bulk Add Ports

Add multiple ports to the available pool.

```bash
POST /network/ports/add
Content-Type: application/json

{
  "ports": [10000, 10001, 10002, 10003, 10004]
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "success": true,
    "message": "Added 5 ports to allocation pool",
    "ports_affected": 5
  }
}
```

**Notes:**
- Duplicate ports are automatically skipped
- Reserved ports are automatically skipped with a warning
- Ports are automatically sorted after adding
- Changes are persisted to `network.json`

### Bulk Remove Ports

Remove multiple ports from the available pool.

```bash
POST /network/ports/remove
Content-Type: application/json

{
  "ports": [10000, 10001, 10002]
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "success": true,
    "message": "Removed 3 ports from allocation pool",
    "ports_affected": 3
  }
}
```

**Notes:**
- Cannot remove ports that are currently allocated to containers
- Non-existent ports are silently skipped
- Changes are persisted to `network.json`

### Get Available Ports

Get a list of currently available (unallocated) ports (up to 100).

```bash
GET /network/ports
```

**Response:**
```json
{
  "success": true,
  "data": {
    "available_ports": [9001, 9002, 9003, ...],
    "port_range": [9000, 19999],
    "allocated_count": 15
  }
}
```

## Container Port Management

These endpoints work with the port pool to allocate/deallocate ports for containers.

### Add Port Binding

```bash
POST /containers/:id/network/ports
Content-Type: application/json

{
  "container_port": "25565",
  "host_port": "auto",  // or specific port like "9000"
  "host_ip": "0.0.0.0",
  "protocol": "tcp"
}
```

### Remove Port Binding

```bash
POST /containers/:id/network/ports/remove
Content-Type: application/json

{
  "container_port": "25565"
}
```

### Update Port Binding

```bash
POST /containers/:id/network/ports/update
Content-Type: application/json

{
  "container_port": "25565",
  "new_host_port": "auto",  // or specific port
  "host_ip": "0.0.0.0"
}
```

### Apply Port Changes

Recreate the container to apply port changes.

```bash
POST /containers/:id/network/apply
```

## Examples

### Add a range of ports to the pool

```bash
# Add ports 10000-10100 to the pool
curl -X POST http://localhost:8080/network/ports/add \
  -H "Content-Type: application/json" \
  -d '{"ports": [10000,10001,10002,10003,10004,10005,10006,10007,10008,10009,10010]}'
```

### Check port pool status

```bash
curl http://localhost:8080/network/ports/pool
```

### Remove unused ports from the pool

```bash
curl -X POST http://localhost:8080/network/ports/remove \
  -H "Content-Type: application/json" \
  -d '{"ports": [9001, 9002, 9003]}'
```

### Get available ports

```bash
curl http://localhost:8080/network/ports
```

## Panel Integration

The panel should:

1. **Initialize the node** with a set of ports when first setting up:
   ```bash
   POST /network/ports/add
   {"ports": [9000, 9001, 9002, ..., 19999]}
   ```

2. **Monitor port usage** via:
   ```bash
   GET /network/ports/pool
   ```

3. **Expand capacity** by adding more ports when needed:
   ```bash
   POST /network/ports/add
   {"ports": [20000, 20001, 20002, ...]}
   ```

4. **Reclaim ports** by removing unused ones:
   ```bash
   POST /network/ports/remove
   {"ports": [9000, 9001, 9002]}
   ```

## Migration Notes

If you have an existing `network.json`:

- The `available_ports` array is the source of truth
- Ports are allocated from this array on a first-available basis
- The panel has full control over which ports are in the pool
