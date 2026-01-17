# Lightd vs Pterodactyl Wings - Architecture Comparison

## Overview

Yes, Lightd is similar to Pterodactyl Wings in concept - both are daemon processes that manage Docker containers on behalf of a control panel. However, there are key architectural differences.

## Architecture Comparison

### Pterodactyl Wings

```
┌─────────────────────────────────────────┐
│         Pterodactyl Panel               │
│         (PHP/Laravel)                   │
│         Port 80/443                     │
└──────────────┬──────────────────────────┘
               │ HTTP/HTTPS API
               │
┌──────────────▼──────────────────────────┐
│         Wings Daemon                    │
│         (Go)                            │
│         Port 8080 (default)             │
│         Runs as: root or pterodactyl    │
├─────────────────────────────────────────┤
│  - Container Management                 │
│  - File Management (SFTP)               │
│  - Console Streaming (WebSocket)        │
│  - Resource Monitoring                  │
│  - Backup Management                    │
└──────────────┬──────────────────────────┘
               │ Docker API
               │
┌──────────────▼──────────────────────────┐
│         Docker Engine                   │
│         /var/run/docker.sock            │
└─────────────────────────────────────────┘
```

### Your System (Lightd)

```
┌─────────────────────────────────────────┐
│         Panel (server-rs)               │
│         (Rust/Axum)                     │
│         Port 3000                       │
│         + MongoDB + Redis               │
└──────────────┬──────────────────────────┘
               │ HTTP API + Auth Token
               │
┌──────────────▼──────────────────────────┐
│         Lightd Daemon                   │
│         (Rust/Axum)                     │
│         Port 8070 (default)             │
│         Runs as: lightd (unprivileged)  │
├─────────────────────────────────────────┤
│  - Container Lifecycle                  │
│  - File Management (Direct API)         │
│  - Console Streaming (WebSocket)        │
│  - Resource Monitoring + RU Billing     │
│  - Snapshot Management                  │
│  - Network/Port Management              │
│  - Firewall Rules (iptables)            │
└──────────────┬──────────────────────────┘
               │ Docker API
               │
┌──────────────▼──────────────────────────┐
│         Docker Engine                   │
│         /var/run/docker.sock            │
└─────────────────────────────────────────┘
```

## Key Differences

### 1. User Execution

**Pterodactyl Wings:**
- Often runs as `root` (dangerous)
- Can run as `pterodactyl` user but requires careful setup
- Needs root for SFTP server and some operations

**Lightd:**
- Designed to run as unprivileged `lightd` user
- No SFTP server (uses HTTP API instead)
- Docker access via `docker` group membership
- Systemd security hardening enabled

### 2. File Management

**Pterodactyl Wings:**
- Built-in SFTP server (port 2022)
- Direct file access via SFTP protocol
- Requires root or complex permissions

**Lightd:**
- HTTP API for file operations
- Token-based authentication
- Direct upload/download endpoints
- No separate SFTP server needed
- Safer and simpler

### 3. Authentication

**Pterodactyl Wings:**
- Node token in config
- SFTP uses separate authentication
- Panel generates temporary tokens

**Lightd:**
- Bearer token authentication
- Single auth mechanism for all operations
- Configurable in `config.json`

### 4. Container Management

**Pterodactyl Wings:**
- Containers run as root inside
- Uses Docker's user namespace remapping (optional)
- Complex permission management

**Lightd:**
- Containers run as root inside (Docker limitation)
- Daemon runs as unprivileged user
- Isolation via Docker's security features
- Firewall rules per container

### 5. Resource Monitoring

**Pterodactyl Wings:**
- Basic CPU/Memory/Disk monitoring
- Stats sent to panel periodically
- No built-in billing

**Lightd:**
- Advanced RU (Resource Unit) system
- Real-time monitoring (1s intervals)
- Built-in billing calculations
- Weighted resource usage
- Suspend on insufficient funds

### 6. Networking

**Pterodactyl Wings:**
- Port allocation managed by panel
- Simple port mapping
- No built-in firewall

**Lightd:**
- Dynamic port pool management
- Bulk port add/remove via API
- Per-container firewall rules
- Network isolation

## How Lightd Runs

### Process Flow

1. **Startup**
   ```bash
   # Systemd starts Lightd as 'lightd' user
   systemctl start lightd
   
   # Lightd process:
   /opt/lightd/lightd
   ├── User: lightd
   ├── Group: lightd
   ├── Working Dir: /opt/lightd
   └── Permissions: No root, no login shell
   ```

2. **Docker Access**
   ```bash
   # Lightd user is in 'docker' group
   groups lightd
   # Output: lightd docker
   
   # Can access Docker socket
   /var/run/docker.sock
   ├── Owner: root:docker
   └── Permissions: srw-rw---- (660)
   ```

3. **Container Creation**
   ```
   Panel Request → Lightd API → Docker API → Container Created
   
   Container runs as:
   ├── Inside: root (Docker default)
   ├── Managed by: lightd user
   ├── Isolated: Docker namespaces
   └── Limited: cgroups (CPU/Memory/Disk)
   ```

4. **File Operations**
   ```
   User → Panel → Lightd API → Docker Exec → File Operation
   
   No SFTP needed:
   ├── Upload: POST /containers/:id/files/upload
   ├── Download: GET /containers/:id/files/download
   ├── List: GET /containers/:id/files
   └── Delete: DELETE /containers/:id/files
   ```

5. **Console Access**
   ```
   User → Panel → Lightd WebSocket → Docker Attach → Container Console
   
   Real-time streaming:
   ├── WebSocket connection
   ├── Token authentication
   └── Bidirectional I/O
   ```

## Security Model

### Pterodactyl Wings

```
Root User
├── Wings Process (root)
│   ├── SFTP Server (root)
│   └── Docker Operations (root)
└── Containers (root inside)
```

**Risk**: If Wings is compromised, attacker has root access to host.

### Lightd

```
Root User
└── Docker Daemon (root)
    └── Containers (root inside, isolated)

Lightd User (unprivileged)
├── Lightd Process (lightd)
│   ├── HTTP API (lightd)
│   ├── WebSocket (lightd)
│   └── Docker API Client (via docker group)
└── Storage (/var/lib/lightd, owned by lightd)
```

**Benefit**: If Lightd is compromised, attacker only has `lightd` user access, not root.

## Docker Group Caveat

Both systems have the same fundamental limitation:

```
docker group = effective root access
```

Why? Because Docker allows:
```bash
# Escape to host as root
docker run -v /:/host -it ubuntu chroot /host
```

**Mitigation strategies:**

1. **Docker Rootless Mode** (advanced)
   - Run Docker daemon as non-root
   - Containers run as non-root
   - More complex setup

2. **Authorization Plugins**
   - Control what Docker operations are allowed
   - Prevent dangerous mounts

3. **AppArmor/SELinux**
   - Mandatory Access Control
   - Restrict container capabilities

4. **Audit Everything**
   - Log all Docker operations
   - Monitor for suspicious activity

## Deployment Comparison

### Pterodactyl Wings

```bash
# Install Wings
curl -sSL https://get.pterodactyl.io/wings | bash

# Configure
nano /etc/pterodactyl/config.yml

# Start
systemctl start wings
```

### Lightd

```bash
# Build
cd Lightd
cargo build --release

# Setup user and permissions
sudo bash setup-user.sh

# Configure
sudo nano /opt/lightd/config.json

# Start
sudo systemctl start lightd
```

## Feature Comparison

| Feature | Pterodactyl Wings | Lightd |
|---------|------------------|--------|
| Container Management | ✅ | ✅ |
| File Management | ✅ SFTP | ✅ HTTP API |
| Console Access | ✅ WebSocket | ✅ WebSocket |
| Resource Monitoring | ✅ Basic | ✅ Advanced (RU) |
| Backups | ✅ | ✅ Snapshots |
| Port Management | ✅ Panel-managed | ✅ Dynamic Pool |
| Firewall | ❌ | ✅ Per-container |
| Billing Integration | ❌ | ✅ Built-in RU |
| User Isolation | ⚠️ Optional | ✅ Default |
| Language | Go | Rust |
| Security Hardening | ⚠️ Manual | ✅ Systemd |

## Performance

### Pterodactyl Wings
- Written in Go
- Goroutines for concurrency
- Good performance
- Mature and battle-tested

### Lightd
- Written in Rust
- Tokio async runtime
- Zero-cost abstractions
- Memory safe
- Newer, less battle-tested

## When to Use Each

### Use Pterodactyl Wings if:
- You need mature, proven software
- You want a large community
- You need extensive plugin ecosystem
- You're comfortable with PHP panel
- You need SFTP access

### Use Lightd if:
- You want modern Rust architecture
- You need built-in billing/RU system
- You want better security by default
- You prefer HTTP API over SFTP
- You need per-container firewalls
- You want dynamic port management

## Migration Path

If migrating from Pterodactyl to Lightd:

1. **Data Migration**
   - Export container configs from Pterodactyl
   - Import to your panel's MongoDB
   - Recreate containers via Lightd

2. **File Migration**
   - Copy container volumes
   - Adjust ownership to `lightd` user
   - Update paths in configs

3. **Port Migration**
   - Document current port allocations
   - Add ports to Lightd pool
   - Update firewall rules

4. **User Migration**
   - Export user data from Pterodactyl
   - Import to your panel
   - Update authentication

## Conclusion

Lightd is conceptually similar to Pterodactyl Wings but with:
- **Better security**: Unprivileged user by default
- **Modern architecture**: Rust + async
- **Built-in billing**: RU system
- **Simpler file management**: HTTP API instead of SFTP
- **More features**: Firewalls, dynamic ports, snapshots

Both systems share the Docker group limitation, but Lightd's design makes it safer by default and easier to harden further.
