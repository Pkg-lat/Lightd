# Lightd Security Setup

## Overview

Running Lightd as root is dangerous and unnecessary. This guide shows how to run Lightd as an unprivileged user with proper security hardening.

## Quick Setup

```bash
# Run the automated setup script
sudo bash setup-user.sh
```

This creates:
- Unprivileged user `lightd` with no login shell
- Dedicated group `lightd`
- Proper directory structure with correct permissions
- Systemd service with security hardening
- Logrotate configuration

## Manual Setup

If you prefer to set things up manually:

### 1. Create User and Group

```bash
# Create system group
sudo groupadd --system lightd

# Create system user (no login shell)
sudo useradd --system \
    --gid lightd \
    --home-dir /opt/lightd \
    --shell /usr/sbin/nologin \
    --comment "Lightd Daemon User" \
    lightd

# Add to docker group (required for Docker socket access)
sudo usermod -aG docker lightd
```

### 2. Create Directory Structure

```bash
# Application directory
sudo mkdir -p /opt/lightd

# Storage directory
sudo mkdir -p /var/lib/lightd/{containers,volumes,snapshots}

# Logs directory
sudo mkdir -p /var/log/lightd

# Set ownership
sudo chown -R lightd:lightd /opt/lightd
sudo chown -R lightd:lightd /var/lib/lightd
sudo chown -R lightd:lightd /var/log/lightd

# Set permissions (750 = owner rwx, group rx, others none)
sudo chmod 750 /opt/lightd
sudo chmod 750 /var/lib/lightd
sudo chmod 750 /var/log/lightd
```

### 3. Install Binary and Configs

```bash
# Copy binary
sudo cp target/release/lightd /opt/lightd/lightd
sudo chown lightd:lightd /opt/lightd/lightd
sudo chmod 750 /opt/lightd/lightd

# Copy configs
sudo cp config.json /opt/lightd/config.json
sudo cp network.json /opt/lightd/network.json
sudo chown lightd:lightd /opt/lightd/*.json
sudo chmod 640 /opt/lightd/*.json
```

### 4. Update Config Paths

Edit `/opt/lightd/config.json` to use the new paths:

```json
{
  "server": {
    "host": "0.0.0.0",
    "port": 8070
  },
  "docker": {
    "socket_path": "/var/run/docker.sock"
  },
  "storage": {
    "base_path": "/var/lib/lightd",
    "containers_path": "/var/lib/lightd/containers",
    "volumes_path": "/var/lib/lightd/volumes"
  }
}
```

### 5. Create Systemd Service

Create `/etc/systemd/system/lightd.service`:

```ini
[Unit]
Description=Lightd Container Daemon
After=network.target docker.service
Requires=docker.service

[Service]
Type=simple
User=lightd
Group=lightd
WorkingDirectory=/opt/lightd
ExecStart=/opt/lightd/lightd
Restart=always
RestartSec=10

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/lightd /var/log/lightd
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictRealtime=true
RestrictNamespaces=true
LockPersonality=true
MemoryDenyWriteExecute=true
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
SystemCallFilter=@system-service
SystemCallErrorNumber=EPERM

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=lightd

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable lightd
sudo systemctl start lightd
sudo systemctl status lightd
```

## Security Features

### User Isolation

- **No login shell**: User cannot be logged into
- **System user**: UID < 1000, not shown in login screens
- **Dedicated group**: Isolated from other users
- **No home directory access**: Cannot access user files

### Systemd Hardening

- **NoNewPrivileges**: Cannot gain new privileges
- **PrivateTmp**: Isolated /tmp directory
- **ProtectSystem**: Read-only system directories
- **ProtectHome**: No access to user home directories
- **ProtectKernelTunables**: Cannot modify kernel parameters
- **ProtectKernelModules**: Cannot load kernel modules
- **RestrictNamespaces**: Limited namespace creation
- **MemoryDenyWriteExecute**: No writable+executable memory
- **SystemCallFilter**: Restricted system calls

### File Permissions

```
/opt/lightd/              750 (lightd:lightd)
├── lightd                750 (lightd:lightd) - executable
├── config.json           640 (lightd:lightd) - config
└── network.json          640 (lightd:lightd) - config

/var/lib/lightd/          750 (lightd:lightd)
├── containers/           750 (lightd:lightd)
├── volumes/              750 (lightd:lightd)
└── snapshots/            750 (lightd:lightd)

/var/log/lightd/          750 (lightd:lightd)
```

## Docker Socket Access

The `lightd` user needs access to the Docker socket. This is achieved by adding the user to the `docker` group:

```bash
sudo usermod -aG docker lightd
```

**Note**: Members of the `docker` group have effective root access because they can run containers with root privileges. This is a Docker limitation, not a Lightd issue. To mitigate:

1. Use Docker's rootless mode (advanced)
2. Implement Docker authorization plugins
3. Use AppArmor/SELinux profiles
4. Monitor Docker API calls

## Logging

Logs are written to systemd journal and can be viewed with:

```bash
# Follow logs
sudo journalctl -u lightd -f

# View recent logs
sudo journalctl -u lightd -n 100

# View logs from today
sudo journalctl -u lightd --since today

# View logs with priority
sudo journalctl -u lightd -p err
```

Logrotate automatically rotates logs in `/var/log/lightd/` (if file logging is enabled).

## Monitoring

Check service status:

```bash
# Service status
sudo systemctl status lightd

# Is service running?
sudo systemctl is-active lightd

# Is service enabled?
sudo systemctl is-enabled lightd

# Resource usage
sudo systemctl show lightd --property=MemoryCurrent,CPUUsageNSec
```

## Troubleshooting

### Permission Denied Errors

```bash
# Check user exists
id lightd

# Check group membership
groups lightd

# Check directory ownership
ls -la /opt/lightd
ls -la /var/lib/lightd

# Check Docker socket access
sudo -u lightd docker ps
```

### Service Won't Start

```bash
# Check logs
sudo journalctl -u lightd -n 50

# Check config syntax
sudo -u lightd /opt/lightd/lightd --check-config

# Test manually
sudo -u lightd /opt/lightd/lightd
```

### Docker Socket Access Issues

```bash
# Check docker group exists
getent group docker

# Check user is in docker group
groups lightd | grep docker

# Check socket permissions
ls -la /var/run/docker.sock

# Restart after group changes
sudo systemctl restart lightd
```

## Upgrading

When upgrading Lightd:

```bash
# Stop service
sudo systemctl stop lightd

# Backup config
sudo cp /opt/lightd/config.json /opt/lightd/config.json.backup

# Install new binary
sudo cp target/release/lightd /opt/lightd/lightd
sudo chown lightd:lightd /opt/lightd/lightd
sudo chmod 750 /opt/lightd/lightd

# Start service
sudo systemctl start lightd

# Check status
sudo systemctl status lightd
```

## Uninstall

To completely remove Lightd:

```bash
# Stop and disable service
sudo systemctl stop lightd
sudo systemctl disable lightd

# Remove service file
sudo rm /etc/systemd/system/lightd.service
sudo systemctl daemon-reload

# Remove user and group
sudo userdel lightd
sudo groupdel lightd

# Remove directories (WARNING: deletes all data!)
sudo rm -rf /opt/lightd
sudo rm -rf /var/lib/lightd
sudo rm -rf /var/log/lightd

# Remove logrotate config
sudo rm /etc/logrotate.d/lightd
```

## Best Practices

1. **Never run as root**: Always use the dedicated `lightd` user
2. **Restrict config access**: Keep configs readable only by `lightd` user
3. **Monitor logs**: Regularly check for security issues
4. **Update regularly**: Keep Lightd and Docker up to date
5. **Backup configs**: Keep backups of config files
6. **Use firewall**: Restrict access to Lightd port (8070)
7. **Enable audit logging**: Track all administrative actions
8. **Rotate logs**: Ensure logs don't fill disk
9. **Monitor resources**: Watch for unusual resource usage
10. **Test updates**: Test in staging before production

## Additional Security

### Firewall Rules

```bash
# Allow only from specific IP
sudo ufw allow from 192.168.1.0/24 to any port 8070

# Or allow from localhost only
sudo ufw allow from 127.0.0.1 to any port 8070
```

### AppArmor Profile

Create `/etc/apparmor.d/opt.lightd.lightd`:

```
#include <tunables/global>

/opt/lightd/lightd {
  #include <abstractions/base>
  #include <abstractions/nameservice>

  /opt/lightd/lightd mr,
  /opt/lightd/*.json r,
  /var/lib/lightd/** rw,
  /var/log/lightd/** rw,
  /var/run/docker.sock rw,
  
  # Deny everything else
  deny /** wx,
}
```

Load profile:

```bash
sudo apparmor_parser -r /etc/apparmor.d/opt.lightd.lightd
```

### SELinux Policy

For SELinux systems, create a custom policy to restrict Lightd's access.

## References

- [Docker Security Best Practices](https://docs.docker.com/engine/security/)
- [Systemd Security Hardening](https://www.freedesktop.org/software/systemd/man/systemd.exec.html)
- [Linux User Management](https://www.kernel.org/doc/html/latest/admin-guide/security.html)
