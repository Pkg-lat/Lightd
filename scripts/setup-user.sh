#!/bin/bash
# Setup script for Lightd - Creates unprivileged user and configures permissions

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Lightd User Setup ===${NC}"

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Error: This script must be run as root${NC}"
    exit 1
fi

# Configuration
LIGHTD_USER="lightd"
LIGHTD_GROUP="lightd"
LIGHTD_HOME="/opt/lightd"
LIGHTD_STORAGE="/var/lib/lightd"
LIGHTD_LOGS="/var/log/lightd"

echo -e "${YELLOW}Creating user and group...${NC}"

# Create group if it doesn't exist
if ! getent group "$LIGHTD_GROUP" > /dev/null 2>&1; then
    groupadd --system "$LIGHTD_GROUP"
    echo -e "${GREEN}✓ Created group: $LIGHTD_GROUP${NC}"
else
    echo -e "${YELLOW}Group $LIGHTD_GROUP already exists${NC}"
fi

# Create user if it doesn't exist
if ! id "$LIGHTD_USER" > /dev/null 2>&1; then
    useradd --system \
        --gid "$LIGHTD_GROUP" \
        --home-dir "$LIGHTD_HOME" \
        --shell /usr/sbin/nologin \
        --comment "Lightd Daemon User" \
        "$LIGHTD_USER"
    echo -e "${GREEN}✓ Created user: $LIGHTD_USER${NC}"
else
    echo -e "${YELLOW}User $LIGHTD_USER already exists${NC}"
fi

# Add lightd user to docker group (required for Docker socket access)
echo -e "${YELLOW}Adding $LIGHTD_USER to docker group...${NC}"
if getent group docker > /dev/null 2>&1; then
    usermod -aG docker "$LIGHTD_USER"
    echo -e "${GREEN}✓ Added $LIGHTD_USER to docker group${NC}"
else
    echo -e "${RED}Warning: docker group not found. Install Docker first!${NC}"
fi

# Create directories
echo -e "${YELLOW}Creating directories...${NC}"

mkdir -p "$LIGHTD_HOME"
mkdir -p "$LIGHTD_STORAGE"/{containers,volumes,snapshots}
mkdir -p "$LIGHTD_LOGS"

# Set ownership
chown -R "$LIGHTD_USER:$LIGHTD_GROUP" "$LIGHTD_HOME"
chown -R "$LIGHTD_USER:$LIGHTD_GROUP" "$LIGHTD_STORAGE"
chown -R "$LIGHTD_USER:$LIGHTD_GROUP" "$LIGHTD_LOGS"

# Set permissions
chmod 750 "$LIGHTD_HOME"
chmod 750 "$LIGHTD_STORAGE"
chmod 750 "$LIGHTD_LOGS"

echo -e "${GREEN}✓ Created and configured directories${NC}"

# Copy binary if it exists
if [ -f "./target/release/lightd" ]; then
    echo -e "${YELLOW}Installing Lightd binary...${NC}"
    cp ./target/release/lightd "$LIGHTD_HOME/lightd"
    chown "$LIGHTD_USER:$LIGHTD_GROUP" "$LIGHTD_HOME/lightd"
    chmod 750 "$LIGHTD_HOME/lightd"
    echo -e "${GREEN}✓ Installed binary to $LIGHTD_HOME/lightd${NC}"
fi

# Copy config if it exists
if [ -f "./config.json" ]; then
    echo -e "${YELLOW}Installing config...${NC}"
    cp ./config.json "$LIGHTD_HOME/config.json"
    chown "$LIGHTD_USER:$LIGHTD_GROUP" "$LIGHTD_HOME/config.json"
    chmod 640 "$LIGHTD_HOME/config.json"
    echo -e "${GREEN}✓ Installed config to $LIGHTD_HOME/config.json${NC}"
fi

# Copy network config if it exists
if [ -f "./network.json" ]; then
    echo -e "${YELLOW}Installing network config...${NC}"
    cp ./network.json "$LIGHTD_HOME/network.json"
    chown "$LIGHTD_USER:$LIGHTD_GROUP" "$LIGHTD_HOME/network.json"
    chmod 640 "$LIGHTD_HOME/network.json"
    echo -e "${GREEN}✓ Installed network config to $LIGHTD_HOME/network.json${NC}"
fi

# Create systemd service
echo -e "${YELLOW}Creating systemd service...${NC}"

cat > /etc/systemd/system/lightd.service << EOF
[Unit]
Description=Lightd Container Daemon
After=network.target docker.service
Requires=docker.service

[Service]
Type=simple
User=$LIGHTD_USER
Group=$LIGHTD_GROUP
WorkingDirectory=$LIGHTD_HOME
ExecStart=$LIGHTD_HOME/lightd
Restart=always
RestartSec=10

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$LIGHTD_STORAGE $LIGHTD_LOGS
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
EOF

chmod 644 /etc/systemd/system/lightd.service
systemctl daemon-reload

echo -e "${GREEN}✓ Created systemd service${NC}"

# Create logrotate config
echo -e "${YELLOW}Creating logrotate config...${NC}"

cat > /etc/logrotate.d/lightd << EOF
$LIGHTD_LOGS/*.log {
    daily
    rotate 14
    compress
    delaycompress
    notifempty
    create 0640 $LIGHTD_USER $LIGHTD_GROUP
    sharedscripts
    postrotate
        systemctl reload lightd > /dev/null 2>&1 || true
    endscript
}
EOF

echo -e "${GREEN}✓ Created logrotate config${NC}"

# Summary
echo ""
echo -e "${GREEN}=== Setup Complete ===${NC}"
echo ""
echo "User: $LIGHTD_USER"
echo "Group: $LIGHTD_GROUP"
echo "Home: $LIGHTD_HOME"
echo "Storage: $LIGHTD_STORAGE"
echo "Logs: $LIGHTD_LOGS"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Edit config: sudo nano $LIGHTD_HOME/config.json"
echo "2. Edit network config: sudo nano $LIGHTD_HOME/network.json"
echo "3. Enable service: sudo systemctl enable lightd"
echo "4. Start service: sudo systemctl start lightd"
echo "5. Check status: sudo systemctl status lightd"
echo "6. View logs: sudo journalctl -u lightd -f"
echo ""
echo -e "${YELLOW}Security notes:${NC}"
echo "- Lightd runs as unprivileged user '$LIGHTD_USER'"
echo "- Docker access via docker group membership"
echo "- Systemd security hardening enabled"
echo "- Logs rotated automatically"
echo ""
