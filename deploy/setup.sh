#!/usr/bin/env bash
# One-shot setup on a fresh Ubuntu EC2 instance. Run as the ubuntu user:
#   curl -fsSL https://raw.githubusercontent.com/Chennn0519/crypto-data-recorder/master/deploy/setup.sh | bash
set -euo pipefail

REPO="https://github.com/Chennn0519/crypto-data-recorder.git"
DIR=/opt/crypto-data-recorder

sudo apt-get update -y
sudo apt-get install -y python3-venv git

if [ ! -d "$DIR" ]; then
    sudo git clone "$REPO" "$DIR"
    sudo chown -R ubuntu:ubuntu "$DIR"
else
    git -C "$DIR" pull
fi

cd "$DIR"
python3 -m venv .venv
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet requests websockets

sudo cp deploy/crypto-recorder.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-recorder

echo "done. check with: systemctl status crypto-recorder"
