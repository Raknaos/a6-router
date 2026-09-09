#!/bin/bash
# vérification finale 1 host : service actif, user, version
HOST="$1"
ssh -i C:/Users/bapti/.ssh/pullbg_vps -o ConnectTimeout=8 -o StrictHostKeyChecking=no root@$HOST 'echo -n "active=$(systemctl is-active a6-router) user=$(ps -o user= -p $(systemctl show -p MainPID --value a6-router) 2>/dev/null | tr -d " ") version=$(python3 -c "import json;print(json.load(open(\"/opt/a6-router/version.json\"))[\"version\"])")"; curl -s http://127.0.0.1:8791/health | head -c 60; echo'
