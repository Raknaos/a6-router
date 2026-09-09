#!/bin/bash
# déclenche l'auto-update sur un VPS : token lu LOCALEMENT, jamais affiché
HOST="$1"
SSH="ssh -i C:/Users/bapti/.ssh/pullbg_vps -o ConnectTimeout=10 -o StrictHostKeyChecking=no root@$HOST"
echo "=== $HOST ==="
$SSH 'TOK=$(python3 -c "import json;print(json.load(open(\"/opt/a6-router/update_token.json\"))[\"token\"])"); curl -s "http://127.0.0.1:8791/admin/update?token=$TOK" ; sleep 10 ; curl -s http://127.0.0.1:8791/health | head -c 200 ; echo ; python3 -c "import json;print(\"version:\",json.load(open(\"/opt/a6-router/version.json\"))[\"version\"])"'
