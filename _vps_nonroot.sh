#!/bin/bash
# P0 sécurité (audit v2.0.0) : routeur non-root sur les VPS.
# Crée le compte a6router, chown /opt/a6-router, installe le service durci, restart.
# SÉQUENTIEL — 1 host à la fois.
HOST="$1"
SSH="ssh -i C:/Users/bapti/.ssh/pullbg_vps -o ConnectTimeout=10 -o StrictHostKeyChecking=no root@$HOST"
echo "=== $HOST ==="
$SSH 'id -u a6router >/dev/null 2>&1 || useradd -r -s /usr/sbin/nologin -d /opt/a6-router a6router
chown -R a6router:a6router /opt/a6-router
chmod 600 /opt/a6-router/key.json /opt/a6-router/update_token.json 2>/dev/null
cp /opt/a6-router/a6-router.service /etc/systemd/system/a6-router.service 2>/dev/null || { scp_local=1; }
if [ "$scp_local" = "1" ]; then echo "service file manquant sur le VPS"; fi
systemctl daemon-reload
systemctl restart a6-router
sleep 6
systemctl is-active a6-router
ps -o user= -p $(systemctl show -p MainPID --value a6-router) 2>/dev/null
curl -s http://127.0.0.1:8791/health | head -c 80; echo'
