# -*- coding: utf-8 -*-
"""Recupere /api/prices du worker marche 30j (VPS Quota.Hub, loopback 8891)
et l'ecrit dans a6-router/market30_vps.json, de facon atomique.

Le worker n'ecoute que sur 127.0.0.1 du VPS : on passe par un pull SSH.
Aucun secret n'est lu ni affiche (la cle SSH est designee par un chemin).
"""
import json
import os
import subprocess
import sys
import time

DIR = r"C:\Users\bapti\Documents\Projets_Hermes\a6-router"
OUT = os.path.join(DIR, "market30_vps.json")
TMP = OUT + ".tmp"
KEY = r"C:\Users\bapti\.ssh\pullbg_vps"
HOST = "root@23.94.144.66"
REMOTE = "curl -s -m 25 http://127.0.0.1:8891/api/prices"
LOG = os.path.join(DIR, "market30_pull.log")


def log(msg):
    ligne = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    try:
        with open(LOG, "a", encoding="utf-8") as fh:
            fh.write(ligne + "\n")
    except Exception:
        pass
    print(ligne)


def main():
    cmd = ["ssh", "-i", KEY, "-o", "StrictHostKeyChecking=no",
           "-o", "ConnectTimeout=15", "-o", "BatchMode=yes", HOST, REMOTE]
    try:
        p = subprocess.run(cmd, capture_output=True, timeout=60)
    except Exception as exc:
        log(f"ECHEC ssh: {type(exc).__name__}: {str(exc)[:80]}")
        return 1
    if p.returncode != 0:
        log(f"ECHEC ssh rc={p.returncode}: {p.stderr.decode('utf-8', 'replace')[:120]}")
        return 1
    brut = p.stdout.decode("utf-8", "replace").strip()
    try:
        d = json.loads(brut)
    except Exception as exc:
        log(f"ECHEC json: {type(exc).__name__}: {brut[:80]}")
        return 1
    modeles = d.get("models") or {}
    # on n'ecrit pas un instantane partiel : sans aucun modele, on garde le precedent
    if not any(v.get("best") for v in modeles.values()):
        log("ECHEC aucun modele avec un best -> instantane precedent conserve")
        return 1
    d["_pulled_at"] = time.time()
    with open(TMP, "w", encoding="utf-8") as fh:
        json.dump(d, fh, ensure_ascii=False)
    os.replace(TMP, OUT)
    avec = [m for m, v in modeles.items() if v.get("best")]
    log(f"OK {len(avec)} modeles avec best | age worker {d.get('age_s')}s "
        f"| passes {d.get('passes')} | {', '.join(sorted(avec))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
