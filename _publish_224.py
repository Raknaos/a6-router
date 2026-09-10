# -*- coding: utf-8 -*-
"""Prepare la release v2.2.4 : sondes PARTAGEES (sondeur unique 20 min) +
retenue minimale du modele (cache de prompts). Calcule les sha256, ecrit
version.json + manifest.json et signe.

N'imprime que la version et des prefixes de hachage — jamais la cle privee.
"""
import hashlib
import io
import json
import os
import sys
import time

DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(DIR)
sys.path.insert(0, DIR)


def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for b in iter(lambda: fh.read(1 << 16), b""):
            h.update(b)
    return h.hexdigest()


VER = "2.2.4"
router_sha = sha(os.path.join(DIR, "router.py"))
cfg_sha = sha(os.path.join(DIR, "config.json"))
today = time.strftime("%Y-%m-%d")

man_path = os.path.join(DIR, "manifest.json")
man = json.load(io.open(man_path, encoding="utf-8"))
man["version"] = VER
man["sha256"] = router_sha
man["config_sha256"] = cfg_sha
man["date"] = today
man.pop("signature", None)
json.dump(man, io.open(man_path, "w", encoding="utf-8"), indent=2)

json.dump({"version": VER, "router_sha256": router_sha, "date": today},
          io.open(os.path.join(DIR, "version.json"), "w", encoding="utf-8"),
          indent=2)

import router  # noqa: E402  (module sans effet de bord hors __main__)
sig = router.sign_manifest(man_path)

print("version        :", VER)
print("router sha256  :", router_sha[:16], "...")
print("config sha256  :", cfg_sha[:16], "...")
print("signature      :", sig)
ok, why = router.verify_manifest_sig(json.load(io.open(man_path, encoding="utf-8")))
print("verification   :", ok, "-", why)
sys.exit(0 if ok else 1)
