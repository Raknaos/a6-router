# -*- coding: utf-8 -*-
"""Verifie que le router tire bien sa decision du marche 30 j (fichier VPS)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import router as R  # noqa: E402  (module sans effet de bord hors __main__)

print("market30_max_age_s =", R.CFG.get("market30_max_age_s"),
      "| fichier present:", os.path.exists(R.MKT30_FILE))
print()
for m in R.MODELS:
    r = R.marketplace_best(m)
    b = (r or {}).get("best") or {}
    alts = (r or {}).get("alts") or []
    succ = max(float(b.get("success") or 100) / 100.0, 0.01)
    if b.get("cost30"):
        cout = b["cost30"] / succ
    else:
        cout = (b.get("in", 0) + b.get("out", 0)) / 2 / succ
    print(f"{m:30s} src={r.get('src')} maj={r.get('maj_s')}s n_alive={r.get('n_alive')}")
    print(f"{'':30s} best={b.get('supplier')} ch{b.get('channel_id')} "
          f"| cout_espere={cout:.6f} | cost30={b.get('cost30')} pire={b.get('worst')} "
          f"sr24={b.get('success')} n24={b.get('n24')}")
    print(f"{'':30s} in={b.get('in')} out={b.get('out')} cache={b.get('cache_read')} "
          f"| in30={b.get('in30')} out30={b.get('out30')} cache30={b.get('cache30')}")
    print(f"{'':30s} panneau epingle: {[a.get('supplier') for a in [b] + alts]}")
    print()
