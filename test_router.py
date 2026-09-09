import json, urllib.request, time

tests = [
    ('Req 1 — calcul simple', 'auto', 'Calcule 27*43 et reponds juste par le nombre.'),
    ('Req 2 — salutation', 'auto', 'Dis bonjour en une phrase.'),
    ('Req 3 — modele force', 'gpt-5.6-luna', 'Dis bonjour en une phrase.'),
    ('Req 4 — apres TTL 65s', 'auto', 'Que vaut 12*12 ?'),
]
for label, model, msg in tests:
    payload = json.dumps({'model': model, 'messages': [{'role': 'user', 'content': msg}], 'max_tokens': 60}).encode()
    r = urllib.request.Request('http://127.0.0.1:8791/v1/chat/completions', data=payload,
        method='POST', headers={'Content-Type': 'application/json'})
    t0 = time.time()
    try:
        with urllib.request.urlopen(r, timeout=120) as resp:
            d = json.loads(resp.read().decode())
        dt = time.time() - t0
        ch = d.get('choices', [{}])[0]
        c = (ch.get('message', {}) or {}).get('content', '') or ''
        rc = ch.get('message', {}).get('reasoning_content', '') or ''
        ar = d.get('a6_router', {})
        u = d.get('usage', {})
        print(f"{label:26s} | {dt:5.1f}s | choisi={ar.get('chosen_model')} supplier={ar.get('supplier')} "
              f"cout~{ar.get('est_cost_usd')}$ | tok {u.get('prompt_tokens')}+{u.get('completion_tokens')} | rep: {c[:40] or rc[:40]}")
    except Exception as e:
        print(f"{label:26s} | ERREUR {e}")
    time.sleep(2)

# /v1/models
r = urllib.request.Request('http://127.0.0.1:8791/v1/models')
with urllib.request.urlopen(r, timeout=10) as resp:
    d = json.loads(resp.read().decode())
print('\nModeles exposes:', [m['id'] for m in d['data']])
