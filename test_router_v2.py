"""Tests v2.2.0 — exécution contre une instance live (python test_router_v2.py [base_url]).
Lecture seule côté secrets : le token admin est lu dans update_token.json local, jamais imprimé.
Aucun appel chat payant ici (max 1 requête 25 tokens si RUN_CHAT=1)."""
import json, sys, os, time, urllib.request, urllib.error, hashlib

BASE = sys.argv[1] if len(sys.argv) > 1 else 'http://127.0.0.1:8795'
DIR = os.path.dirname(os.path.abspath(__file__))
TOK = ''
try:
    TOK = json.load(open(os.path.join(DIR, 'update_token.json'), encoding='utf-8')).get('token', '')
except Exception:
    pass

PASS, FAIL = [], []

def check(name, cond, detail=''):
    (PASS if cond else FAIL).append(name)
    print(('PASS ' if cond else 'FAIL ') + name + ((' — ' + detail) if detail and not cond else ''))

def get(path, timeout=8, raw=False):
    try:
        r = urllib.request.urlopen(BASE + path, timeout=timeout)
        b = r.read()
        return (b if raw else json.loads(b))
    except urllib.error.HTTPError as e:
        return {'__http': e.code, 'body': e.read()[:200]}
    except Exception as e:
        return {'__err': str(e)[:120]}

def hget(path):
    try:
        r = urllib.request.urlopen(BASE + path, timeout=8)
        return r.status, dict(r.getheaders())
    except Exception as e:
        return 0, {'err': str(e)[:80]}

# 1. /health enrichi
h = get('/health')
check('/health ok', h.get('ok') is True)
check('/health version', str(h.get('version', '')) >= '2.2.0', str(h.get('version')))
check('/health uptime_s présent', isinstance(h.get('uptime_s'), int))
check('/health savings_usd présent', 'savings_usd' in h)
check('/health pin présent', 'pin' in h)

# 2. /v1/models avec prix
m = get('/v1/models')
ids = [x['id'] for x in m.get('data', [])]
check('/models auto en 1er', ids and ids[0] == 'auto')
priced = [x for x in m.get('data', [])[1:] if x.get('pricing_usd_per_mtok')]
check('/models prix présents', len(priced) >= 1, f'{len(priced)}/{len(ids)-1}')

# 3. /state enrichi
s = get('/state')
check('/state market_cache_age_s', 'market_cache_age_s' in s)
check('/state market prix', any((s.get('market') or {}).values()))
check('/state cooldowns remaining_s', all('remaining_s' in v for v in (s.get('cooldowns') or {}).values()))

# 4. /cache savings
c = get('/cache')
check('/cache savings_vs_pire', 'savings_usd_vs_pire_canal' in c)

# 5. /metrics Prometheus
mt = get('/metrics', raw=True)
try:
    txt = mt.decode()
except Exception:
    txt = ''
check('/metrics format', txt.startswith('a6router_'))
check('/metrics savings line', 'a6router_savings_usd_total' in txt)

# 6. /dashboard HTML
dh = get('/dashboard', raw=True)
try:
    html = dh.decode()
except Exception:
    html = ''
check('/dashboard HTML', html.startswith('<!doctype html>') and 'A6-Router' in html)
check('/dashboard auto-refresh', 'setInterval' in html)

# 7. admin sans token -> 403
r403 = get('/admin/config')
check('/admin/config sans token 403', r403.get('__http') == 403, str(r403)[:80])

# 8. admin avec token -> 200 + sha cohérent
cfg = get(f'/admin/config?token={TOK}') if TOK else {'__skip': 1}
if '__skip' not in cfg:
    local_sha = hashlib.sha256(open(os.path.join(DIR, 'config.json'), 'rb').read()).hexdigest()
    check('/admin/config sha == local', cfg.get('sha256', '') == local_sha)
    check('/admin/config floor 80', (cfg.get('config') or {}).get('min_success_rate') == 80)
else:
    print('SKIP /admin/config (pas de token local)')

# 9. /admin/probe sans token -> 403 ; avec token -> 202
p403 = get('/admin/probe')
check('/admin/probe sans token 403', p403.get('__http') == 403)
p202 = get(f'/admin/probe?token={TOK}') if TOK else {'__skip': 1}
check('/admin/probe 202' if '__skip' not in p202 else 'SKIP probe', ('__skip' not in p202 and p202.get('probing') is True) or '__skip' in p202)

# 10. 404 propre modèle inconnu
bad = urllib.request.Request(BASE + '/v1/chat/completions',
    data=json.dumps({'model': 'modelexistantpas', 'max_tokens': 5, 'messages': [{'role': 'user', 'content': 'x'}]}).encode(),
    headers={'Content-Type': 'application/json'})
try:
    urllib.request.urlopen(bad, timeout=8)
    check('404 modèle inconnu', False, 'pas de 404')
except urllib.error.HTTPError as e:
    j = json.loads(e.read())
    check('404 modèle inconnu', e.code == 404 and j['error']['code'] == 'model_not_found')

# 11. n>1 rejeté
bad2 = urllib.request.Request(BASE + '/v1/chat/completions',
    data=json.dumps({'model': 'auto', 'n': 3, 'max_tokens': 5, 'messages': [{'role': 'user', 'content': 'x'}]}).encode(),
    headers={'Content-Type': 'application/json'})
try:
    urllib.request.urlopen(bad2, timeout=8)
    check('n>1 rejeté 400', False, 'pas de 400')
except urllib.error.HTTPError as e:
    j = json.loads(e.read())
    check('n>1 rejeté 400', e.code == 400 and j['error']['code'] == 'unsupported_n')

# 12. headers de réponse (X-A6-Router-Pin) — sur 404 la réponse _json passe par extra_headers ? non.
# On vérifie simplement que /metrics et /dashboard n'exposent pas de secret.
check('/metrics sans secret', 'sk-' not in txt)
check('/dashboard sans secret', 'sk-' not in html)

print(f'\n== RÉSULTAT: {len(PASS)} PASS / {len(FAIL)} FAIL')
if FAIL:
    print('ÉCHECS:', ', '.join(FAIL))
    sys.exit(1)
