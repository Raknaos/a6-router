"""
A6-ROUTER v2 — Proxy local intelligent entre Hermes et A6API.
Choisit à CHAQUE requête le modèle au meilleur prix (canal le moins cher vivant),
arbitré par la latence réelle, avec failover et cooldown automatiques.

Fonctionnement :
  - Prix marché : endpoint public A6API, cache 60 s (stale-while-revalidate en fond).
  - Sonde réelle par modèle toutes les 20 min (~30 tokens, quasi gratuit) : latence réelle + vitamines.
  - Sélection à chaque requête : prix canal frais × pénalité latence (sonde + p50 marché).
  - Échec d'un canal (erreur/timeout/solde) -> cooldown, bascule immédiate sur le suivant.
  - Effort : le client choisit (reasoning_effort), le routeur le transmet tel quel.

Endpoints : /v1/chat/completions (stream OK) · /v1/models · /health · /state
Lancement : python router.py [--port 8791]
Logs : router.log (console), costs.jsonl (coûts), state.json (snapshot).
"""
import json, os, re, sys, time, threading, urllib.request, urllib.error, urllib.parse
import concurrent.futures as cf
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DIR = os.path.dirname(os.path.abspath(__file__))
CFG = json.load(open(os.path.join(DIR, 'config.json'), encoding='utf-8'))

def load_key():
    try:
        kj = json.load(open(os.path.join(DIR, 'key.json'), encoding='utf-8'))
        if kj.get('api_key', '').startswith('sk-'):
            return kj['api_key']
    except Exception:
        pass
    env = os.environ.get('HERMES_CUSTOM_A6API_API_KEY')
    if env and env.startswith('sk-'):
        return env
    home = os.path.expanduser('~')
    for p in (os.path.join(home, '.hermes', '.env'),
              os.path.join(home, 'AppData', 'Local', 'hermes', '.env')):
        try:
            content = open(p, encoding='utf-8').read()
        except Exception:
            continue
        m = re.search(r'HERMES_CUSTOM_A6API_API_KEY\s*=\s*["\']?([A-Za-z0-9_\-\.]+)', content)
        if not m:
            m = re.search(r'A6API[_-]?KEY\s*=\s*["\']?([A-Za-z0-9_\-\.]+)', content)
        if m:
            return m.group(1)
        for line in content.splitlines():
            if 'a6api' in line.lower() and '=' in line:
                k = line.split('=', 1)[1].strip().strip('"').strip("'")
                if k.startswith('sk-'):
                    return k
    raise SystemExit('Cle A6API introuvable (key.json, variable env ou ~/.hermes/.env)')

KEY = load_key()
API_BASE = 'https://api.a6api.com'
MKT_BASE = 'https://a6api.com/api/marketplace/public/channels/search'
MODELS = CFG['models']

STATE = {'models': {}, 'updated': None, 'cycles': 0, 'requests_served': 0, 'failovers': 0}
MKT = {}                      # model_id -> {'r': ..., 'ts': ...}
MKT_LOCK = threading.Lock()
COOLDOWN = {}                 # model_id -> (until_ts, reason)
LOCK = threading.Lock()

def log(msg):
    line = time.strftime('%H:%M:%S') + ' ' + str(msg)
    print(line, flush=True)

def cost_log(entry):
    try:
        with LOCK:
            with open(os.path.join(DIR, 'costs.jsonl'), 'a', encoding='utf-8') as f:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
    except Exception:
        pass

def save_state():
    try:
        with LOCK:
            snap = json.loads(json.dumps(STATE, default=str))
        with open(os.path.join(DIR, 'state.json'), 'w', encoding='utf-8') as f:
            json.dump(snap, f, ensure_ascii=False, indent=1, default=str)
    except Exception:
        pass

def in_cooldown(model_id):
    until, reason = COOLDOWN.get(model_id, (0, ''))
    if time.time() < until:
        return True, reason
    return False, ''

# ---------------- Marché (gratuit, TTL + refresh en fond) ----------------

def marketplace_best(model_id):
    url = f'{MKT_BASE}?page=1&page_size=50&model={urllib.parse.quote(model_id)}&sort=price'
    r = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + KEY, 'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(r, timeout=15) as resp:
        d = json.loads(resp.read().decode())
    items = d.get('data', {}).get('items', [])
    alive = [i for i in items if not i.get('supplier_channel_disabled')
             and i.get('listing_availability') == 1
             and i.get('recent_success_rate', 0) >= CFG['min_success_rate'] * 100
             and i.get('sample_count', 0) >= 20]
    alive.sort(key=lambda i: i.get('input_price_micros', 10**18))
    if not alive:
        return {'best': None, 'n_alive': 0, 'total': d.get('data', {}).get('total', 0)}
    b = alive[0]
    # liste des 3 premiers canaux pour failover prix si le 1er tombe
    alts = [{'supplier': x.get('supplier_nickname'),
             'in': x.get('input_price_micros', 0) / 1e6,
             'out': x.get('output_price_micros', 0) / 1e6,
             'success': x.get('recent_success_rate', 0) / 100,
             'p50_ms': x.get('recent_p50_ms')} for x in alive[:3]]
    best = alts[0]
    return {'best': best, 'alts': alts, 'n_alive': len(alive), 'total': d.get('data', {}).get('total', 0)}

def market_get(model_id, max_age_s=None):
    """Retourne immédiatement le cache. Un cache absent ne bloque jamais une requête."""
    max_age = max_age_s if max_age_s is not None else CFG['market_ttl_s']
    with MKT_LOCK:
        ent = MKT.get(model_id)
    if ent and (time.time() - ent['ts'] < max_age):
        return ent['r']
    if ent:
        def _bg():
            try:
                r = marketplace_best(model_id)
                with MKT_LOCK: MKT[model_id] = {'r': r, 'ts': time.time()}
            except Exception: pass
        threading.Thread(target=_bg, daemon=True).start()
        return ent['r']
    return None

def warm_market():
    """Précharge les prix en parallèle, sans ralentir le serveur."""
    def get(m):
        try:
            r = marketplace_best(m)
            with MKT_LOCK: MKT[m] = {'r': r, 'ts': time.time()}
        except Exception as e: log(f'MARCHÉ {m}: {str(e)[:60]}')
    with cf.ThreadPoolExecutor(max_workers=len(MODELS)) as ex:
        list(ex.map(get, MODELS))


# ---------------- Sonde réelle (payante, minuscule) ----------------

def probe_model(model_id):
    """Sonde STREAM avec un prompt réaliste : le TTFT mesuré est comparable aux requêtes."""
    payload = {'model': model_id, 'messages': [{'role': 'user',
               'content': 'Nomme 3 langages de programmation en une ligne.'}],
               'max_tokens': 40, 'reasoning_effort': 'low', 'stream': True}
    r = urllib.request.Request(API_BASE + '/v1/chat/completions', data=json.dumps(payload).encode(),
        method='POST', headers={'Authorization': 'Bearer ' + KEY, 'Content-Type': 'application/json'})
    t0 = time.time()
    try:
        with urllib.request.urlopen(r, timeout=CFG['probe_timeout_s']) as resp:
            ttft = None
            out = b''
            while True:
                chunk = resp.read(512)
                if not chunk:
                    break
                out += chunk
                if ttft is None and b'"content"' in chunk:
                    ttft = time.time() - t0
            dt = time.time() - t0
        tin = tout = 0
        try:
            u = json.loads(out.decode().split('data: [DONE]')[0].strip().split('\n')[-1][6:]).get('usage', {})
            tin = u.get('prompt_tokens') or 0
            tout = u.get('completion_tokens') or 0
        except Exception:
            pass
        anom = tin > 200
        return {'ok': True, 'latency_s': round(ttft or dt, 2), 'tin': tin, 'tout': tout, 'anomaly': anom}
    except urllib.error.HTTPError as e:
        try:
            err = json.loads(e.read().decode() or '{}')
            em = err.get('error', {})
            msg = em.get('message', f'HTTP {e.code}') if isinstance(em, dict) else f'HTTP {e.code}'
        except Exception:
            msg = f'HTTP {e.code}'
        return {'ok': False, 'error': msg[:120]}
    except Exception as e:
        return {'ok': False, 'error': str(e)[:120]}

def probe_cycle():
    for model_id in MODELS:
        cd, reason = in_cooldown(model_id)
        if cd:
            log(f'SONDE {model_id} ignorée (cooldown {reason})')
            continue
        mkt = market_get(model_id, max_age_s=5)
        probe = probe_model(model_id)
        prev = STATE['models'].get(model_id, {})
        entry = {'probe': probe, 'fails': prev.get('fails', 0), 'last_ok': prev.get('last_ok')}
        if probe.get('ok'):
            entry['fails'] = 0
            entry['last_ok'] = time.strftime('%H:%M:%S')
            NET_STREAK.pop(model_id, None)
            with MKT_LOCK:
                b = ((MKT.get(model_id) or {}).get('r') or {}).get('best')
            if b:
                est = probe['tin'] * b['in'] / 1e6 + probe['tout'] * b['out'] / 1e6
                entry['probe_cost_usd'] = round(est, 8)
            with LOCK:
                STATE['models'][model_id] = entry
        else:
            entry['fails'] = prev.get('fails', 0) + 1
            entry['last_error'] = probe.get('error', '')[:120]
            with LOCK:
                STATE['models'][model_id] = entry
            cost_log({'ts': time.strftime('%Y-%m-%d %H:%M:%S'), 'type': 'probe-fail', 'model': model_id,
                      'error': probe.get('error', '')[:120]})
        log(f"SONDE {model_id:30s} {'OK ' + str(probe.get('latency_s')) + 's' if probe.get('ok') else 'FAIL ' + (probe.get('error') or '')[:60]}")
        cost_log({'ts': time.strftime('%Y-%m-%d %H:%M:%S'), 'type': 'probe', 'model': model_id,
                  'ok': probe.get('ok', False), 'latency_s': probe.get('latency_s'),
                  'tin': probe.get('tin'), 'tout': probe.get('tout'),
                  'cost_usd': entry.get('probe_cost_usd')})
        save_state()
        time.sleep(0.4)
    with LOCK:
        STATE['cycles'] += 1
        STATE['updated'] = time.strftime('%Y-%m-%d %H:%M:%S')
    save_state()

def probe_loop():
    while True:
        try:
            probe_cycle()
        except Exception as e:
            log('ERREUR cycle sonde: ' + str(e)[:120])
        time.sleep(CFG['probe_interval_s'])

# ---------------- Sélection temps réel ----------------

def live_candidates(requested_model):
    """Liste ordonnée de modèles candidats pour CETTE requête.
    Prix marché (cache frais) + latence = max(sonde, p50 marché). Cooldowns respectés."""
    now = time.time()
    cand = []
    for m in MODELS:
        cd, reason = in_cooldown(m)
        if cd:
            continue
        mkt = market_get(m)
        b = (mkt or {}).get('best')
        if not b:
            continue
        with LOCK:
            pe = STATE['models'].get(m, {})
        probe = pe.get('probe') or {}
        # latence de référence pour l'EXCLUSION = sonde seule (petit prompt, normalisée) ;
        # le TTFT des grosses requêtes dépend de la taille du prompt et ne doit PAS exclure
        # (sinon tout est exclu dès que la session dépasse ~10k tokens)
        probe_lat = probe.get('latency_s') or 0
        if probe_lat > CFG['max_latency_s'] or b.get('success', 0) < CFG['min_success_rate']:
            continue
        cost_mid = (b['in'] + b['out']) / 2
        # SÉLECTION STRICTE : le moins cher parmi les éligibles (latence correcte = filtre)
        cand.append((cost_mid, m, b))
    cand.sort(key=lambda x: x[0])
    if requested_model and requested_model != 'auto' and requested_model in MODELS:
        cd, _ = in_cooldown(requested_model)
        rest = [m for _, m, _ in cand if m != requested_model]
        if cd:
            log(f'modele demandé {requested_model} en cooldown ({_reason_str}), candidats de secours')
            return rest, cand
        # même en cooldown fraîchement posé pendant la sélection, on essaie le demandé 1er
        return [requested_model] + rest, cand
    return [m for _, m, _ in cand], cand

def _reason_str(x):
    return str(x)

# ---------------- Forward + failover ----------------

def normalize_payload(model_id, payload):
    """Adapte la requête au modèle choisi : effort supporté + garde-fous."""
    body = dict(payload)
    body['model'] = model_id
    eff = (body.get('reasoning_effort') or '').lower()
    # les modèles économiques n'acceptent pas max/xhigh → forcer low (rapidité + coût)
    if eff in ('max', 'xhigh'):
        body['reasoning_effort'] = 'low'
        body.pop('reasoning', None)
    return body

def forward(model_id, payload, timeout):
    body = normalize_payload(model_id, payload)
    body.pop('stream_options', None)
    is_stream = bool(body.get('stream'))
    r = urllib.request.Request(API_BASE + '/v1/chat/completions', data=json.dumps(body).encode(),
        method='POST', headers={'Authorization': 'Bearer ' + KEY, 'Content-Type': 'application/json'})
    t0 = time.time()
    try:
        resp = urllib.request.urlopen(r, timeout=timeout)
        return resp, None, is_stream, time.time() - t0
    except urllib.error.HTTPError as e:
        try:
            err = json.loads(e.read().decode() or '{}')
            em = err.get('error', {})
            msg = em.get('message', f'HTTP {e.code}') if isinstance(em, dict) else f'HTTP {e.code}'
        except Exception:
            msg = f'HTTP {e.code}'
        return None, (e.code, msg), is_stream, time.time() - t0
    except Exception as e:
        return None, (-1, str(e)[:120]), is_stream, time.time() - t0

def classify_error(status, msg):
    m = (msg or '').lower()
    if '可用额度不足' in msg or 'insufficient' in m or '余额' in msg:
        return 'solde'
    if status in (401, 403):
        return 'solde'   # clé/groupes : cooldown long
    if status == 503 or '没有可用渠道' in msg or 'no_available_channel' in msg:
        return 'canal'
    if status == 404 or '不支持' in msg:
        return 'canal'
    if status == 400:
        return 'params'
    if status in (429,):
        return 'rate'
    return 'net'

COOLDOWN_DUR = {'solde': CFG['cooldown_solde_s'], 'canal': CFG['cooldown_s'],
                'params': CFG['cooldown_s'], 'rate': 60, 'net': 90}
NET_STREAK = {}   # échecs réseau consécutifs par modèle -> cooldown progressif

def on_failure(model_id, status, msg):
    kind = classify_error(status, msg)
    dur = COOLDOWN_DUR.get(kind, 120)
    if kind == 'net':
        # le modèle saoute les grosses requêtes : 90s, puis 300s, 900s, 1800s...
        NET_STREAK[model_id] = NET_STREAK.get(model_id, 0) + 1
        dur = min(1800, dur * (2 ** (NET_STREAK[model_id] - 1)))
    COOLDOWN[model_id] = (time.time() + dur, f'{kind}: {(msg or "")[:60]}')
    log(f'COOLDOWN {model_id} {dur}s ({kind} x{NET_STREAK.get(model_id, 0)}) {str(msg)[:70]}')
    with LOCK:
        STATE['failovers'] += 1
    cost_log({'ts': time.strftime('%Y-%m-%d %H:%M:%S'), 'type': 'failover', 'model': model_id,
              'kind': kind, 'status': status, 'error': (msg or '')[:150]})
    return kind

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _json(self, code, obj, extra_headers=None):
        data = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == '/health':
            self._json(200, {'ok': True, 'cycles': STATE.get('cycles'), 'updated': STATE.get('updated'),
                             'requests_served': STATE.get('requests_served'), 'failovers': STATE.get('failovers'),
                             'cooldowns': {m: r for m, (u, r) in COOLDOWN.items() if u > time.time()}})
        elif self.path == '/state':
            with MKT_LOCK:
                mkts = {m: e['r'] for m, e in MKT.items()}
            with LOCK:
                st = {'models': json.loads(json.dumps(STATE['models'], default=str)),
                      'cooldowns': {m: {'until': time.strftime('%H:%M:%S', time.localtime(u)), 'reason': r}
                                    for m, (u, r) in COOLDOWN.items() if u > time.time()},
                      'updated': STATE.get('updated'), 'requests_served': STATE.get('requests_served'),
                      'failovers': STATE.get('failovers')}
            self._json(200, st)
        elif self.path == '/v1/models':
            self._json(200, {'object': 'list', 'data': [
                {'id': 'auto', 'object': 'model', 'created': 1626777600,
                 'owned_by': 'a6-router: meilleur prix valide en temps reel'}] + [
                {'id': m, 'object': 'model', 'created': 1626777600, 'owned_by': 'a6-router'} for m in MODELS]})
        else:
            self._json(404, {'error': {'message': 'not found'}})

    def do_POST(self):
        if not self.path.startswith('/v1/chat/completions'):
            self._json(404, {'error': {'message': 'not found'}})
            return
        try:
            n = int(self.headers.get('Content-Length', 0))
            payload = json.loads(self.rfile.read(n).decode('utf-8'))
        except Exception as e:
            self._json(400, {'error': {'message': f'bad request: {e}'}})
            return
        requested = payload.get('model', 'auto')
        candidates, scored = live_candidates(requested)
        if not candidates:
            self._json(503, {'error': {'message': 'aucun modele disponible (cooldowns ou marche inaccessible)'}})
            return
        last_err = None
        for model_id in candidates[:4]:
            t_start = time.time()
            resp, err, is_stream, _dt = forward(model_id, payload, CFG['first_byte_timeout_s'])
            ttft = None
            pre = b''
            if is_stream and resp is not None:
                # TTFT = 1er chunk SSE contenant du contenu (ignorer meta/raisonnement)
                try:
                    while ttft is None:
                        chunk = resp.read(512)
                        if not chunk:
                            break
                        pre += chunk
                        if b'"content"' in chunk:
                            ttft = time.time() - t_start
                except Exception:
                    pass
            if err:
                status, msg = err
                last_err = (status, msg)
                on_failure(model_id, status, msg)
                continue
            # succès
            with LOCK:
                STATE['requests_served'] += 1
                # EWMA du TTFT réel pour ce modèle (alpha 0.3) — utilisé dans le scoring
                if ttft:
                    pe = STATE['models'].setdefault(model_id, {})
                    prev = pe.get('ttft_ewma') or ttft
                    pe['ttft_ewma'] = round(0.7 * prev + 0.3 * ttft, 2)
            if is_stream:
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.send_header('X-A6-Router-Model', model_id)
                self.send_header('Cache-Control', 'no-cache')
                self.end_headers()
                try:
                    if pre:
                        self.wfile.write(pre)
                        self.wfile.flush()
                    while True:
                        chunk = resp.read(1024)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                        self.wfile.flush()
                except Exception:
                    pass
                try:
                    resp.close()
                except Exception:
                    pass
                cost_log({'ts': time.strftime('%Y-%m-%d %H:%M:%S'), 'type': 'request', 'model': model_id,
                          'stream': True, 'requested': requested, 'ttft_s': round(ttft, 2) if ttft else None})
                return
            try:
                d = json.loads(resp.read().decode())
            except Exception:
                on_failure(model_id, -1, 'réponse non-JSON')
                continue
            try:
                resp.close()
            except Exception:
                pass
            u = d.get('usage', {})
            tin = u.get('prompt_tokens') or u.get('input_tokens') or 0
            tout = u.get('completion_tokens') or u.get('output_tokens') or 0
            b = (market_get(model_id) or {}).get('best') or {}
            est = None
            if b:
                est = round(tin * b.get('in', 0) / 1e6 + tout * b.get('out', 0) / 1e6, 8)
            cost_log({'ts': time.strftime('%Y-%m-%d %H:%M:%S'), 'type': 'request', 'model': model_id,
                      'tin': tin, 'tout': tout, 'est_cost_usd': est, 'supplier': b.get('supplier'),
                      'requested': requested})
            d['a6_router'] = {'chosen_model': model_id, 'est_cost_usd': est,
                              'supplier': b.get('supplier'), 'requested': requested}
            self._json(200, d, {'X-A6-Router-Model': model_id})
            return
        self._json(502, {'error': {'message': f'tous les candidats ont echoue; derniere erreur: {last_err}'}})

class ExclusiveServer(ThreadingHTTPServer):
    # Windows : SO_REUSEADDR autorise le double-bind -> l'anti-doublon doit être un bind EXCLUSIF
    allow_reuse_address = False
    allow_reuse_port = False

def main():
    port = CFG.get('port', 8791)
    if '--port' in sys.argv:
        port = int(sys.argv[sys.argv.index('--port') + 1])
    # garde anti-doublon : si une instance tourne déjà, on quitte proprement
    try:
        with urllib.request.urlopen(f'http://127.0.0.1:{port}/health', timeout=2) as r:
            if json.loads(r.read().decode()).get('ok'):
                log('instance déjà active sur ce port — arrêt (anti-doublon).')
                return
    except Exception:
        pass
    # écrit le pid pour l'orchestration
    try:
        with open(os.path.join(DIR, 'router.pid'), 'w', encoding='utf-8') as f:
            f.write(str(os.getpid()))
    except Exception:
        pass
    log('préchargement marché en parallèle...')
    warm_market()
    threading.Thread(target=probe_loop, daemon=True).start()
    try:
        srv = ExclusiveServer(('127.0.0.1', port), Handler)
    except OSError:
        log('port %d déjà pris (bind exclusif refusé) — arrêt (anti-doublon).' % port)
        return
    log(f'A6-Router v2 sur http://127.0.0.1:{port} | modeles: {MODELS}')
    log(f"prix marché rafraîchis toutes les ~{CFG['market_ttl_s']}s (par requête), sondes toutes les {CFG['probe_interval_s']}s")
    log('base_url Hermes: http://127.0.0.1:%d/v1  |  model: auto' % port)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        log('arret')

if __name__ == '__main__':
    main()
