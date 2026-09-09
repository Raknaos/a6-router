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
import subprocess, atexit, uuid
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
SUCCESS_SCALE = 10000  # l'API A6API renvoie recent_success_rate sur 0-10000 (8500 = 85 %)
MIN_SUCCESS = CFG.get('min_success_rate', 80)      # 80 (0-100) — floor de fiabilité
MIN_SUCCESS_RAW = int(MIN_SUCCESS * SUCCESS_SCALE / 100)  # 8000 (échelle API)

# ── Paramètres v2.1.0 (tous en CFG.get avec défaut safe = comportement v2.0.0) ──
# Hystérésis : ne quitter le canal courant que si un autre est HYSTERESIS_PCT % moins
# cher (coût espéré) — préserve le cache de prompts du canal (cache_read ~0,1x input).
HYSTERESIS_PCT = float(CFG.get('hysteresis_pct', 15)) / 100.0
# Timeout du 1er essai ADAPTATIF : max(first_attempt_min_s, TTFT_ewma du modèle × 2).
# Les audits ont prouvé qu'un 8 s fixe coupait des canaux sains (TTFT réel 7,8-12,6 s).
FIRST_TIMEOUT_MIN = float(CFG.get('first_attempt_min_s', 8))
TTFT_MARGIN = float(CFG.get('ttft_margin_x', 2.0))
# dérive observée A6API : 1 USD ≈ 514 356 quota (audit global 9)
QUOTA_PER_USD = float(CFG.get('quota_per_usd', 514356))

STATE = {'models': {}, 'updated': None, 'cycles': 0, 'requests_served': 0, 'failovers': 0}
MKT = {}                      # model_id -> {'r': ..., 'ts': ...}
MKT_BUSY = set()              # single-flight : modèles en cours de refresh marché
MKT_LOCK = threading.Lock()
COOLDOWN = {}                 # model_id -> (until_ts, reason)
NET_STREAK = {}               # échecs net consécutifs par modèle
USAGE_STATS = {}              # model_id -> {'req', 'errs', 'in_cached', 'in_uncached', 'est_cost_usd'}
PIN = {'model': None, 'since': 0}   # hystérésis : modèle élu en cours (cache chaud)
UPDATE_LOCK = threading.Lock()      # single-flight auto-update (update_loop × /admin)
LOCK = threading.Lock()
LOG_LOCK = threading.Lock()   # dédié aux I/O fichier (ne bloque jamais le chemin requête)

def log(msg):
    """Journal fichier avec rotation (~1 Mo x 3) + stdout si console existe (pythonw: silencieux)."""
    line = time.strftime('%H:%M:%S') + ' ' + str(msg)
    try:
        print(line, flush=True)
    except Exception:
        pass
    try:
        p = os.path.join(DIR, 'router.log')
        with LOG_LOCK:
            if os.path.exists(p) and os.path.getsize(p) > 1_000_000:
                for old in ('.2', '.1'):
                    prev = p + old
                    if os.path.exists(prev):
                        os.replace(prev, p + ('.3' if old == '.2' else '.2'))
                os.replace(p, p + '.1')
            with open(p, 'a', encoding='utf-8') as f:
                f.write(line + '\n')
    except Exception:
        pass

def cost_log(entry):
    try:
        with LOG_LOCK:
            with open(os.path.join(DIR, 'costs.jsonl'), 'a', encoding='utf-8') as f:
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
    except Exception as e:
        try: log(f'cost_log ERR: {str(e)[:60]}')
        except Exception: pass

def save_state():
    try:
        with LOCK:
            snap = json.loads(json.dumps(STATE, default=str))
        tmp = os.path.join(DIR, 'state.json.tmp')
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(snap, f, ensure_ascii=False, indent=1, default=str)
        os.replace(tmp, os.path.join(DIR, 'state.json'))
    except Exception:
        pass

def in_cooldown(model_id):
    until, reason = COOLDOWN.get(model_id, (0, ''))
    if time.time() < until:
        return True, reason
    return False, ''

# ---------------- Marché (gratuit, TTL + refresh en fond) ----------------

def marketplace_best(model_id):
    """Meilleur canal pour un modèle : parmi les canaux vivants >= floor succès,
    tri par COÛT ESPÉRÉ = prix_in / (succès) — un canal à 85 % qui coûte 0.0012
    a un coût espéré de 0.0012/0.85 = 0.0014, comparable au fiable à 0.0036/1.0."""
    url = f'{MKT_BASE}?page=1&page_size=50&model={urllib.parse.quote(model_id)}&sort=price'
    r = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + KEY, 'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(r, timeout=15) as resp:
        d = json.loads(resp.read().decode())
    items = d.get('data', {}).get('items', [])
    alive = [i for i in items if not i.get('supplier_channel_disabled')
             and i.get('listing_availability') == 1
             and (i.get('recent_success_rate') or 0) >= MIN_SUCCESS_RAW
             and (i.get('sample_count') or 0) >= 20]
    # coût espéré d'entrée = prix_in * 100 / succès(%)
    def expected_in(i):
        s = max((i.get('recent_success_rate') or 0) / SUCCESS_SCALE, 0.01)
        return i.get('input_price_micros', 10**18) / s
    alive.sort(key=expected_in)
    if not alive:
        return {'best': None, 'n_alive': 0, 'total': d.get('data', {}).get('total', 0)}
    b = alive[0]
    # liste des 3 premiers canaux pour failover prix si le 1er tombe
    alts = [{'supplier': x.get('supplier_nickname'),
             'in': x.get('input_price_micros', 0) / 1e6,
             'out': x.get('output_price_micros', 0) / 1e6,
             'success': (x.get('recent_success_rate') or 0) / SUCCESS_SCALE * 100,
             'cache_read': x.get('cache_read_price_micros', 0) / 1e6,       # prix lecture cache
             'cache_hit': (x.get('cache_hit_rate_24h') or 0) / 100,          # 0-100
             'p50_ms': x.get('recent_p50_ms')} for x in alive[:3]]
    best = alts[0]
    return {'best': best, 'alts': alts, 'n_alive': len(alive), 'total': d.get('data', {}).get('total', 0)}

def market_get(model_id, max_age_s=None):
    """Retourne immédiatement le cache. Un cache absent ne bloque jamais une requête,
    mais déclenche désormais un refresh single-flight (auto-réparation du cache vide au
    boot — audit global 1 : sinon 503 permanent jusqu'au redémarrage)."""
    max_age = max_age_s if max_age_s is not None else CFG.get('market_ttl_s', 60)
    with MKT_LOCK:
        ent = MKT.get(model_id)
    if ent and (time.time() - ent['ts'] < max_age):
        return ent['r']
    if ent:
        with MKT_LOCK:
            if model_id in MKT_BUSY:
                return ent['r']          # un refresh est déjà en cours — on ne spawn pas en cascade
            MKT_BUSY.add(model_id)
        def _bg():
            try:
                r = marketplace_best(model_id)
                with MKT_LOCK: MKT[model_id] = {'r': r, 'ts': time.time()}
            except Exception: pass
            finally:
                with MKT_LOCK: MKT_BUSY.discard(model_id)
        threading.Thread(target=_bg, daemon=True).start()
        return ent['r']
    # cache ABSENT (boot, panne réseau au warm) : refresh single-flight en fond + auto-repair loop
    with MKT_LOCK:
        if model_id in MKT_BUSY:
            return None
        MKT_BUSY.add(model_id)
    def _bg():
        try:
            r = marketplace_best(model_id)
            with MKT_LOCK: MKT[model_id] = {'r': r, 'ts': time.time()}
        except Exception: pass
        finally:
            with MKT_LOCK: MKT_BUSY.discard(model_id)
    threading.Thread(target=_bg, daemon=True).start()
    return None

def warm_market():
    """Précharge les prix en parallèle en ARRIÈRE-PLAN (thread daemon) :
    le serveur HTTP ne doit jamais attendre le marché au boot.
    Retente tant que le cache est vide (max 5 essais) — sinon 503 permanent."""
    def get(m):
        for attempt in range(5):
            try:
                r = marketplace_best(m)
                with MKT_LOCK: MKT[m] = {'r': r, 'ts': time.time()}
                return
            except Exception as e:
                log(f'MARCHÉ {m} essai {attempt+1}/5: {str(e)[:60]}')
                time.sleep(2 * (attempt + 1))
    def _bg():
        with cf.ThreadPoolExecutor(max_workers=len(MODELS)) as ex:
            list(ex.map(get, MODELS))
    threading.Thread(target=_bg, daemon=True).start()


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
                if ttft is None and b'"content"' in out:
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
        # PROBING DIFFÉRENCIÉ (audit coût) : un canal stable n'a pas besoin de sonde
        # toutes les 10 min — vision-exp est sondée 1×/h, les autres 1×/probe_interval.
        with LOCK:
            pe = STATE['models'].get(model_id, {})
        last_ok = pe.get('last_ok')
        if last_ok and model_id.endswith('-exp'):
            try:
                hh, mm, _ = last_ok.split(':')
                if time.localtime().tm_hour == int(hh) and time.localtime().tm_min < int(mm):
                    continue  # déjà sondé cette heure
            except Exception:
                pass
        mkt = market_get(model_id, max_age_s=5)
        probe = probe_model(model_id)
        with LOCK:
            prev = STATE['models'].get(model_id, {})
        entry = dict(prev)
        entry.update({'probe': probe, 'last_probe': time.strftime('%H:%M:%S')})
        if probe.get('ok'):
            entry['fails'] = 0
            entry['last_ok'] = time.strftime('%H:%M:%S')
            with LOCK:
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
    Prix marché (cache frais) + latence = max(sonde, p50 marché). Cooldowns respectés.
    HYSTÉRÉSIS : le modèle élu courant (PIN) est retenu tant que le meilleur
    concurrent n'est pas HYSTERESIS_PCT % moins cher (coût espéré) — préserve le
    cache de prompts (cache_read ~0,1x input, hit ~90 %) au lieu de tourner."""
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
        probe_lat = probe.get('latency_s') or 0
        if probe_lat > CFG.get('max_latency_s', 12) or b.get('success', 0) < MIN_SUCCESS:
            continue
        # coût ESPÉRÉ = (in+out)/2 / succès : un canal moins fiable paie son prix divisé
        # par sa probabilité de succès (les échecs coûtent du temps, pas des tokens)
        success = max(float(b.get('success', 100) or 100) / 100.0, 0.01)
        cost_expected = (b['in'] + b['out']) / 2 / success
        cand.append((cost_expected, m, b))
    cand.sort(key=lambda x: x[0])
    # ── HYSTÉRÉSIS : garder le canal élu (cache chaud) sauf si clairement battu ──
    if cand:
        with LOCK:
            pinned = PIN.get('model')
        if pinned and not in_cooldown(pinned)[0]:
            pc = next((c for c, m, _ in cand if m == pinned), None)
            if pc is not None:
                best_c = cand[0][0]
                if best_c > 0 and pc <= best_c * (1 + HYSTERESIS_PCT):
                    # le pinned reste dans la marge -> il reprend la tête
                    cand.sort(key=lambda x: 0 if x[1] == pinned else 1)
        # mise à jour du pin : toujours le candidat qui finit en tête
        with LOCK:
            PIN['model'] = cand[0][1]
            PIN['since'] = now
    if requested_model and requested_model != 'auto' and requested_model in MODELS:
        cd, reason = in_cooldown(requested_model)
        rest = [m for _, m, _ in cand if m != requested_model]
        if cd:
            log(f'modele demandé {requested_model} en cooldown ({reason}), candidats de secours')
            return rest, cand
        # même en cooldown fraîchement posé pendant la sélection, on essaie le demandé 1er
        return [requested_model] + rest, cand
    return [m for _, m, _ in cand], cand

# ---------------- Forward + failover ----------------

def normalize_payload(model_id, payload):
    """Adapte la requête au modèle choisi : effort supporté + garde-fous."""
    body = dict(payload)
    body['model'] = model_id
    eff = body.get('reasoning_effort')
    # les modèles économiques n'acceptent pas max/xhigh → forcer low (rapidité + coût)
    if isinstance(eff, str) and eff.lower() in ('max', 'xhigh'):
        body['reasoning_effort'] = 'low'
        body.pop('reasoning', None)
    elif not isinstance(eff, str):
        # champ non-string (int/bool/...) : certains canaux 400 dessus — on le retire
        body.pop('reasoning_effort', None)
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
# NET_STREAK défini en tête de fichier (près de COOLDOWN), partagé par le routeur et l'auto-update

# ── AUTO-UPDATE ────────────────────────────────────────────────────────────
import hashlib, shutil, base64
UPD = CFG.get('update') or {}
UPD_MANIFEST_URL = UPD.get('manifest_url') or 'https://raw.githubusercontent.com/Raknaos/a6-router/main/manifest.json'
UPD_ALLOWED_HOSTS = ('raw.githubusercontent.com', 'github.com', 'objects.githubusercontent.com')
# Clé PUBLIQUE embarquée (Ed25519) : le manifest doit être signé par la clé privée
# détenue UNIQUEMENT par le mainteneur (jamais dans le repo). Un repo compromis ne
# suffit plus à pousser du code : il faudrait aussi la clé privée.
# Rotation de clé = nouvelle clé dans le code + manifest signé AVEC l'ancienne clé.
SIGN_PUB_B64 = 'MCowBQYDK2VwAyEAbfeCEleVYP2cQyOEGVyAbtohos44ydDfaeM6M4iw7PI='

def verify_manifest_sig(manifest):
    """Vérifie la signature Ed25519 du manifest. Retourne (ok, raison).
    Signature = sign(sha256(version|sha256|router_url)) en base64."""
    if not SIGN_PUB_B64:
        return True, 'pas de clé publique embarquée — vérification désactivée'
    try:
        sig = base64.b64decode(manifest.get('signature', ''))
        payload = f"{manifest.get('version','')}|{manifest.get('sha256','')}|{manifest.get('router_url','')}".encode()
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
        from cryptography.hazmat.primitives import serialization
        pem = b'-----BEGIN PUBLIC KEY-----\n' + SIGN_PUB_B64.encode() + b'\n-----END PUBLIC KEY-----'
        pub = serialization.load_pem_public_key(pem)
        pub.verify(sig, payload)
        return True, 'signature valide'
    except Exception as e:
        return False, f'signature invalide: {str(e)[:80]}'

def sign_manifest(manifest_path):
    """Signe le manifest local avec la clé privée du mainteneur (usage build, jamais en prod)."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives import serialization
    key_path = os.environ.get('A6ROUTER_SIGN_KEY') or os.path.join(
        os.path.expanduser('~'), 'AppData', 'Local', 'hermes', 'a6router-sign-key.pem')
    m = json.load(open(manifest_path, encoding='utf-8'))
    payload = f"{m['version']}|{m['sha256']}|{m['router_url']}".encode()
    priv = serialization.load_pem_private_key(open(key_path, 'rb').read(), password=None)
    m['signature'] = base64.b64encode(priv.sign(payload)).decode()
    json.dump(m, open(manifest_path, 'w', encoding='utf-8'), indent=2)
    return m['signature'][:16] + '...'

def read_version():
    try:
        return json.load(open(os.path.join(DIR, 'version.json'), encoding='utf-8'))
    except Exception:
        return {'version': '0.0.0', 'router_sha256': ''}

def read_token():
    try:
        return json.load(open(os.path.join(DIR, 'update_token.json'), encoding='utf-8')).get('token', '')
    except Exception:
        return ''

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()

def http_get(url, timeout=30):
    host = urllib.parse.urlparse(url).netloc
    if host not in UPD_ALLOWED_HOSTS:
        raise ValueError(f'hote non autorise: {host}')
    req = urllib.request.Request(url, headers={'User-Agent': 'a6-router-updater'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def check_update(force=False):
    """Vérifie le manifest, télécharge, vérifie le sha256, remplace puis redémarre.
    Jamais de downgrade (sauf force=1). Jamais d'écriture si le sha256 ne correspond pas.
    Single-flight : update_loop (6 h) et /admin/update ne peuvent pas se croiser
    (audit concurrence : deux check_update croisés = .bak incohérent / exit au milieu)."""
    with UPDATE_LOCK:
        return _check_update_impl(force)

def _check_update_impl(force=False):
    ver = read_version()
    try:
        man = json.loads(http_get(UPD_MANIFEST_URL, timeout=15).decode())
    except Exception as e:
        return {'updated': False, 'reason': f'manifest injoignable: {str(e)[:80]}'}
    ok, why = verify_manifest_sig(man)
    if not ok:
        # manifest non signé ou signature invalide -> on refuse TOUTE écriture
        log(f'UPDATE REFUSÉ: {why}')
        return {'updated': False, 'reason': f'{why} — mise à jour refusée'}
    local, remote = str(ver.get('version', '0.0.0')), str(man.get('version', '0'))
    if not force and remote <= local:
        return {'updated': False, 'reason': f'a jour (local {local} / distant {remote})'}
    try:
        code = http_get(man['router_url'], timeout=30)
    except Exception as e:
        return {'updated': False, 'reason': f'telechargement impossible: {str(e)[:80]}'}
    got = hashlib.sha256(code).hexdigest()
    want = (man.get('sha256') or '').lower()
    if not want or got != want:
        return {'updated': False, 'reason': f'sha256 refuse ({got[:12]}...) — fichier NON modifie'}
    path = os.path.join(DIR, 'router.py')
    try:
        shutil.copy2(path, os.path.join(DIR, 'router.py.bak'))
        shutil.copy2(os.path.join(DIR, 'version.json'), os.path.join(DIR, 'version.json.bak'))
        with open(path + '.new', 'wb') as f:
            f.write(code)
        json.dump({'version': remote, 'router_sha256': got, 'date': man.get('date', ''),
                   'pending': True}, open(os.path.join(DIR, 'version.json'), 'w'), indent=2)
        os.replace(path + '.new', path)
    except Exception as e:
        return {'updated': False, 'reason': f'ecriture impossible: {str(e)[:80]}'}
    log(f'UPDATE {local} -> {remote} (sha {got[:12]}...) — redemarrage supervise')
    cost_log({'ts': time.strftime('%Y-%m-%d %H:%M:%S'), 'type': 'update', 'from': local, 'to': remote,
              'sha256': got[:16]})
    if os.name == 'nt':
        # os._exit ne déclenche PAS atexit : on lance ici le helper de relance (Windows)
        try:
            h = update_helper_path()
            subprocess.Popen([sys.executable, h], creationflags=getattr(subprocess, 'DETACHED_PROCESS', 0))
        except Exception:
            pass  # schtasks relancera en dernier recours
    threading.Timer(2.0, lambda: os._exit(0)).start()
    return {'updated': True, 'from': local, 'to': remote, 'sha256': got[:16],
            'note': 'fichier remplace; redemarrage en cours (systemd/schtasks relance)'}

def boot_commit_or_rollback():
    """Au démarrage : si version.json est 'pending', ce code est un essai.
    On note le moment ; si main() plante, l'appelant restaure router.py.bak."""
    ver = read_version()
    own = sha256_file(os.path.join(DIR, 'router.py'))
    if ver.get('pending') and own != ver.get('router_sha256'):
        # incohérence manifest/code : on refuse de démarrer un code non vérifié
        bak = os.path.join(DIR, 'router.py.bak')
        if os.path.exists(bak):
            shutil.copy2(bak, os.path.join(DIR, 'router.py'))
            shutil.copy2(os.path.join(DIR, 'version.json.bak'), os.path.join(DIR, 'version.json'))
            log('ROLLBACK : sha du nouveau code != manifest, version precedente restauree')
            os._exit(1)
    return ver.get('pending')

def confirm_update():
    """Appelé après un démarrage réussi avec version.json pending -> on valide."""
    ver = read_version()
    ver.pop('pending', None)
    json.dump(ver, open(os.path.join(DIR, 'version.json'), 'w'), indent=2)
    log(f"UPDATE confirme: v{ver.get('version')} (sha {str(ver.get('router_sha256'))[:12]}...)")

def update_loop():
    interval_h = float(UPD.get('interval_h', 6) or 6)
    if not UPD.get('enabled', True):
        return
    while True:
        time.sleep(interval_h * 3600)
        try:
            r = check_update()
            if r.get('updated'):
                log(f"auto-update: {r['from']} -> {r['to']}")
        except Exception as e:
            log(f'auto-update erreur: {str(e)[:80]}')

def update_helper_path():
    # Windows : pas de superviseur réactif (schtasks toutes les 5 min) -> relance dédiée
    if os.name == 'nt':
        helper = os.path.join(DIR, 'update_helper.py')
        with open(helper, 'w', encoding='utf-8') as f:
            f.write('import time, subprocess, sys, os\n'
                    'time.sleep(3)\n'
                    'subprocess.Popen([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "router.py")])\n')
        return helper
    return None  # systemd/schtasks Linux relancent nativement

def on_failure(model_id, status, msg):
    kind = classify_error(status, msg)
    if kind == 'params':
        # erreur de paramètres CLIENT (payload invalide) : le modèle n'est pas en cause
        # -> aucun cooldown, sinon 6 mauvaises requetes d'un client excluent le modele 5 min
        log(f'PARAMS invalides (pas de cooldown modele): {str(msg)[:70]}')
        cost_log({'ts': time.strftime('%Y-%m-%d %H:%M:%S'), 'type': 'params-reject', 'model': model_id,
                  'error': str(msg)[:150]})
        return kind
    with LOCK:
        USAGE_STATS.setdefault(model_id, {'req': 0, 'errs': 0, 'in_cached': 0,
                                          'in_uncached': 0, 'est_cost_usd': 0.0})['errs'] += 1
    dur = COOLDOWN_DUR.get(kind, 120)
    if kind == 'net':
        # le modèle saute les grosses requêtes : backoff progressif 90s, 180s, 360s, 720s, 1440s, 1800s max
        with LOCK:
            NET_STREAK[model_id] = NET_STREAK.get(model_id, 0) + 1
            dur = min(1800, dur * (2 ** (NET_STREAK[model_id] - 1)))
    with LOCK:
        COOLDOWN[model_id] = (time.time() + dur, f'{kind}: {(msg or "")[:60]}')
    with LOCK:
        streak = NET_STREAK.get(model_id, 0)
    log(f'COOLDOWN {model_id} {dur}s ({kind} x{streak}) {str(msg)[:70]}')
    with LOCK:
        STATE['failovers'] += 1
    cost_log({'ts': time.strftime('%Y-%m-%d %H:%M:%S'), 'type': 'failover', 'model': model_id,
              'kind': kind, 'status': status, 'error': (msg or '')[:150]})
    return kind

class Handler(BaseHTTPRequestHandler):
    server_version = 'A6Router'   # ne pas révéler BaseHTTP/Python dans les headers
    sys_version = ''

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
            with LOCK:
                cds = {m: r for m, (u, r) in dict(COOLDOWN).items() if u > time.time()}
            self._json(200, {'ok': True, 'version': read_version().get('version'),
                             'cycles': STATE.get('cycles'), 'updated': STATE.get('updated'),
                             'requests_served': STATE.get('requests_served'), 'failovers': STATE.get('failovers'),
                             'cooldowns': cds})
        elif self.path == '/state':
            with MKT_LOCK:
                mkts = {m: e['r'] for m, e in MKT.items()}
            with LOCK:
                cds = {m: {'until': time.strftime('%H:%M:%S', time.localtime(u)), 'reason': r}
                       for m, (u, r) in dict(COOLDOWN).items() if u > time.time()}
                st = {'version': read_version().get('version'),
                      'models': json.loads(json.dumps(STATE['models'], default=str)),
                      'usage_stats': json.loads(json.dumps(USAGE_STATS, default=str)),
                      'pin': dict(PIN),
                      'cooldowns': cds,
                      'updated': STATE.get('updated'), 'requests_served': STATE.get('requests_served'),
                      'failovers': STATE.get('failovers')}
            self._json(200, st)
        elif self.path == '/cache':
            # rapport lecture seule : où le cache de prompts est utilisé, gain estimé
            with LOCK:
                stats = {m: dict(v) for m, v in USAGE_STATS.items()}
            rows = []
            tot_cached = tot_uncached = 0
            gain = 0.0
            for m, v in stats.items():
                b = ((market_get(m) or {}).get('best') or {})
                cr = b.get('cache_read') or 0
                pi = b.get('in') or 0
                g = v.get('in_cached', 0) * (pi - cr) / 1e6
                gain += g
                tot_cached += v.get('in_cached', 0)
                tot_uncached += v.get('in_uncached', 0)
                rows.append({'model': m, 'supplier': b.get('supplier'),
                             'in_cached_tokens': v.get('in_cached', 0),
                             'in_uncached_tokens': v.get('in_uncached', 0),
                             'hit_pct': round(100 * v.get('in_cached', 0) /
                                              max(v.get('in_cached', 0) + v.get('in_uncached', 0), 1), 1),
                             'gain_usd': round(g, 6)})
            self._json(200, {'models': rows, 'total_cached_tokens': tot_cached,
                             'total_uncached_tokens': tot_uncached,
                             'hit_pct_total': round(100 * tot_cached / max(tot_cached + tot_uncached, 1), 1),
                             'gain_usd_cumule': round(gain, 6)})
        elif self.path == '/v1/models':
            self._json(200, {'object': 'list', 'data': [
                {'id': 'auto', 'object': 'model', 'created': 1626777600,
                 'owned_by': 'a6-router: meilleur prix valide en temps reel'}] + [
                {'id': m, 'object': 'model', 'created': 1626777600, 'owned_by': 'a6-router'} for m in MODELS]})
        elif self.path.startswith('/admin/update'):
            # mise a jour sur demande : /admin/update?token=... (force=1 accepte un downgrade)
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            tok = (qs.get('token') or [''])[0]
            want = read_token()
            if not want or tok != want:
                self._json(403, {'error': {'message': 'token invalide'}})
                return
            r = check_update(force=(qs.get('force') or ['0'])[0] == '1')
            self._json(200, r)
        else:
            self._json(404, {'error': {'message': 'not found'}})

    def do_POST(self):
        if not self.path.startswith('/v1/chat/completions'):
            self._json(404, {'error': {'message': 'not found'}})
            return
        try:
            n = int(self.headers.get('Content-Length', 0))
            if n > 10_000_000:  # plafond 10 Mo — 413 au lieu d'un read mémoire illimité
                self._json(413, {'error': {'message': 'payload trop volumineux (max 10 Mo)'}})
                return
            payload = json.loads(self.rfile.read(n).decode('utf-8'))
        except Exception as e:
            self._json(400, {'error': {'message': f'bad request: {e}'}})
            return
        requested = payload.get('model', 'auto')
        # Compat OpenAI : un modèle inconnu (ni 'auto' ni dans MODELS) -> 404 clair,
        # pas un 200 silencieux routé vers un autre modèle (casse les SDK)
        if requested != 'auto' and requested not in MODELS:
            self._json(404, {'error': {'message': f"modele '{requested}' inconnu du routeur — "
                                                  f"utilisez 'auto' ou un de: {', '.join(MODELS)}",
                                       'type': 'invalid_request_error', 'code': 'model_not_found'}})
            return
        # validation locale des limites de tokens : 400 SANS appel upstream (ni cooldown)
        for tk in ('max_tokens', 'max_completion_tokens'):
            v = payload.get(tk)
            if v is not None and (not isinstance(v, int) or isinstance(v, bool) or v <= 0):
                self._json(400, {'error': {'message': f'{tk} doit être un entier positif',
                                           'type': 'invalid_request_error', 'code': 'invalid_max_tokens'}})
                return
        candidates, scored = live_candidates(requested)
        if not candidates:
            self._json(503, {'error': {'message': 'aucun modele disponible (cooldowns ou marche inaccessible)',
                                       'type': 'server_error', 'code': 'no_model_available'}})
            return
        req_id = uuid.uuid4().hex[:12]
        last_err = None
        for i, model_id in enumerate(candidates[:4]):
            # TIMEOUT ADAPTATIF (audits perf/fiabilité) : le 1er essai a un budget de
            # max(first_attempt_min_s, TTFT_ewma du modèle × 2) — un 8 s fixe coupait
            # des canaux SAINS (TTFT réel mesuré 7,8-12,6 s sur qwen) -> faux failovers.
            with LOCK:
                ewma = (STATE['models'].get(model_id, {}) or {}).get('ttft_ewma') or 0
            if i == 0:
                timeout = max(FIRST_TIMEOUT_MIN, (ewma or 0) * TTFT_MARGIN)
            else:
                timeout = CFG.get('first_byte_timeout_s', 25)
            t_start = time.time()
            resp, err, is_stream, _dt = forward(model_id, payload, timeout)
            ttft = None
            pre = b''
            if is_stream and resp is not None:
                # TTFT = 1er chunk SSE contenant du contenu (ignorer meta/raisonnement)
                try:
                    while ttft is None and time.time() - t_start < timeout:
                        chunk = resp.read(512)
                        if not chunk:
                            break
                        pre += chunk
                        # tester sur pre+chunk : le motif peut être coupé entre deux read
                        if b'"content"' in pre:
                            ttft = time.time() - t_start
                except Exception as e:
                    err = err or (-1, f'lecture stream: {str(e)[:80]}')
            if err:
                status, msg = err
                last_err = (status, msg)
                log(f'REQ {req_id} essai {i+1} {model_id}: {status} {str(msg)[:80]}')
                kind = on_failure(model_id, status, msg)
                if kind == 'params':
                    # payload du client refusé par le canal -> pas la faute du modèle,
                    # inutile d'essayer les autres : réponse 400 immédiate
                    self._json(400, {'error': {'message': f'parametres invalides: {str(msg)[:120]}',
                                               'type': 'invalid_request_error', 'code': status}})
                    return
                continue
            if is_stream and ttft is None:
                # P0 : 200 "réussi" mais AUCUN chunk de contenu (EOF ou timeout avant contenu)
                # -> réponse vide : on bascule sur le candidat suivant, on ne sert PAS du vide
                last_err = (-1, f'{model_id}: 200 sans contenu (EOF/timeout avant 1er token)')
                log(f'REQ {req_id} essai {i+1} {model_id}: stream vide')
                on_failure(model_id, -1, '200 sans contenu (stream vide)')
                continue
            # ── SUCCÈS ──
            log(f'REQ {req_id} servi par {model_id} (essai {i+1}/{len(candidates[:4])}, ttft {round(ttft,2) if ttft else "?"}s)')
            with LOCK:
                STATE['requests_served'] += 1
                us = USAGE_STATS.setdefault(model_id, {'req': 0, 'errs': 0, 'in_cached': 0,
                                                       'in_uncached': 0, 'est_cost_usd': 0.0})
                us['req'] += 1
                # EWMA du TTFT réel pour ce modèle (alpha 0.3)
                if ttft:
                    pe = STATE['models'].setdefault(model_id, {})
                    prev = pe.get('ttft_ewma') or ttft
                    pe['ttft_ewma'] = round(0.7 * prev + 0.3 * ttft, 2)
            if is_stream:
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.send_header('X-A6-Router-Model', model_id)
                self.send_header('X-A6-Request-Id', req_id)
                self.send_header('Cache-Control', 'no-cache')
                self.end_headers()
                # P0 (audits obs/perf) : parser le bloc usage du FINAL des SSE —
                # 96-99 % des tokens passent en stream et n'étaient JAMAIS logués
                # (coût réel par requête inconnu). La sonde le fait déjà : le canal
                # émet bien un dernier chunk usage même en stream.
                usage = None
                try:
                    if pre:
                        self.wfile.write(pre)
                        self.wfile.flush()
                    buf = b''
                    done = False
                    while not done:
                        chunk = resp.read(1024)
                        if not chunk:
                            break
                        buf += chunk
                        if b'data: [DONE]' in buf:
                            done = True
                        self.wfile.write(chunk)
                        self.wfile.flush()
                        if len(buf) > 256 * 1024:
                            buf = buf[-65536:]  # fenêtre glissante : le usage est à la fin
                    # parser les SSE complets du buffer final
                    try:
                        sse = buf.decode(errors='replace')
                        for line in reversed(sse.split('\n')):
                            line = line.strip()
                            if line.startswith('data:') and line != 'data: [DONE]' and '"usage"' in line:
                                j = json.loads(line[5:].strip())
                                if j.get('usage'):
                                    usage = j['usage']
                                    break
                    except Exception:
                        pass
                except Exception as e:
                    # RUPTURE MI-STREAM (audit fiabilité : mort APRÈS le 1er token —
                    # jusqu'ici avalée en silence, réponse tronquée servie, 0 cooldown)
                    log(f'REQ {req_id} rupture mi-stream {model_id}: {str(e)[:80]}')
                    cost_log({'ts': time.strftime('%Y-%m-%d %H:%M:%S'), 'type': 'stream-break',
                              'req_id': req_id, 'model': model_id, 'error': str(e)[:120]})
                    on_failure(model_id, -1, f'rupture mi-stream: {str(e)[:80]}')
                try:
                    resp.close()
                except Exception:
                    pass
                tin = tout = tin_cached = None
                est = None
                if usage:
                    d2 = usage.get('prompt_tokens_details') or {}
                    tin = usage.get('prompt_tokens') or usage.get('input_tokens') or 0
                    tout = usage.get('completion_tokens') or usage.get('output_tokens') or 0
                    tin_cached = d2.get('cached_tokens') or 0
                    b = (market_get(model_id) or {}).get('best') or {}
                    if b:
                        cr = b.get('cache_read') or 0
                        # coût réel : tokens cachés au prix cache, le reste au prix plein
                        est = round(((tin - tin_cached) * b.get('in', 0) / 1e6
                                     + tin_cached * cr / 1e6
                                     + tout * b.get('out', 0) / 1e6), 8)
                        with LOCK:
                            us['in_cached'] += tin_cached
                            us['in_uncached'] += (tin - tin_cached)
                            us['est_cost_usd'] = round(us['est_cost_usd'] + (est or 0), 8)
                cost_log({'ts': time.strftime('%Y-%m-%d %H:%M:%S'), 'type': 'request',
                          'req_id': req_id, 'model': model_id, 'stream': True,
                          'tin': tin, 'tout': tout, 'tin_cached': tin_cached,
                          'est_cost_usd': est, 'ttft_s': round(ttft, 2) if ttft else None,
                          'requested': requested})
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
            pd = u.get('prompt_tokens_details') or {}
            tin = u.get('prompt_tokens') or u.get('input_tokens') or 0
            tout = u.get('completion_tokens') or u.get('output_tokens') or 0
            tin_cached = pd.get('cached_tokens') or 0
            b = (market_get(model_id) or {}).get('best') or {}
            est = None
            if b:
                cr = b.get('cache_read') or 0
                est = round(((tin - tin_cached) * b.get('in', 0) / 1e6
                             + tin_cached * cr / 1e6
                             + tout * b.get('out', 0) / 1e6), 8)
                with LOCK:
                    us['in_cached'] += tin_cached
                    us['in_uncached'] += (tin - tin_cached)
                    us['est_cost_usd'] = round(us['est_cost_usd'] + (est or 0), 8)
            cost_log({'ts': time.strftime('%Y-%m-%d %H:%M:%S'), 'type': 'request',
                      'req_id': req_id, 'model': model_id, 'stream': False,
                      'tin': tin, 'tout': tout, 'tin_cached': tin_cached,
                      'est_cost_usd': est, 'supplier': b.get('supplier'),
                      'requested': requested})
            d['a6_router'] = {'chosen_model': model_id, 'est_cost_usd': est,
                              'supplier': b.get('supplier'), 'requested': requested,
                              'req_id': req_id}
            self._json(200, d, {'X-A6-Router-Model': model_id, 'X-A6-Request-Id': req_id})
            return
        # tous les candidats ont échoué
        log(f'REQ {req_id} ÉCHOUÉE: {last_err}')
        cost_log({'ts': time.strftime('%Y-%m-%d %H:%M:%S'), 'type': 'exhausted',
                  'req_id': req_id, 'requested': requested, 'error': str(last_err)[:150]})
        self._json(502, {'error': {'message': f'tous les candidats ont echoue; derniere erreur: {last_err}',
                                   'type': 'server_error', 'code': 'all_candidates_failed'}})

class ExclusiveServer(ThreadingHTTPServer):
    # Windows : SO_REUSEADDR autorise le double-bind -> l'anti-doublon doit être un bind EXCLUSIF
    allow_reuse_address = False
    allow_reuse_port = False
    daemon_threads = True
    # audit concurrence : ThreadingHTTPServer non borné = saturation par rafale de
    # connexions (chaque requête = un thread). Proxy local : 64 suffisent largement.
    max_concurrent = 64

    def process_request(self, request, client_address):
        # refus propre au-delà du cap (au lieu d'un thread illimité)
        active = getattr(self, '_active_count', 0)
        if active >= self.max_concurrent:
            try:
                request.sendall(b'HTTP/1.1 503 Service Unavailable\r\n'
                                b'Content-Type: application/json\r\nContent-Length: 58\r\n\r\n'
                                b'{"error":{"message":"routeur surcharge (max 64 concurrent)"}}\r\n')
            except Exception:
                pass
            try:
                request.close()
            except Exception:
                pass
            return
        self._active_count = active + 1
        try:
            super().process_request(request, client_address)
        finally:
            self._active_count = max(0, self._active_count - 1)

def main():
    port = CFG.get('port', 8791)
    bind_host = '127.0.0.1'
    if '--port' in sys.argv:
        port = int(sys.argv[sys.argv.index('--port') + 1])
    # garde-fou sécurité : le routeur dépense la clé A6API — hors loopback interdit SAUF --expose
    if '--expose' in sys.argv:
        bind_host = '0.0.0.0'
    elif any(a.startswith('--bind=') for a in sys.argv):
        bind_host = sys.argv[[i for i, a in enumerate(sys.argv) if a.startswith('--bind=')][0]].split('=', 1)[1]
    elif os.environ.get('A6ROUTER_BIND') == '0.0.0.0':
        bind_host = '0.0.0.0'
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
    log('préchargement marché en ARRIÈRE-PLAN (serveur démarré immédiatement)...')
    warm_market()
    threading.Thread(target=probe_loop, daemon=True).start()
    threading.Thread(target=update_loop, daemon=True).start()
    # essai post-update : si pending, la version sera confirmée après un serveur vivant
    if boot_commit_or_rollback():
        threading.Timer(30.0, confirm_update).start()
    try:
        srv = ExclusiveServer((bind_host, port), Handler)
    except OSError:
        log('port %d déjà pris (bind exclusif refusé) — arrêt (anti-doublon).' % port)
        return
    log(f'A6-Router v{(read_version().get("version") or "?")} sur http://{bind_host}:{port} | modeles: {MODELS}')
    log(f"prix marché rafraîchis toutes les ~{CFG['market_ttl_s']}s (par requête), sondes toutes les {CFG['probe_interval_s']}s")
    log('base_url Hermes: http://127.0.0.1:%d/v1  |  model: auto' % port)
    if os.name == 'nt':
        # relance sans superviseur réactif : un helper détaché redémarre après os._exit
        h = update_helper_path()
        atexit.register(lambda: subprocess.Popen([sys.executable, h],
                       creationflags=getattr(subprocess, 'DETACHED_PROCESS', 0)))
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        log('arret')

if __name__ == '__main__':
    main()
