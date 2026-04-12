import re, uuid, json, time, logging, threading
from collections import deque
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ══════════════════════════════════════════
#  CONFIG — modifiez ici
# ══════════════════════════════════════════
BOT_TOKEN      = "8701514042:AAFJCcwvmhoi_aSlwD0Pn0i45BomEm-bgd0"
ADMIN_ID       = 8792747145
CHANNEL_1_ID   = "@ppssurf"
CHANNEL_1_NAME = "KAMER-SURF CHANNEL"
CHANNEL_1_URL  = "https://t.me/ppssurf"
CHANNEL_2_ID   = "@ppsrun"
CHANNEL_2_NAME = "PPS RUN"
CHANNEL_2_URL  = "https://t.me/ppsrun"
_REGION   = "us-central1"
_PREFIX   = "pps-tech"
_WS_PATH  = "/ppsrun/"
_CONN_TAG = "PPS-TECH"
_MAX_Q    = 15
_IMG      = "ghcr.io/xtls/xray-core:latest"
BRAND     = "PPS_TECH"

# ══════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("PPSCLOUD")

# ══════════════════════════════════════════
#  ÉTAT GLOBAL
# ══════════════════════════════════════════
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")
job_queue      = deque()
processing     = False
proc_lock      = threading.Lock()
svc_counter    = 1
users_db       = {}
user_sessions  = {}

cfg = {
    "wait_msg": (
        "✅ *Lien reçu avec succès !*\n\n"
        "⚙️ Nos systèmes traitent votre demande.\n"
        "Cela peut prendre quelques minutes.\n\n"
        "📡 *PPS\_TECH* est sur le coup 🚀"
    ),
    "welcome_msg": (
        "🌟 *Bienvenue sur PPS\_TECH Bot !*\n\n"
        "{role} — Bonjour {name} 👋\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📌 *Fonctionnement :*\n\n"
        "1️⃣ Démarrez un lab Skills Google\n"
        "2️⃣ Cliquez sur *Open Google Console*\n"
        "3️⃣ Copiez le lien\n"
        "4️⃣ Envoyez-le ici ✅\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🏷️ *PPS\_TECH* 🇨🇲"
    ),
    "help_content": [],
    "extra_buttons": [],
    "timeout_sec": 600,
}

_PROGRESS = [
    "🔄 Initialisation...", "🔐 Vérification des accès...",
    "📡 Connexion à l'environnement...", "🏗️ Configuration en cours...",
    "⚡ Déploiement sur l'infrastructure...", "🌐 Attribution de l'adresse...",
    "🔗 Construction du lien...", "✅ Finalisation...",
    "⏳ Presque terminé...", "🎯 Dernière vérification...",
]

# ══════════════════════════════════════════
#  UTILITAIRES
# ══════════════════════════════════════════
def esc(text):
    return str(text).replace("_", "\\_")

def register_user(user):
    uid = user.id
    if uid not in users_db:
        users_db[uid] = {
            "username":   user.username or "N/A",
            "first_name": user.first_name or "N/A",
            "joined_at":  datetime.now().strftime("%Y-%m-%d %H:%M"),
            "uses": 0,
        }
    else:
        users_db[uid]["username"]   = user.username or "N/A"
        users_db[uid]["first_name"] = user.first_name or "N/A"

def is_admin(uid): return uid == ADMIN_ID

def is_subscribed(uid):
    missing = []
    for ch_id, ch_name in [(CHANNEL_1_ID, CHANNEL_1_NAME), (CHANNEL_2_ID, CHANNEL_2_NAME)]:
        try:
            m = bot.get_chat_member(ch_id, uid)
            if m.status in ("left", "kicked", "banned"):
                missing.append(ch_name)
        except Exception:
            missing.append(ch_name)
    return (len(missing) == 0, missing)

def progress_bar(step, total=6):
    filled = round((step / total) * 10)
    return f"`[{'█'*filled}{'░'*(10-filled)}] {round((step/total)*100)}%`"

def next_svc():
    global svc_counter
    name = f"{_PREFIX}-{svc_counter:04d}"
    svc_counter += 1
    return name

def _make_conn_link(host_url, uid):
    host = host_url.replace("https://","").replace("http://","").rstrip("/")
    path_enc = _WS_PATH.replace("/", "%2F")
    return (f"vless://{uid}@{host}:443?path={path_enc}"
            f"&security=tls&encryption=none&host={host}&type=ws&sni={host}#{_CONN_TAG}")

def _build_xray_cfg(uid):
    return {
        "log": {"loglevel": "warning"},
        "inbounds": [{
            "port": 8080, "protocol": "vless",
            "settings": {"clients": [{"id": uid, "level": 0}], "decryption": "none"},
            "streamSettings": {"network": "ws", "wsSettings": {"path": _WS_PATH}},
        }],
        "outbounds": [{"protocol": "freedom", "tag": "direct"}],
    }

# ══════════════════════════════════════════
#  CLAVIERS
# ══════════════════════════════════════════
def join_kb():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton(f"📡 {CHANNEL_1_NAME}", url=CHANNEL_1_URL),
        InlineKeyboardButton(f"🎯 {CHANNEL_2_NAME}", url=CHANNEL_2_URL),
        InlineKeyboardButton("✅ J'ai rejoint", callback_data="check_sub"),
    )
    return kb

def cancel_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("❌ Annuler", callback_data="cancel"))
    return kb

def main_menu_kb(uid):
    kb = InlineKeyboardMarkup(row_width=2)
    for b in [x for x in cfg["extra_buttons"] if x.get("position") == "top"]:
        kb.add(InlineKeyboardButton(b["label"], url=b["url"]))
    kb.add(
        InlineKeyboardButton("📖 Aide",        callback_data="help"),
        InlineKeyboardButton("📊 Ma position", callback_data="position"),
    )
    if is_admin(uid):
        kb.add(
            InlineKeyboardButton("👥 Utilisateurs",  callback_data="admin_users"),
            InlineKeyboardButton("⚙️ Configuration", callback_data="admin_config"),
        )
        kb.add(
            InlineKeyboardButton("📢 Diffusion", callback_data="admin_broadcast"),
            InlineKeyboardButton("💬 Messages",  callback_data="admin_messages"),
        )
    else:
        kb.add(InlineKeyboardButton("💬 Contacter PPS\_TECH", callback_data="contact"))
    for b in [x for x in cfg["extra_buttons"] if x.get("position") != "top"]:
        kb.add(InlineKeyboardButton(b["label"], url=b["url"]))
    return kb

def config_kb():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("✏️ Modifier message d'attente",  callback_data="cfg_wait"),
        InlineKeyboardButton("✏️ Modifier message d'accueil",  callback_data="cfg_welcome"),
        InlineKeyboardButton("➕ Ajouter bouton",              callback_data="cfg_add_btn"),
        InlineKeyboardButton("🗑️ Supprimer boutons",           callback_data="cfg_del_btn"),
        InlineKeyboardButton("🔙 Retour",                      callback_data="cfg_back"),
    )
    return kb

# ══════════════════════════════════════════
#  EXTRACTION CREDENTIALS
# ══════════════════════════════════════════
def _extract_from_url(url):
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        for key in ['fallback','relay','continue','next','redirect']:
            encoded = params.get(key, [''])[0]
            if encoded:
                decoded = unquote(encoded)
                em = re.search(r"[\w.+-]+@(?:qwiklabs\.net|cloudskillsboost\.goog)", decoded, re.I)
                pm = re.search(r'project[=:]([a-z][a-z0-9\-]{4,28})', decoded)
                if em or pm:
                    return {"email": em.group(0) if em else None,
                            "project_id": pm.group(1) if pm else None}
        pm = re.search(r'project[=:]([a-z][a-z0-9\-]{4,28})', url)
        if pm:
            return {"project_id": pm.group(1)}
    except Exception as e:
        log.warning(f"URL parse: {e}")
    return {}

def _extract_credentials(page):
    result = {}
    try:
        page.wait_for_load_state("networkidle", timeout=20000)
    except Exception:
        pass
    body = page.inner_text("body")
    html = page.content()
    full = body + "\n" + html
    for pat in [r"[\w.+-]+@qwiklabs\.net",
                r"[\w.+-]+@cloudskillsboost\.goog",
                r"student-[\w]+@[^\s<\"']+"]:
        m = re.search(pat, full, re.I)
        if m:
            result["email"] = m.group(0).strip()
            break
    for pat in [r"(?:Password|Mot de passe)\s*[:\-=|]\s*([A-Za-z0-9!@#$%^&*\-_]{6,40})",
                r"(?:password|passwd)[\"']?\s*[:\-=|>]\s*[\"']?([A-Za-z0-9!@#$%^&*\-_]{6,40})",
                r"data-password=[\"']([A-Za-z0-9!@#$%^&*\-_]{8,40})[\"']"]:
        m = re.search(pat, full, re.I)
        if m:
            result["password"] = m.group(1).strip()
            break
    for pat in [r"(?:Project ID|project[_\s\-]?id)\s*[:\-=|]\s*([a-z][a-z0-9\-]{4,28})",
                r'"projectId"\s*:\s*"([a-z][a-z0-9\-]{4,28})"',
                r"project[=:]([a-z][a-z0-9\-]{4,28})"]:
        m = re.search(pat, full, re.I)
        if m:
            result["project_id"] = m.group(1).strip()
            break
    log.info(f"Creds => email:{'OK' if result.get('email') else 'X'} "
             f"pwd:{'OK' if result.get('password') else 'X'} "
             f"proj:{'OK' if result.get('project_id') else 'X'}")
    return result

# ══════════════════════════════════════════
#  HELPER fetch — UN SEUL ARGUMENT (dict)
#  C'est le correctif principal de l'erreur
#  evaluate() takes 2 to 3 arguments but 4 given
# ══════════════════════════════════════════
def _fetch(page, url, method="GET", token=None, body=None, use_cookies=False):
    """
    Effectue un fetch depuis le contexte du navigateur.
    Tous les paramètres sont passés dans UN SEUL dict Python
    pour respecter la limite Playwright : page.evaluate(script, arg).
    """
    body_json = json.dumps(body) if body else "null"

    if use_cookies:
        script = """(p) => {
            const opts = { method: p.method, credentials: 'include' };
            if (p.body !== null) {
                opts.headers = { 'Content-Type': 'application/json' };
                opts.body = p.body;
            }
            return fetch(p.url, opts).then(r => r.json()).catch(e => ({ __error: e.toString() }));
        }"""
        arg = {"url": url, "method": method, "body": body_json}
    else:
        script = """(p) => {
            const opts = { method: p.method };
            if (p.token) opts.headers = { 'Authorization': 'Bearer ' + p.token };
            if (p.body !== null) {
                opts.headers = Object.assign(opts.headers || {}, { 'Content-Type': 'application/json' });
                opts.body = p.body;
            }
            return fetch(p.url, opts).then(r => r.json()).catch(e => ({ __error: e.toString() }));
        }"""
        arg = {"url": url, "method": method, "token": token or "", "body": body_json}

    return page.evaluate(script, arg)


def _fetch_no_json(page, url, method="POST", token=None, body=None, use_cookies=False):
    """
    Fetch sans attendre de JSON en retour (pour activation API, IAM, etc.)
    Retourne le status HTTP.
    """
    body_json = json.dumps(body) if body else "null"

    if use_cookies:
        script = """(p) => {
            const opts = { method: p.method, credentials: 'include' };
            if (p.body !== null) {
                opts.headers = { 'Content-Type': 'application/json' };
                opts.body = p.body;
            }
            return fetch(p.url, opts).then(r => r.status).catch(() => 0);
        }"""
        arg = {"url": url, "method": method, "body": body_json}
    else:
        script = """(p) => {
            const opts = { method: p.method };
            if (p.token) opts.headers = { 'Authorization': 'Bearer ' + p.token };
            if (p.body !== null) {
                opts.headers = Object.assign(opts.headers || {}, { 'Content-Type': 'application/json' });
                opts.body = p.body;
            }
            return fetch(p.url, opts).then(r => r.status).catch(() => 0);
        }"""
        arg = {"url": url, "method": method, "token": token or "", "body": body_json}

    return page.evaluate(script, arg)

# ══════════════════════════════════════════
#  TOKEN OAUTH — 3 MÉTHODES
# ══════════════════════════════════════════
def _get_token(page, proj_id):
    # Méthode 1 : script bootstrap embarqué dans la page
    token = page.evaluate("""() => {
        try {
            for (const s of document.querySelectorAll('script')) {
                const m = s.textContent.match(/["']?access_token["']?\\s*[=:]\\s*["']?(ya29\\.[A-Za-z0-9_\\-]+)/);
                if (m) return m[1];
            }
            // Chercher aussi dans window.__cloudConsoleBootstrap
            const boot = window.__cloudConsoleBootstrap || window.__INITIAL_STATE__ || {};
            const str = JSON.stringify(boot);
            const m2 = str.match(/ya29\\.[A-Za-z0-9_\\-]{20,}/);
            if (m2) return m2[0];
        } catch(e) {}
        return null;
    }""")
    if token:
        log.info("Token: script bootstrap")
        return token

    # Méthode 2 : interception réseau
    holder = {}

    def on_response(resp):
        if holder.get("t"):
            return
        try:
            auth = resp.request.headers.get("authorization", "")
            m = re.search(r"Bearer (ya29\.[A-Za-z0-9_\-]+)", auth)
            if m and "googleapis.com" in resp.url:
                holder["t"] = m.group(1)
        except Exception:
            pass

    try:
        page.on("response", on_response)
        # Naviguer vers Cloud Run pour déclencher les appels API
        page.goto(
            f"https://console.cloud.google.com/run?project={proj_id}",
            wait_until="domcontentloaded", timeout=30000
        )
        page.wait_for_timeout(10000)
        page.remove_listener("response", on_response)
        if holder.get("t"):
            log.info("Token: interception réseau")
            return holder["t"]
    except Exception as e:
        log.warning(f"Token méthode 2: {e}")
        try:
            page.remove_listener("response", on_response)
        except Exception:
            pass

    # Méthode 3 : cookie session (credentials: include)
    # On teste avec un GET simple sur l'API Cloud Run
    test_url = (f"https://{_REGION}-run.googleapis.com/apis/serving.knative.dev/v1"
                f"/namespaces/{proj_id}/services")
    status = page.evaluate(
        """(u) => fetch(u, {credentials: 'include'}).then(r => r.status).catch(() => 0)""",
        test_url
    )
    if status and status < 500:
        log.info(f"Token: cookie session (status={status})")
        return "__COOKIES__"

    raise RuntimeError(
        "Impossible d'obtenir le token d'accès Google.\n"
        "Assurez-vous d'utiliser le lien *Open Google Console* avec le lab bien démarré."
    )

# ══════════════════════════════════════════
#  LOGIN GOOGLE
# ══════════════════════════════════════════
def _google_login(page, email, password):
    try:
        page.goto("https://accounts.google.com/signin/v2/identifier",
                  wait_until="load", timeout=30000)
        page.wait_for_timeout(2000)

        inp = page.wait_for_selector(
            "input[type=email],input[name=identifier]",
            state="visible", timeout=15000
        )
        inp.fill(email)
        page.keyboard.press("Enter")
        page.wait_for_timeout(3000)

        # Sélection de compte si liste affichée
        try:
            ab = page.query_selector(f"[data-identifier='{email}']")
            if ab:
                ab.click()
                page.wait_for_timeout(2000)
        except Exception:
            pass

        pwd = page.wait_for_selector(
            "input[type=password],input[name=password]",
            state="visible", timeout=15000
        )
        page.wait_for_timeout(500)
        pwd.fill(password)
        page.wait_for_timeout(500)
        page.keyboard.press("Enter")
        page.wait_for_timeout(8000)

        # Bouton "Continuer" / "Accept" éventuel
        for sel in ["#submit", "button[type=submit]", "[jsname='LgbsSe']",
                    "button:has-text('Continue')", "button:has-text('Accept')"]:
            try:
                btn = page.query_selector(sel)
                if btn and btn.is_visible():
                    btn.click()
                    page.wait_for_timeout(3000)
                    break
            except Exception:
                pass

        return True
    except Exception as e:
        log.warning(f"Login: {e}")
        return False

# ══════════════════════════════════════════
#  DÉPLOIEMENT
# ══════════════════════════════════════════
def _deploy(url, svc_name, on_step, cancelled):
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    info = _extract_from_url(url)

    with sync_playwright() as p:
        on_step(1)
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--window-size=1280,900",
            ]
        )
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        # Masquer webdriver
        ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        )
        page = ctx.new_page()

        try:
            if cancelled[0]:
                raise RuntimeError("Annulé")

            on_step(2)
            # Charger le lien SSO Qwiklabs
            try:
                page.goto(url, wait_until="load", timeout=90000)
            except PWTimeout:
                pass
            page.wait_for_timeout(5000)

            if cancelled[0]:
                raise RuntimeError("Annulé")

            # Extraire les credentials depuis la page
            creds = _extract_credentials(page)
            if not creds.get("email") and info.get("email"):
                creds["email"] = info["email"]
            if not creds.get("project_id") and info.get("project_id"):
                creds["project_id"] = info["project_id"]

            email    = creds.get("email")
            password = creds.get("password")
            proj_id  = creds.get("project_id")

            # Dernier recours : chercher project_id dans l'URL actuelle
            if not proj_id:
                m = re.search(r'project[=:]([a-z][a-z0-9\-]{4,28})', page.url)
                if m:
                    proj_id = m.group(1)

            if not proj_id:
                raise RuntimeError(
                    "Project ID introuvable.\n"
                    "Utilisez le lien *Open Google Console* avec le lab démarré."
                )

            log.info(f"proj={proj_id} email={email}")

            # Login si credentials disponibles
            if email and password:
                on_step(3)
                _google_login(page, email, password)
            elif email:
                log.warning("Password non trouvé — on tente sans re-login")

            if cancelled[0]:
                raise RuntimeError("Annulé")

            on_step(4)
            # Navigation console pour déclencher l'authentification
            try:
                page.goto(
                    f"https://console.cloud.google.com/run?project={proj_id}",
                    wait_until="domcontentloaded", timeout=60000
                )
            except PWTimeout:
                pass
            page.wait_for_timeout(10000)

            if cancelled[0]:
                raise RuntimeError("Annulé")

            on_step(5)
            token = _get_token(page, proj_id)
            use_cookies = (token == "__COOKIES__")

            # ── Activation des APIs nécessaires ──
            for api in ["run.googleapis.com", "containerregistry.googleapis.com"]:
                try:
                    enable_url = (
                        f"https://serviceusage.googleapis.com/v1/projects"
                        f"/{proj_id}/services/{api}:enable"
                    )
                    _fetch_no_json(
                        page, enable_url, method="POST",
                        token=token if not use_cookies else None,
                        body={},
                        use_cookies=use_cookies
                    )
                    log.info(f"API {api} activation envoyée")
                except Exception as e:
                    log.warning(f"API {api}: {e}")

            page.wait_for_timeout(15000)

            if cancelled[0]:
                raise RuntimeError("Annulé")

            # ── Construction du service Xray ──
            uid_conn = str(uuid.uuid4())
            xray_cfg = _build_xray_cfg(uid_conn)
            svc_def = {
                "apiVersion": "serving.knative.dev/v1",
                "kind": "Service",
                "metadata": {"name": svc_name, "namespace": proj_id},
                "spec": {"template": {
                    "metadata": {"annotations": {
                        "autoscaling.knative.dev/maxScale": "1",
                        "run.googleapis.com/startup-cpu-boost": "true",
                    }},
                    "spec": {
                        "containerConcurrency": 250,
                        "timeoutSeconds": 3600,
                        "containers": [{
                            "image": _IMG,
                            "ports": [{"containerPort": 8080}],
                            "resources": {"limits": {"memory": "256Mi", "cpu": "1000m"}},
                            "env": [{"name": "XCFG", "value": json.dumps(xray_cfg)}],
                            "command": ["sh", "-c"],
                            "args": [
                                "echo $XCFG>/etc/xray/config.json"
                                "&&xray run -config /etc/xray/config.json"
                            ],
                        }],
                    },
                }},
            }

            deploy_url = (
                f"https://{_REGION}-run.googleapis.com/apis/serving.knative.dev/v1"
                f"/namespaces/{proj_id}/services"
            )

            # ── Déploiement ──
            resp = _fetch(
                page, deploy_url, method="POST",
                token=token if not use_cookies else None,
                body=svc_def,
                use_cookies=use_cookies
            )

            if resp is None:
                raise RuntimeError("Aucune réponse du serveur Cloud Run.")

            if "__error" in resp:
                raise RuntimeError(f"Erreur réseau: {resp['__error']}")

            if "error" in resp:
                code = resp["error"].get("code", "?")
                msg  = resp["error"].get("message", "Erreur inconnue")
                raise RuntimeError(f"Déploiement refusé ({code}): {msg}")

            # ── Polling jusqu'à ce que le service soit prêt ──
            on_step(6)
            svc_url = None
            status_url = (
                f"https://{_REGION}-run.googleapis.com/apis/serving.knative.dev/v1"
                f"/namespaces/{proj_id}/services/{svc_name}"
            )

            for attempt in range(60):
                if cancelled[0]:
                    raise RuntimeError("Annulé")
                page.wait_for_timeout(7000)
                try:
                    st = _fetch(
                        page, status_url, method="GET",
                        token=token if not use_cookies else None,
                        use_cookies=use_cookies
                    )
                    if st and "__error" not in st:
                        u = st.get("status", {}).get("url")
                        if u:
                            svc_url = u
                            break
                        # Vérifier si le déploiement a explicitement échoué
                        for c in st.get("status", {}).get("conditions", []):
                            if c.get("type") == "Ready" and c.get("status") == "False":
                                raise RuntimeError(
                                    f"Échec déploiement: {c.get('message', '?')}"
                                )
                except RuntimeError:
                    raise
                except Exception as e:
                    log.warning(f"Poll {attempt+1}: {e}")
                log.info(f"Attente service ({attempt+1}/60)...")

            if not svc_url:
                raise RuntimeError(
                    "Timeout: service non démarré après 7 minutes.\n"
                    "Le projet Qwiklabs est peut-être suspendu ou l'image inaccessible."
                )

            # ── Rendre le service public (allUsers) ──
            try:
                iam_url = (
                    f"https://{_REGION}-run.googleapis.com/v1/projects"
                    f"/{proj_id}/locations/{_REGION}/services/{svc_name}:setIamPolicy"
                )
                iam_body = {
                    "policy": {
                        "bindings": [{
                            "role": "roles/run.invoker",
                            "members": ["allUsers"]
                        }]
                    }
                }
                _fetch_no_json(
                    page, iam_url, method="POST",
                    token=token if not use_cookies else None,
                    body=iam_body,
                    use_cookies=use_cookies
                )
                log.info("IAM public OK")
            except Exception as e:
                log.warning(f"IAM: {e}")

            return {
                "host": svc_url,
                "link": _make_conn_link(svc_url, uid_conn),
                "ref":  svc_name,
            }

        finally:
            try:
                browser.close()
            except Exception:
                pass

# ══════════════════════════════════════════
#  FILE D'ATTENTE
# ══════════════════════════════════════════
def process_queue():
    global processing
    with proc_lock:
        if processing:
            return
        processing = True
    try:
        while job_queue:
            job       = job_queue[0]
            chat_id   = job["chat_id"]
            svc       = job["svc"]
            url       = job["url"]
            uname     = job.get("username", "Anonyme")
            cancelled = job["cancelled"]

            sent   = bot.send_message(chat_id, cfg["wait_msg"], reply_markup=cancel_kb())
            msg_id = sent.message_id
            info   = {"n": 0, "tick": 0}
            stop   = [False]

            def ticker(chat_id=chat_id, msg_id=msg_id, info=info,
                       stop=stop, cancelled=cancelled):
                while not stop[0] and not cancelled[0]:
                    time.sleep(5)
                    if stop[0] or cancelled[0]:
                        break
                    info["tick"] += 1
                    txt = _PROGRESS[info["tick"] % len(_PROGRESS)]
                    bar = progress_bar(min(info["n"], 6))
                    try:
                        bot.edit_message_text(
                            f"⚡ *PPS\_TECH* — Traitement\n\n{bar}\n\n{txt}",
                            chat_id, msg_id,
                            parse_mode="Markdown", reply_markup=cancel_kb()
                        )
                    except Exception:
                        pass

            threading.Thread(target=ticker, daemon=True).start()

            def on_step(n, info=info):
                info["n"] = n

            try:
                result = _deploy(url, svc, on_step, cancelled)
                stop[0] = True
                time.sleep(0.5)

                if cancelled[0]:
                    try:
                        bot.edit_message_text(
                            "❌ *Opération annulée*",
                            chat_id, msg_id, parse_mode="Markdown"
                        )
                    except Exception:
                        pass
                else:
                    try:
                        bot.edit_message_text(
                            f"✅ *Terminé !*\n\n{progress_bar(6)}\n\n🎯 *PPS\_TECH* 🇨🇲",
                            chat_id, msg_id, parse_mode="Markdown"
                        )
                    except Exception:
                        pass

                    bot.send_message(
                        chat_id,
                        f"🎉 *Voici votre lien !*\n\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🌐 *Adresse :*\n`{result['host']}`\n\n"
                        f"🔑 *Lien VLESS :*\n`{result['link']}`\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"1️⃣ Copiez le lien\n"
                        f"2️⃣ Ouvrez votre app\n"
                        f"3️⃣ Importez & Connectez 🚀\n\n"
                        f"🏷️ *PPS\_TECH* 🇨🇲",
                        parse_mode="Markdown", reply_markup=main_menu_kb(chat_id)
                    )
                    if chat_id in users_db:
                        users_db[chat_id]["uses"] += 1
                    try:
                        bot.send_message(
                            ADMIN_ID,
                            f"✅ Déployé\n👤 `{uname}`\n"
                            f"🔖 `{svc}`\n🌐 `{result['host']}`",
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass

            except Exception as e:
                stop[0] = True
                log.error(f"Erreur [{svc}]: {e}")
                if not cancelled[0]:
                    try:
                        bot.edit_message_text(
                            f"❌ *Échec*\n\n{str(e)[:400]}\n\n"
                            f"💡 Vérifiez :\n• Lab bien démarré\n"
                            f"• Lien *Open Google Console*\n"
                            f"• Projet non suspendu",
                            chat_id, msg_id,
                            parse_mode="Markdown",
                            reply_markup=main_menu_kb(chat_id)
                        )
                    except Exception:
                        pass
                    try:
                        bot.send_message(
                            ADMIN_ID,
                            f"❌ Échec\n👤 `{uname}`\n❗ `{str(e)[:500]}`",
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass
            finally:
                job_queue.popleft()
                for i, j in enumerate(job_queue):
                    try:
                        bot.send_message(
                            j["chat_id"],
                            f"📊 *File d'attente*\n\n"
                            f"📍 Position : *{i+1}/{len(job_queue)}*",
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass
    finally:
        with proc_lock:
            processing = False

# ══════════════════════════════════════════
#  HANDLER LIEN — accepte les liens PARTOUT
# ══════════════════════════════════════════
def _handle_url(uid, text, first_name, username):
    if not is_admin(uid):
        ok, _ = is_subscribed(uid)
        if not ok:
            bot.send_message(
                uid, "🔒 *Accès restreint*\n\nRejoignez nos canaux.",
                reply_markup=join_kb(), parse_mode="Markdown"
            )
            return

    if any(j["chat_id"] == uid for j in job_queue):
        pos = next(i+1 for i, j in enumerate(job_queue) if j["chat_id"] == uid)
        bot.send_message(
            uid, f"⏳ *Déjà en cours !*\n\n📍 Position : *{pos}*",
            parse_mode="Markdown"
        )
        return

    if len(job_queue) >= _MAX_Q:
        bot.send_message(
            uid, "🚫 *Service surchargé*\n\nRéessayez dans quelques minutes.",
            parse_mode="Markdown"
        )
        return

    user_sessions.pop(uid, None)

    svc = next_svc()
    cancelled = [False]
    job_queue.append({
        "chat_id":   uid,
        "username":  first_name or username or "Anonyme",
        "url":       text,
        "svc":       svc,
        "cancelled": cancelled,
    })
    pos = len(job_queue)
    if pos == 1:
        bot.send_message(
            uid, "✅ *Lien reçu !*\n\n⚡ Traitement immédiat...",
            parse_mode="Markdown"
        )
    else:
        bot.send_message(
            uid, f"✅ *Lien reçu !*\n\n📍 Position : *{pos}*",
            parse_mode="Markdown"
        )
    threading.Thread(target=process_queue, daemon=True).start()

# ══════════════════════════════════════════
#  HANDLERS MESSAGES
# ══════════════════════════════════════════
@bot.message_handler(commands=["start"])
def cmd_start(msg):
    register_user(msg.from_user)
    uid  = msg.from_user.id
    name = msg.from_user.first_name or "Utilisateur"
    if not is_admin(uid):
        ok, missing = is_subscribed(uid)
        if not ok:
            bot.send_message(
                uid,
                f"🔒 *Accès restreint — PPS\_TECH*\n\n"
                f"Bonjour {name} 👋\n\n"
                f"Rejoignez nos canaux officiels 📡\n\n"
                f"❌ Manquants :\n" + "\n".join(f"• {ch}" for ch in missing),
                reply_markup=join_kb(), parse_mode="Markdown"
            )
            return
    role = "👑 Administrateur" if is_admin(uid) else "👤 Utilisateur"
    text = cfg["welcome_msg"].format(role=role, name=name)
    bot.send_message(uid, text, reply_markup=main_menu_kb(uid), parse_mode="Markdown")


@bot.message_handler(
    func=lambda m: m.text and (
        "http://" in m.text or "https://" in m.text or "www." in m.text
    )
)
def handle_link(msg):
    register_user(msg.from_user)
    _handle_url(
        msg.from_user.id, msg.text.strip(),
        msg.from_user.first_name, msg.from_user.username
    )


@bot.message_handler(func=lambda m: m.from_user.id in user_sessions)
def handle_session_msg(msg):
    uid     = msg.from_user.id
    session = user_sessions.get(uid, {})
    state   = session.get("state")

    # Lien prioritaire sur toute session
    if msg.text and ("http://" in msg.text or "https://" in msg.text):
        _handle_url(uid, msg.text.strip(),
                    msg.from_user.first_name, msg.from_user.username)
        return

    if state == "contact":
        user_sessions.pop(uid, None)
        name = msg.from_user.first_name or msg.from_user.username or "Anonyme"
        try:
            bot.send_message(
                ADMIN_ID, f"💬 *Message de {name}*\n\n{msg.text}",
                parse_mode="Markdown"
            )
            bot.send_message(
                uid, "✅ *Message envoyé à PPS\_TECH !*",
                parse_mode="Markdown", reply_markup=main_menu_kb(uid)
            )
        except Exception:
            bot.send_message(uid, "❌ Erreur d'envoi.", parse_mode="Markdown")

    elif state == "cfg_wait":
        user_sessions.pop(uid, None)
        cfg["wait_msg"] = msg.text
        bot.send_message(
            uid, "✅ *Message d'attente mis à jour !*",
            parse_mode="Markdown", reply_markup=main_menu_kb(uid)
        )

    elif state == "cfg_welcome":
        user_sessions.pop(uid, None)
        cfg["welcome_msg"] = msg.text
        bot.send_message(
            uid, "✅ *Message d'accueil mis à jour !*",
            parse_mode="Markdown", reply_markup=main_menu_kb(uid)
        )

    elif state == "cfg_add_btn":
        user_sessions.pop(uid, None)
        try:
            parts = msg.text.strip().split("|")
            label = parts[0].strip()
            url   = parts[1].strip()
            pos   = parts[2].strip() if len(parts) > 2 else "bottom"
            cfg["extra_buttons"].append({"label": label, "url": url, "position": pos})
            bot.send_message(
                uid, f"✅ *Bouton ajouté :* {label}",
                parse_mode="Markdown", reply_markup=main_menu_kb(uid)
            )
        except Exception:
            bot.send_message(
                uid,
                "❌ Format invalide.\nUtilisez : `Nom | https://lien.com | top`",
                parse_mode="Markdown"
            )

    elif state == "cfg_broadcast":
        user_sessions.pop(uid, None)
        count = 0
        for u in users_db:
            try:
                bot.send_message(u, msg.text, parse_mode="Markdown")
                count += 1
            except Exception:
                pass
        bot.send_message(
            uid, f"📢 *Diffusé à {count} utilisateurs*",
            parse_mode="Markdown", reply_markup=main_menu_kb(uid)
        )

# ══════════════════════════════════════════
#  CALLBACKS
# ══════════════════════════════════════════
@bot.callback_query_handler(func=lambda c: c.data == "check_sub")
def cb_check_sub(call):
    uid = call.from_user.id
    ok, _ = is_subscribed(uid)
    if ok:
        bot.answer_callback_query(call.id, "✅ Accès accordé !")
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        call.message.from_user = call.from_user
        cmd_start(call.message)
    else:
        bot.answer_callback_query(
            call.id, "❌ Pas encore abonné aux deux canaux.", show_alert=True
        )


@bot.callback_query_handler(func=lambda c: c.data == "cancel")
def cb_cancel(call):
    uid = call.from_user.id
    for j in list(job_queue):
        if j["chat_id"] == uid:
            j["cancelled"][0] = True
            break
    user_sessions.pop(uid, None)
    bot.answer_callback_query(call.id, "❌ Annulé")
    try:
        bot.edit_message_text(
            "❌ *Opération annulée*",
            call.message.chat.id, call.message.message_id,
            parse_mode="Markdown"
        )
    except Exception:
        pass
    bot.send_message(uid, "Que voulez-vous faire ?", reply_markup=main_menu_kb(uid))


@bot.callback_query_handler(func=lambda c: c.data == "help")
def cb_help(call):
    uid = call.from_user.id
    if not cfg["help_content"]:
        bot.send_message(
            uid, "📖 *Aide — PPS\_TECH*\n\nAucun contenu disponible.",
            parse_mode="Markdown"
        )
    else:
        for item in cfg["help_content"]:
            try:
                t = item.get("type")
                if t == "text":
                    bot.send_message(uid, item["content"], parse_mode="Markdown")
                elif t == "video":
                    bot.send_video(uid, item["file_id"], caption=item.get("caption", ""))
                elif t == "photo":
                    bot.send_photo(uid, item["file_id"], caption=item.get("caption", ""))
                elif t == "document":
                    bot.send_document(uid, item["file_id"], caption=item.get("caption", ""))
            except Exception:
                pass


@bot.callback_query_handler(func=lambda c: c.data == "position")
def cb_position(call):
    uid = call.from_user.id
    pos = next((i+1 for i, j in enumerate(job_queue) if j["chat_id"] == uid), None)
    if pos:
        bot.send_message(
            uid, f"📊 *Position : {pos}/{len(job_queue)}*", parse_mode="Markdown"
        )
    else:
        bot.send_message(
            uid, "📭 *Vous n'avez pas de demande en cours.*", parse_mode="Markdown"
        )


@bot.callback_query_handler(func=lambda c: c.data == "contact")
def cb_contact(call):
    uid = call.from_user.id
    user_sessions[uid] = {"state": "contact"}
    bot.send_message(
        uid, "💬 *Contacter PPS\_TECH*\n\nÉcrivez votre message :",
        parse_mode="Markdown", reply_markup=cancel_kb()
    )


# ── Admin callbacks ──
@bot.callback_query_handler(
    func=lambda c: c.data == "admin_users" and is_admin(c.from_user.id)
)
def cb_users(call):
    if not users_db:
        bot.send_message(
            ADMIN_ID, "📭 *Aucun utilisateur enregistré.*", parse_mode="Markdown"
        )
        return
    lines = [f"👥 *Utilisateurs — {len(users_db)}*\n"]
    for i, (uid, info) in enumerate(users_db.items(), 1):
        lines.append(
            f"{i}. `{uid}` | {info['username']} | {info['first_name']} | 🔁{info['uses']}x"
        )
    full = "\n".join(lines)
    for chunk in [full[i:i+3800] for i in range(0, len(full), 3800)]:
        bot.send_message(ADMIN_ID, chunk, parse_mode="Markdown")


@bot.callback_query_handler(
    func=lambda c: c.data == "admin_config" and is_admin(c.from_user.id)
)
def cb_config(call):
    bot.send_message(
        ADMIN_ID,
        "⚙️ *Configuration PPS\_TECH*\n\nChoisissez ce que vous voulez modifier :",
        parse_mode="Markdown", reply_markup=config_kb()
    )


@bot.callback_query_handler(
    func=lambda c: c.data == "cfg_wait" and is_admin(c.from_user.id)
)
def cb_cfg_wait(call):
    user_sessions[ADMIN_ID] = {"state": "cfg_wait"}
    bot.send_message(
        ADMIN_ID,
        f"✏️ *Message d'attente actuel :*\n\n{cfg['wait_msg']}\n\nEnvoyez le nouveau texte :",
        parse_mode="Markdown", reply_markup=cancel_kb()
    )


@bot.callback_query_handler(
    func=lambda c: c.data == "cfg_welcome" and is_admin(c.from_user.id)
)
def cb_cfg_welcome(call):
    user_sessions[ADMIN_ID] = {"state": "cfg_welcome"}
    bot.send_message(
        ADMIN_ID,
        "✏️ Envoyez le nouveau message d'accueil.\n"
        "Utilisez `{role}` et `{name}` comme variables :",
        parse_mode="Markdown", reply_markup=cancel_kb()
    )


@bot.callback_query_handler(
    func=lambda c: c.data == "cfg_add_btn" and is_admin(c.from_user.id)
)
def cb_cfg_add_btn(call):
    user_sessions[ADMIN_ID] = {"state": "cfg_add_btn"}
    bot.send_message(
        ADMIN_ID,
        "➕ *Ajouter un bouton*\n\nFormat : `Nom | https://lien.com | top`\n"
        "Position : `top` ou `bottom`",
        parse_mode="Markdown", reply_markup=cancel_kb()
    )


@bot.callback_query_handler(
    func=lambda c: c.data == "cfg_del_btn" and is_admin(c.from_user.id)
)
def cb_cfg_del_btn(call):
    cfg["extra_buttons"] = []
    bot.send_message(
        ADMIN_ID, "🗑️ *Tous les boutons supprimés.*",
        parse_mode="Markdown", reply_markup=main_menu_kb(ADMIN_ID)
    )


@bot.callback_query_handler(func=lambda c: c.data == "cfg_back")
def cb_cfg_back(call):
    bot.send_message(
        call.from_user.id, "🏠 *Menu principal*",
        parse_mode="Markdown", reply_markup=main_menu_kb(call.from_user.id)
    )


@bot.callback_query_handler(
    func=lambda c: c.data == "admin_broadcast" and is_admin(c.from_user.id)
)
def cb_broadcast(call):
    user_sessions[ADMIN_ID] = {"state": "cfg_broadcast"}
    bot.send_message(
        ADMIN_ID,
        f"📢 *Diffusion*\n\n{len(users_db)} utilisateur(s) enregistré(s).\n\nEnvoyez le message :",
        parse_mode="Markdown", reply_markup=cancel_kb()
    )


@bot.callback_query_handler(
    func=lambda c: c.data == "admin_messages" and is_admin(c.from_user.id)
)
def cb_messages(call):
    bot.send_message(
        ADMIN_ID,
        "💬 *Messages reçus*\n\nLes messages des utilisateurs vous sont transférés directement.",
        parse_mode="Markdown"
    )

# ══════════════════════════════════════════
#  LANCEMENT
# ══════════════════════════════════════════
if __name__ == "__main__":
    print("╔══════════════════════════════════════╗")
    print("║  ⚡  PPS CLOUD RUN BOT  🇨🇲          ║")
    print("╚══════════════════════════════════════╝")
    log.info("Démarrage...")
    bot.infinity_polling(timeout=30, long_polling_timeout=20)
