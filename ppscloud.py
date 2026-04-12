# ================================================================
#  PPS CLOUD RUN BOT — Version corrigée
#  Fichier : ppscloud.py
# ================================================================

import os, re, uuid, json, time, logging, threading
from collections import deque
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ════════════════════════════════════════════════════════════
#  CONFIG
# ════════════════════════════════════════════════════════════
BOT_TOKEN  = "8701514042:AAFJCcwvmhoi_aSlwD0Pn0i45BomEm-bgd0"
ADMIN_ID   = 8792747145

CHANNEL_1_ID   = "@ppssurf"
CHANNEL_1_NAME = "KAMER-SURF CHANNEL"
CHANNEL_1_URL  = "https://t.me/ppssurf"

CHANNEL_2_ID   = "@ppsrun"
CHANNEL_2_NAME = "PPS RUN"
CHANNEL_2_URL  = "https://t.me/ppsrun"

_REGION    = "us-central1"
_PREFIX    = "pps-tech"
_WS_PATH   = "/ppsrun/"
_CONN_TAG  = "PPS-TECH"
_MAX_Q     = 15
_IMG       = "ghcr.io/xtls/xray-core:latest"

BRAND = "PPS_TECH"

# ════════════════════════════════════════════════════════════
#  LOGGING
# ════════════════════════════════════════════════════════════
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("PPS_CLOUD")

# ════════════════════════════════════════════════════════════
#  ÉTAT GLOBAL
# ════════════════════════════════════════════════════════════
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")
job_queue = deque()
processing = False
processing_lock = threading.Lock()
svc_counter = 1
users_db = {}
user_sessions = {}

bot_config = {
    "wait_message": (
        "✅ *Lien reçu avec succès !*\n\n"
        "⚙️ Nos systèmes traitent votre demande.\n"
        "Cela peut prendre quelques minutes.\n\n"
        "📡 *PPS_TECH* est sur le coup 🚀"
    ),
    "help_content": [],
    "extra_buttons": [],
}

_PROGRESS = [
    "🔄 Initialisation...",
    "🔐 Vérification des accès...",
    "📡 Connexion à l'environnement...",
    "🏗️ Configuration en cours...",
    "⚡ Déploiement sur l'infrastructure...",
    "🌐 Attribution de l'adresse...",
    "🔗 Construction du lien...",
    "✅ Finalisation...",
    "⏳ Presque terminé...",
    "🎯 Dernière vérification...",
]

# ════════════════════════════════════════════════════════════
#  UTILITAIRES
# ════════════════════════════════════════════════════════════

def register_user(user):
    uid = user.id
    if uid not in users_db:
        users_db[uid] = {
            "username": user.username or "N/A",
            "first_name": user.first_name or "N/A",
            "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "uses": 0
        }
    else:
        users_db[uid]["username"] = user.username or "N/A"
        users_db[uid]["first_name"] = user.first_name or "N/A"

def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

def is_subscribed(uid: int) -> tuple:
    missing = []
    for ch_id, ch_name in [(CHANNEL_1_ID, CHANNEL_1_NAME), (CHANNEL_2_ID, CHANNEL_2_NAME)]:
        try:
            m = bot.get_chat_member(ch_id, uid)
            if m.status in ("left", "kicked", "banned"):
                missing.append(ch_name)
        except Exception:
            missing.append(ch_name)
    return (len(missing) == 0, missing)

def join_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton(f"📡 {CHANNEL_1_NAME}", url=CHANNEL_1_URL),
        InlineKeyboardButton(f"🎯 {CHANNEL_2_NAME}", url=CHANNEL_2_URL),
        InlineKeyboardButton("✅ J'ai rejoint", callback_data="check_sub")
    )
    return kb

def cancel_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("❌ Annuler", callback_data="cancel"))
    return kb

def main_menu_kb(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    for b in [b for b in bot_config["extra_buttons"] if b.get("position") == "top"]:
        kb.add(InlineKeyboardButton(b["label"], url=b["url"]))
    kb.add(
        InlineKeyboardButton("📖 Aide", callback_data="help"),
        InlineKeyboardButton("📊 Ma position", callback_data="position"),
    )
    if is_admin(uid):
        kb.add(
            InlineKeyboardButton("👥 Utilisateurs", callback_data="users"),
            InlineKeyboardButton("⚙️ Configuration", callback_data="config"),
        )
        kb.add(
            InlineKeyboardButton("📢 Diffusion", callback_data="broadcast"),
            InlineKeyboardButton("💬 Messages", callback_data="messages"),
        )
    else:
        kb.add(InlineKeyboardButton("💬 Contacter PPS_TECH", callback_data="contact"))
    for b in [b for b in bot_config["extra_buttons"] if b.get("position") != "top"]:
        kb.add(InlineKeyboardButton(b["label"], url=b["url"]))
    return kb

def progress_bar(step: int, total: int = 6) -> str:
    filled = round((step / total) * 10)
    bar = "█" * filled + "░" * (10 - filled)
    pct = round((step / total) * 100)
    return f"`[{bar}] {pct}%`"

def next_svc() -> str:
    global svc_counter
    name = f"{_PREFIX}-{svc_counter:04d}"
    svc_counter += 1
    return name

def _make_conn_link(host_url: str, uid: str) -> str:
    host = host_url.replace("https://", "").replace("http://", "").rstrip("/")
    path_enc = _WS_PATH.replace("/", "%2F")
    return (
        f"vless://{uid}@{host}:443"
        f"?path={path_enc}"
        f"&security=tls"
        f"&encryption=none"
        f"&host={host}"
        f"&type=ws"
        f"&sni={host}"
        f"#{_CONN_TAG}"
    )

def _build_cfg(uid: str) -> dict:
    return {
        "log": {"loglevel": "warning"},
        "inbounds": [{
            "port": 8080,
            "protocol": "vless",
            "settings": {"clients": [{"id": uid, "level": 0}], "decryption": "none"},
            "streamSettings": {"network": "ws", "wsSettings": {"path": _WS_PATH}}
        }],
        "outbounds": [{"protocol": "freedom", "tag": "direct"}]
    }

# ════════════════════════════════════════════════════════════
#  EXTRACTION ROBUSTE DES CREDENTIALS
# ════════════════════════════════════════════════════════════

def _extract_from_url(url: str) -> dict:
    """Parser l'URL pour extraire email/project directement"""
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        for key in ['fallback', 'relay', 'continue', 'next', 'redirect']:
            encoded = params.get(key, [''])[0]
            if encoded:
                decoded = unquote(encoded)
                email_m = re.search(r"[\w.+-]+@(?:qwiklabs\.net|cloudskillsboost\.goog)", decoded, re.I)
                proj_m  = re.search(r'project[=:]([a-z][a-z0-9\-]{4,28})', decoded)
                if email_m or proj_m:
                    return {
                        "email":      email_m.group(0) if email_m else None,
                        "project_id": proj_m.group(1)  if proj_m  else None,
                        "method":     "url_parse"
                    }

        proj_m = re.search(r'project[=:]([a-z][a-z0-9\-]{4,28})', url)
        if proj_m:
            return {"project_id": proj_m.group(1), "method": "url_direct"}
    except Exception as e:
        log.warning(f"URL parse failed: {e}")
    return {}


def _extract_credentials(page) -> dict:
    """Extraction multi-méthode depuis la page Qwiklabs"""
    result = {}
    try:
        page.wait_for_load_state("networkidle", timeout=20_000)
    except Exception:
        pass

    body = page.inner_text("body")
    html = page.content()
    full = body + "\n" + html

    # Email
    for pattern in [
        r"[\w.+-]+@qwiklabs\.net",
        r"[\w.+-]+@cloudskillsboost\.goog",
        r"student-[\w]+@[^\s<\"']+",
    ]:
        m = re.search(pattern, full, re.I)
        if m:
            result["email"] = m.group(0).strip()
            break

    # Password — plusieurs formats Qwiklabs
    for pattern in [
        r"(?:Password|Mot de passe)\s*[:\-=\|]\s*([A-Za-z0-9!@#$%^&*\-_]{6,40})",
        r"(?:password|passwd)[\"']?\s*[:\-=\|>]\s*[\"']?([A-Za-z0-9!@#$%^&*\-_]{6,40})",
        r"(?:data-password|value)[\"']=[\"']([A-Za-z0-9!@#$%^&*\-_]{8,40})[\"']",
    ]:
        m = re.search(pattern, full, re.I)
        if m:
            result["password"] = m.group(1).strip()
            break

    # Project ID
    for pattern in [
        r"(?:Project ID|project[_\s\-]?id)\s*[:\-=\|]\s*([a-z][a-z0-9\-]{4,28})",
        r'"projectId"\s*:\s*"([a-z][a-z0-9\-]{4,28})"',
        r"project[=:]([a-z][a-z0-9\-]{4,28})",
    ]:
        m = re.search(pattern, full, re.I)
        if m:
            result["project_id"] = m.group(1).strip()
            break

    log.info(f"Credentials extraits : email={'OK' if result.get('email') else 'MANQUANT'} | "
             f"password={'OK' if result.get('password') else 'MANQUANT'} | "
             f"project={'OK' if result.get('project_id') else 'MANQUANT'}")
    return result


# ════════════════════════════════════════════════════════════
#  RÉCUPÉRATION DU TOKEN OAUTH — MÉTHODES MULTIPLES
# ════════════════════════════════════════════════════════════

def _get_oauth_token(page, proj_id: str) -> str:
    """
    Récupère le token OAuth via 3 méthodes différentes.
    Plus de dépendance à l'endpoint RPC interne cassé.
    """

    # ── Méthode 1 : fetch() avec credentials:include (cookies de session) ──
    token = page.evaluate("""async (proj) => {
        try {
            const r = await fetch(
                'https://cloudresourcemanager.googleapis.com/v1/projects/' + proj,
                { credentials: 'include', headers: { 'x-origin': 'https://console.cloud.google.com' } }
            );
            // Si la requête passe, la session est valide — extraire le token des cookies
            const cookieStr = document.cookie;
            const m = cookieStr.match(/GCLB_BEARER=([A-Za-z0-9._\\-]+)/);
            if (m) return m[1];
        } catch(e) {}
        return null;
    }""", proj_id)
    if token:
        log.info("Token obtenu via méthode 1 (cookie GCLB_BEARER)")
        return token

    # ── Méthode 2 : Cloud Console interne __BOOTSTRAP__ ──
    token = page.evaluate("""() => {
        try {
            // Google injecte le token dans la variable globale de la console
            const scripts = Array.from(document.querySelectorAll('script'));
            for (const s of scripts) {
                const m = s.textContent.match(/["']?access_token["']?\\s*[=:]\\s*["']?(ya29\\.[A-Za-z0-9_\\-]+)/);
                if (m) return m[1];
            }
        } catch(e) {}
        return null;
    }""")
    if token:
        log.info("Token obtenu via méthode 2 (script bootstrap)")
        return token

    # ── Méthode 3 : Intercepter un appel API depuis la console ──
    try:
        token_holder = {}

        def intercept(response):
            if "googleapis.com" in response.url and not token_holder.get("token"):
                auth = response.request.headers.get("authorization", "")
                m = re.search(r"Bearer (ya29\.[A-Za-z0-9_\-]+)", auth)
                if m:
                    token_holder["token"] = m.group(1)

        page.on("response", intercept)

        # Déclencher un appel API en naviguant sur Cloud Run
        page.goto(f"https://console.cloud.google.com/run?project={proj_id}",
                  wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(8_000)
        page.remove_listener("response", intercept)

        if token_holder.get("token"):
            log.info("Token obtenu via méthode 3 (interception réseau)")
            return token_holder["token"]
    except Exception as e:
        log.warning(f"Méthode 3 échouée: {e}")

    # ── Méthode 4 : Appel direct API via fetch avec cookies ──
    token = page.evaluate("""async (proj) => {
        try {
            // Tenter un appel direct à l'API Run pour voir si la session cookie suffit
            const test = await fetch(
                `https://${arguments[1]}-run.googleapis.com/apis/serving.knative.dev/v1/namespaces/${proj}/services`,
                { credentials: 'include' }
            );
            if (test.status === 200) return '__COOKIE_SESSION_OK__';
        } catch(e) {}
        return null;
    }""", proj_id, _REGION)

    if token == "__COOKIE_SESSION_OK__":
        log.info("Session cookie valide — utilisation sans Bearer token")
        return "__USE_COOKIES__"

    raise RuntimeError(
        "Impossible d'obtenir le token d'accès.\n"
        "Vérifiez que vous êtes bien connecté à Google Cloud dans le lab "
        "et que le lien SSO est celui de 'Open Google Console'."
    )


# ════════════════════════════════════════════════════════════
#  AUTHENTIFICATION GOOGLE — RÉSISTANTE AUX BLOCAGES
# ════════════════════════════════════════════════════════════

def _google_login(page, email: str, password: str) -> bool:
    """Login Google avec gestion des écrans intermédiaires"""
    try:
        page.goto("https://accounts.google.com/signin/v2/identifier",
                  wait_until="load", timeout=30_000)
        page.wait_for_timeout(2_000)

        # Remplir email
        email_input = page.wait_for_selector(
            "input[type=email], input[name=identifier]",
            state="visible", timeout=15_000
        )
        email_input.fill(email)
        page.wait_for_timeout(800)

        # Cliquer Suivant
        page.keyboard.press("Enter")
        page.wait_for_timeout(3_000)

        # Gérer écrans intermédiaires (compte déjà utilisé, choisir compte...)
        for _ in range(3):
            try:
                # Si page de sélection de compte
                account_btn = page.query_selector(f"[data-identifier='{email}']")
                if account_btn:
                    account_btn.click()
                    page.wait_for_timeout(2_000)
                    break
            except Exception:
                pass

        # Remplir password
        pwd_input = page.wait_for_selector(
            "input[type=password], input[name=password]",
            state="visible", timeout=15_000
        )
        page.wait_for_timeout(500)
        pwd_input.fill(password)
        page.wait_for_timeout(800)
        page.keyboard.press("Enter")
        page.wait_for_timeout(6_000)

        # Vérifier si connecté
        current_url = page.url
        if "myaccount.google.com" in current_url or "console.cloud.google.com" in current_url:
            return True

        # Gérer "continuer" ou "j'accepte"
        for selector in ["#submit", "button[type=submit]", "[jsname='LgbsSe']"]:
            btn = page.query_selector(selector)
            if btn:
                btn.click()
                page.wait_for_timeout(3_000)
                break

        return True

    except Exception as e:
        log.warning(f"Login échoué: {e}")
        return False


# ════════════════════════════════════════════════════════════
#  DÉPLOIEMENT PRINCIPAL — CORRIGÉ
# ════════════════════════════════════════════════════════════

def _deploy(url: str, svc_name: str, on_step, cancelled: list) -> dict:
    """Déploiement Cloud Run via Playwright — version corrigée"""
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    info = _extract_from_url(url)

    with sync_playwright() as p:
        on_step(1, "Init")
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
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
        # Masquer Playwright (éviter détection headless)
        ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        """)
        page = ctx.new_page()

        try:
            if cancelled[0]:
                raise RuntimeError("Annulé par l'utilisateur")

            on_step(2, "Chargement")
            log.info(f"Ouverture du lien SSO : {url[:80]}...")

            # Charger le lien SSO Qwiklabs
            try:
                page.goto(url, wait_until="load", timeout=90_000)
            except PWTimeout:
                page.wait_for_timeout(5_000)

            page.wait_for_timeout(5_000)

            if cancelled[0]:
                raise RuntimeError("Annulé par l'utilisateur")

            # Extraire credentials depuis la page
            creds = _extract_credentials(page)

            # Compléter avec ce qu'on a extrait de l'URL
            if not creds.get("email") and info.get("email"):
                creds["email"] = info["email"]
            if not creds.get("project_id") and info.get("project_id"):
                creds["project_id"] = info["project_id"]

            email      = creds.get("email")
            password   = creds.get("password")
            proj_id    = creds.get("project_id")

            if not proj_id:
                # Dernière tentative : extraire project depuis l'URL actuelle
                m = re.search(r'project[=:]([a-z][a-z0-9\-]{4,28})', page.url)
                if m:
                    proj_id = m.group(1)

            if not proj_id:
                raise RuntimeError(
                    "Project ID introuvable dans le lien.\n"
                    "Utilisez le lien de 'Open Google Console' et vérifiez que le lab est démarré."
                )

            log.info(f"Project ID: {proj_id} | Email: {email} | Password: {'OK' if password else 'N/A'}")

            # ── Authentification si credentials disponibles ──
            if email and password:
                on_step(3, "Auth")
                log.info("Tentative de login Google...")
                login_ok = _google_login(page, email, password)
                if login_ok:
                    log.info("Login réussi")
                else:
                    log.warning("Login incertain, on continue quand même...")
            else:
                log.info("Pas de credentials — utilisation de la session SSO directe")

            if cancelled[0]:
                raise RuntimeError("Annulé par l'utilisateur")

            on_step(4, "Console")

            # Aller sur Cloud Run Console
            page.goto(
                f"https://console.cloud.google.com/run?project={proj_id}",
                wait_until="domcontentloaded", timeout=60_000
            )
            page.wait_for_timeout(8_000)

            if cancelled[0]:
                raise RuntimeError("Annulé par l'utilisateur")

            # Récupérer le token OAuth
            on_step(5, "Token")
            token = _get_oauth_token(page, proj_id)
            use_cookies = (token == "__USE_COOKIES__")

            # Activer les APIs requises
            log.info("Activation des APIs...")
            for api in ["run.googleapis.com", "containerregistry.googleapis.com"]:
                try:
                    if use_cookies:
                        page.evaluate(f"""async () => {{
                            await fetch(
                                'https://serviceusage.googleapis.com/v1/projects/{proj_id}/services/{api}:enable',
                                {{ method:'POST', credentials:'include',
                                   headers:{{'Content-Type':'application/json'}} }}
                            );
                        }}""")
                    else:
                        page.evaluate(f"""async () => {{
                            await fetch(
                                'https://serviceusage.googleapis.com/v1/projects/{proj_id}/services/{api}:enable',
                                {{ method:'POST',
                                   headers:{{'Authorization':'Bearer {token}','Content-Type':'application/json'}} }}
                            );
                        }}""")
                except Exception as e:
                    log.warning(f"API activation warning ({api}): {e}")

            page.wait_for_timeout(12_000)

            if cancelled[0]:
                raise RuntimeError("Annulé par l'utilisateur")

            # Préparer la config Xray
            on_step(5, "Déploiement")
            uid_conn = str(uuid.uuid4())
            cfg = _build_cfg(uid_conn)
            cfg_json = json.dumps(cfg).replace('"', '\\"')

            svc_def = {
                "apiVersion": "serving.knative.dev/v1",
                "kind": "Service",
                "metadata": {"name": svc_name, "namespace": proj_id},
                "spec": {
                    "template": {
                        "metadata": {
                            "annotations": {
                                "autoscaling.knative.dev/maxScale": "1",
                                "run.googleapis.com/startup-cpu-boost": "true"
                            }
                        },
                        "spec": {
                            "containerConcurrency": 250,
                            "timeoutSeconds": 3600,
                            "containers": [{
                                "image": _IMG,
                                "ports": [{"containerPort": 8080}],
                                "resources": {"limits": {"memory": "256Mi", "cpu": "1000m"}},
                                "env": [{"name": "PPS_CFG", "value": json.dumps(cfg)}],
                                "command": ["sh", "-c"],
                                "args": [
                                    "echo $PPS_CFG > /etc/xray/config.json && "
                                    "xray run -config /etc/xray/config.json"
                                ]
                            }]
                        }
                    }
                }
            }

            # Déployer le service
            auth_headers = (
                f"{{'Content-Type':'application/json'}}"
                if use_cookies else
                f"{{'Authorization':'Bearer {token}','Content-Type':'application/json'}}"
            )
            credentials_mode = "'include'" if use_cookies else "'same-origin'"

            resp = page.evaluate(f"""async () => {{
                const r = await fetch(
                    'https://{_REGION}-run.googleapis.com/apis/serving.knative.dev/v1/namespaces/{proj_id}/services',
                    {{
                        method: 'POST',
                        credentials: {credentials_mode},
                        headers: {auth_headers},
                        body: JSON.stringify({json.dumps(svc_def)})
                    }}
                );
                return await r.json();
            }}""")

            if "error" in resp:
                err_msg = resp["error"].get("message", "Erreur inconnue")
                err_code = resp["error"].get("code", 0)
                raise RuntimeError(f"Déploiement refusé ({err_code}): {err_msg}")

            # Attendre que le service soit prêt
            on_step(6, "Attente")
            svc_url = None
            log.info("Attente du démarrage du service...")

            for attempt in range(45):
                if cancelled[0]:
                    raise RuntimeError("Annulé par l'utilisateur")

                page.wait_for_timeout(7_000)

                try:
                    st = page.evaluate(f"""async () => {{
                        const r = await fetch(
                            'https://{_REGION}-run.googleapis.com/apis/serving.knative.dev/v1/namespaces/{proj_id}/services/{svc_name}',
                            {{
                                credentials: {credentials_mode},
                                headers: {auth_headers}
                            }}
                        );
                        return await r.json();
                    }}""")

                    url_found = st.get("status", {}).get("url")
                    if url_found:
                        svc_url = url_found
                        log.info(f"Service prêt : {svc_url}")
                        break

                    conditions = st.get("status", {}).get("conditions", [])
                    for c in conditions:
                        if c.get("type") == "Ready" and c.get("status") == "False":
                            raise RuntimeError(f"Déploiement échoué : {c.get('message', 'Erreur inconnue')}")

                except RuntimeError:
                    raise
                except Exception as e:
                    log.warning(f"Polling attempt {attempt+1}: {e}")

                log.info(f"Attente service ({attempt+1}/45)...")

            if not svc_url:
                raise RuntimeError(
                    "Timeout : le service Cloud Run n'a pas démarré en temps voulu.\n"
                    "Le projet est peut-être suspendu ou les APIs non activées."
                )

            # Rendre le service public
            try:
                page.evaluate(f"""async () => {{
                    await fetch(
                        'https://{_REGION}-run.googleapis.com/v1/projects/{proj_id}/locations/{_REGION}/services/{svc_name}:setIamPolicy',
                        {{
                            method: 'POST',
                            credentials: {credentials_mode},
                            headers: {auth_headers},
                            body: JSON.stringify({{
                                policy: {{
                                    bindings: [{{
                                        role: 'roles/run.invoker',
                                        members: ['allUsers']
                                    }}]
                                }}
                            }})
                        }}
                    );
                }}""")
                log.info("Politique IAM appliquée (accès public)")
            except Exception as e:
                log.warning(f"IAM policy warning: {e}")

            return {
                "host": svc_url,
                "link": _make_conn_link(svc_url, uid_conn),
                "ref": svc_name,
            }

        finally:
            try:
                browser.close()
            except Exception:
                pass


# ════════════════════════════════════════════════════════════
#  FILE D'ATTENTE
# ════════════════════════════════════════════════════════════

def process_queue():
    global processing
    with processing_lock:
        if processing:
            return
        processing = True

    try:
        while job_queue:
            job = job_queue[0]
            chat_id  = job["chat_id"]
            svc      = job["svc"]
            url      = job["url"]
            uname    = job.get("username", "Anonyme")
            cancelled = job["cancelled"]

            sent   = bot.send_message(chat_id, bot_config["wait_message"], reply_markup=cancel_kb())
            msg_id = sent.message_id

            step_info = {"n": 0, "tick": 0}
            stop_flag = [False]

            def ticker():
                while not stop_flag[0] and not cancelled[0]:
                    time.sleep(5)
                    if stop_flag[0] or cancelled[0]:
                        break
                    step_info["tick"] += 1
                    msg = _PROGRESS[step_info["tick"] % len(_PROGRESS)]
                    bar = progress_bar(min(step_info["n"], 6))
                    try:
                        bot.edit_message_text(
                            f"⚡ *PPS_TECH* — Traitement\n\n{bar}\n\n{msg}",
                            chat_id, msg_id,
                            parse_mode="Markdown",
                            reply_markup=cancel_kb()
                        )
                    except Exception:
                        pass

            threading.Thread(target=ticker, daemon=True).start()

            def on_step(n, _):
                step_info["n"] = n

            try:
                result = _deploy(url, svc, on_step, cancelled)
                stop_flag[0] = True
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
                            f"✅ *Terminé !*\n\n{progress_bar(6)}\n\n🎯 *PPS_TECH* 🇨🇲",
                            chat_id, msg_id, parse_mode="Markdown"
                        )
                    except Exception:
                        pass

                    bot.send_message(
                        chat_id,
                        f"🎉 *Voici votre lien !*\n\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🌐 *Adresse :*\n`{result['host']}`\n\n"
                        f"🔑 *Lien de connexion :*\n`{result['link']}`\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"📋 *Utilisation :*\n"
                        f"1️⃣ Copiez le lien\n"
                        f"2️⃣ Ouvrez votre app\n"
                        f"3️⃣ Importez\n"
                        f"4️⃣ Connectez 🚀\n\n"
                        f"🏷️ *PPS_TECH* 🇨🇲",
                        parse_mode="Markdown",
                        reply_markup=main_menu_kb(chat_id)
                    )

                    if chat_id in users_db:
                        users_db[chat_id]["uses"] += 1

                    try:
                        bot.send_message(
                            ADMIN_ID,
                            f"✅ Déployé\n👤 `{uname}`\n🔖 `{svc}`\n🌐 `{result['host']}`",
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass

            except Exception as e:
                stop_flag[0] = True
                log.error(f"Erreur [{svc}]: {e}")
                if not cancelled[0]:
                    try:
                        bot.edit_message_text(
                            f"❌ *Échec*\n\n{str(e)[:400]}\n\n"
                            f"💡 Assurez-vous que :\n"
                            f"• Le lab est bien démarré\n"
                            f"• Vous utilisez le lien *Open Google Console*\n"
                            f"• Le projet n'est pas suspendu",
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
                            f"📊 *File PPS_TECH*\n\n📍 Position : *{i+1}/{len(job_queue)}*",
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass
    finally:
        with processing_lock:
            processing = False


# ════════════════════════════════════════════════════════════
#  HANDLERS
# ════════════════════════════════════════════════════════════

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
                f"🔒 *Accès restreint — PPS_TECH*\n\n"
                f"Bonjour {name} 👋\n\n"
                f"Rejoignez nos canaux officiels 📡\n\n"
                f"❌ Manquants :\n" + "\n".join(f"• {ch}" for ch in missing),
                reply_markup=join_kb(),
                parse_mode="Markdown"
            )
            return

    bot.send_message(
        uid,
        f"🌟 *Bienvenue sur PPS_TECH Bot !*\n\n"
        f"{'👑 Administrateur' if is_admin(uid) else '👤 Utilisateur'} — Bonjour {name} 👋\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 *Fonctionnement :*\n\n"
        f"1️⃣ Démarrez un lab Skills Google\n"
        f"2️⃣ Cliquez sur *Open Google Console*\n"
        f"3️⃣ Copiez le lien\n"
        f"4️⃣ Envoyez-le ici ✅\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🏷️ *PPS_TECH* 🇨🇲",
        reply_markup=main_menu_kb(uid),
        parse_mode="Markdown"
    )


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
        # ✅ FIX : utiliser from_user du callback, pas du message
        call.message.from_user = call.from_user
        cmd_start(call.message)
    else:
        bot.answer_callback_query(
            call.id,
            "❌ Vous n'êtes pas encore abonné aux deux canaux.",
            show_alert=True
        )


@bot.callback_query_handler(func=lambda c: c.data == "cancel")
def cb_cancel(call):
    uid = call.from_user.id
    # ✅ FIX : annuler le job actif via le flag cancelled
    for j in list(job_queue):
        if j["chat_id"] == uid:
            j["cancelled"][0] = True
            break
    user_sessions.pop(uid, None)
    bot.answer_callback_query(call.id, "❌ Annulé")
    try:
        bot.edit_message_text(
            "❌ *Opération annulée*",
            call.message.chat.id,
            call.message.message_id,
            parse_mode="Markdown"
        )
    except Exception:
        pass


@bot.callback_query_handler(func=lambda c: c.data == "help")
def cb_help(call):
    if not bot_config["help_content"]:
        bot.send_message(
            call.from_user.id,
            "📖 *Aide — PPS_TECH*\n\nAucun contenu disponible.",
            parse_mode="Markdown"
        )
    else:
        for item in bot_config["help_content"]:
            try:
                if item["type"] == "text":
                    bot.send_message(call.from_user.id, item["content"], parse_mode="Markdown")
                elif item["type"] == "video":
                    bot.send_video(call.from_user.id, item["file_id"], caption=item.get("caption", ""))
                elif item["type"] == "photo":
                    bot.send_photo(call.from_user.id, item["file_id"], caption=item.get("caption", ""))
                elif item["type"] == "document":
                    bot.send_document(call.from_user.id, item["file_id"], caption=item.get("caption", ""))
            except Exception:
                pass


@bot.callback_query_handler(func=lambda c: c.data == "position")
def cb_position(call):
    uid = call.from_user.id
    pos = next((i+1 for i, j in enumerate(job_queue) if j["chat_id"] == uid), None)
    if pos:
        bot.send_message(uid, f"📊 *Position : {pos}/{len(job_queue)}*", parse_mode="Markdown")
    else:
        bot.send_message(uid, "📭 *Pas dans la file*", parse_mode="Markdown")


@bot.callback_query_handler(func=lambda c: c.data == "contact")
def cb_contact(call):
    user_sessions[call.from_user.id] = {"state": "contact"}
    bot.send_message(
        call.from_user.id,
        "💬 *Contacter PPS_TECH*\n\nÉcrivez votre message en un seul bloc :",
        parse_mode="Markdown",
        reply_markup=cancel_kb()
    )


@bot.message_handler(
    func=lambda m: m.from_user.id in user_sessions
    and user_sessions[m.from_user.id].get("state") == "contact"
)
def handle_contact_msg(msg):
    user_sessions.pop(msg.from_user.id, None)
    name = msg.from_user.first_name or msg.from_user.username or "Anonyme"
    try:
        bot.send_message(
            ADMIN_ID,
            f"💬 *Message de {name}*\n\n{msg.text}",
            parse_mode="Markdown"
        )
        bot.send_message(
            msg.chat.id,
            "✅ *Message envoyé à PPS_TECH !*",
            parse_mode="Markdown",
            reply_markup=main_menu_kb(msg.from_user.id)
        )
    except Exception:
        bot.send_message(msg.chat.id, "❌ Erreur d'envoi.", parse_mode="Markdown")


@bot.message_handler(
    func=lambda m: m.text and ("http" in m.text.lower() or "www." in m.text.lower())
)
def handle_link(msg):
    register_user(msg.from_user)
    uid = msg.from_user.id

    if not is_admin(uid):
        ok, _ = is_subscribed(uid)
        if not ok:
            bot.send_message(
                uid,
                "🔒 *Accès restreint*\n\nRejoignez nos canaux.",
                reply_markup=join_kb(),
                parse_mode="Markdown"
            )
            return

    text = msg.text.strip()

    if any(j["chat_id"] == uid for j in job_queue):
        pos = next(i+1 for i, j in enumerate(job_queue) if j["chat_id"] == uid)
        bot.send_message(uid, f"⏳ *Déjà en cours !*\n\n📍 Position : *{pos}*", parse_mode="Markdown")
        return

    if len(job_queue) >= _MAX_Q:
        bot.send_message(
            uid,
            "🚫 *Service surchargé*\n\nRéessayez dans quelques minutes.",
            parse_mode="Markdown"
        )
        return

    svc       = next_svc()
    cancelled = [False]  # ✅ flag partagé entre le job et le handler cancel

    job_queue.append({
        "chat_id":   uid,
        "user_id":   uid,
        "username":  msg.from_user.first_name or msg.from_user.username or "Anonyme",
        "url":       text,
        "svc":       svc,
        "cancelled": cancelled,
    })

    pos = len(job_queue)
    if pos == 1:
        bot.send_message(uid, "✅ *Lien reçu !*\n\n⚡ Traitement immédiat...", parse_mode="Markdown")
    else:
        bot.send_message(uid, f"✅ *Lien reçu !*\n\n📍 Position : *{pos}*", parse_mode="Markdown")

    threading.Thread(target=process_queue, daemon=True).start()


@bot.callback_query_handler(func=lambda c: c.data == "users" and is_admin(c.from_user.id))
def cb_users(call):
    if not users_db:
        bot.send_message(ADMIN_ID, "📭 *Aucun utilisateur*", parse_mode="Markdown")
        return
    lines = [f"👥 *Utilisateurs — {len(users_db)}*\n"]
    for i, (uid, info) in enumerate(users_db.items(), 1):
        lines.append(
            f"{i}. 🆔 `{uid}` | 👤 {info['username']} | "
            f"📛 {info['first_name']} | 🔁 {info['uses']}x"
        )
    full = "\n".join(lines)
    for chunk in [full[i:i+3800] for i in range(0, len(full), 3800)]:
        bot.send_message(ADMIN_ID, chunk, parse_mode="Markdown")


# ════════════════════════════════════════════════════════════
#  LANCEMENT
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════╗
║  ⚡  PPS CLOUD RUN BOT                  ║
║  🇨🇲  Made in Cameroon                   ║
╚══════════════════════════════════════════╝
    """)
    log.info("Bot démarré — En attente...")
    bot.infinity_polling(timeout=30, long_polling_timeout=20)
