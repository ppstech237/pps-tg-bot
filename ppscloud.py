"""
PPS CLOUD RUN BOT
=================
Approche correcte basée sur l'analyse du vrai lien SSO Qwiklabs :

Le lien contient :
  - token=XXX       → token SSO Qwiklabs (authentifie sans password)
  - project=XXX     → project ID GCP
  - Email=XXX       → email du compte lab

Flow :
  1. Extraire token, project_id, email du lien
  2. Appeler skills.google/google_sso avec le token → obtenir cookies Google
  3. Utiliser ces cookies pour appeler l'API Cloud Run (fetch avec credentials)
  4. Déployer xray-core, récupérer l'URL
  5. Retourner le lien VLESS
"""

import re, uuid, json, time, logging, threading, requests as req
from collections import deque
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote, quote

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ══════════════════════════════════════════
#  CONFIG
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("PPSCLOUD")

bot           = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")
job_queue     = deque()
processing    = False
proc_lock     = threading.Lock()
svc_counter   = 1
users_db      = {}
user_sessions = {}

cfg = {
    "wait_msg": (
        "✅ *Lien reçu !*\n\n"
        "⚙️ Déploiement en cours...\n\n"
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
    "help_content":  [],
    "extra_buttons": [],
}

# ══════════════════════════════════════════
#  UTILITAIRES
# ══════════════════════════════════════════
def register_user(user):
    uid = user.id
    if uid not in users_db:
        users_db[uid] = {"username": user.username or "N/A",
                         "first_name": user.first_name or "N/A",
                         "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                         "uses": 0}
    else:
        users_db[uid]["username"]   = user.username or "N/A"
        users_db[uid]["first_name"] = user.first_name or "N/A"

def is_admin(uid):    return uid == ADMIN_ID
def is_subscribed(uid):
    missing = []
    for ch_id, ch_name in [(CHANNEL_1_ID, CHANNEL_1_NAME), (CHANNEL_2_ID, CHANNEL_2_NAME)]:
        try:
            m = bot.get_chat_member(ch_id, uid)
            if m.status in ("left","kicked","banned"): missing.append(ch_name)
        except Exception: missing.append(ch_name)
    return (len(missing) == 0, missing)

def progress_bar(step, total=6):
    filled = round((step/total)*10)
    return f"`[{'█'*filled}{'░'*(10-filled)}] {round((step/total)*100)}%`"

def next_svc():
    global svc_counter
    name = f"{_PREFIX}-{svc_counter:04d}"
    svc_counter += 1
    return name

def _make_conn_link(host_url, uid_conn):
    host = host_url.replace("https://","").replace("http://","").rstrip("/")
    path_enc = _WS_PATH.replace("/","%2F")
    return (f"vless://{uid_conn}@{host}:443"
            f"?path={path_enc}&security=tls&encryption=none"
            f"&host={host}&type=ws&sni={host}#{_CONN_TAG}")

def _build_xray_cfg(uid_conn):
    return {
        "log": {"loglevel":"warning"},
        "inbounds": [{
            "port": 8080, "protocol": "vless",
            "settings": {"clients":[{"id":uid_conn,"level":0}],"decryption":"none"},
            "streamSettings": {"network":"ws","wsSettings":{"path":_WS_PATH}},
        }],
        "outbounds": [{"protocol":"freedom","tag":"direct"}],
    }

# ══════════════════════════════════════════
#  CLAVIERS
# ══════════════════════════════════════════
def join_kb():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(f"📡 {CHANNEL_1_NAME}", url=CHANNEL_1_URL),
           InlineKeyboardButton(f"🎯 {CHANNEL_2_NAME}", url=CHANNEL_2_URL),
           InlineKeyboardButton("✅ J\'ai rejoint", callback_data="check_sub"))
    return kb

def cancel_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("❌ Annuler", callback_data="cancel"))
    return kb

def main_menu_kb(uid):
    kb = InlineKeyboardMarkup(row_width=2)
    for b in [x for x in cfg["extra_buttons"] if x.get("position")=="top"]:
        kb.add(InlineKeyboardButton(b["label"], url=b["url"]))
    kb.add(InlineKeyboardButton("📖 Aide", callback_data="help"),
           InlineKeyboardButton("📊 Ma position", callback_data="position"))
    if is_admin(uid):
        kb.add(InlineKeyboardButton("👥 Utilisateurs", callback_data="admin_users"),
               InlineKeyboardButton("⚙️ Configuration", callback_data="admin_config"))
        kb.add(InlineKeyboardButton("📢 Diffusion", callback_data="admin_broadcast"),
               InlineKeyboardButton("💬 Messages", callback_data="admin_messages"))
    else:
        kb.add(InlineKeyboardButton("💬 Contacter PPS\_TECH", callback_data="contact"))
    for b in [x for x in cfg["extra_buttons"] if x.get("position")!="top"]:
        kb.add(InlineKeyboardButton(b["label"], url=b["url"]))
    return kb

def config_kb():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✏️ Message d\'attente",  callback_data="cfg_wait"),
           InlineKeyboardButton("✏️ Message d\'accueil",  callback_data="cfg_welcome"),
           InlineKeyboardButton("➕ Ajouter bouton",      callback_data="cfg_add_btn"),
           InlineKeyboardButton("🗑️ Supprimer boutons",  callback_data="cfg_del_btn"),
           InlineKeyboardButton("🔙 Retour",              callback_data="cfg_back"))
    return kb

# ══════════════════════════════════════════
#  EXTRACTION DU LIEN SSO
#  Le lien contient déjà tout :
#  - token= (SSO Qwiklabs)
#  - project= (project_id GCP)
#  - Email= (email du lab)
# ══════════════════════════════════════════
def _parse_sso_link(url):
    """
    Extrait token SSO, project_id et email depuis le lien Open Google Console.
    Le lien a la structure :
    https://www.skills.google/google_sso
      ?token=XXX
      &relay=https://console.cloud.google.com/...?project=YYY
      &fallback=...#Email=ZZZ@qwiklabs.net
    """
    result = {}
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        # 1. Token SSO Qwiklabs (clé directe)
        token = params.get("token", [""])[0]
        if token:
            result["sso_token"] = token

        # 2. Project ID — chercher dans relay ou fallback
        for key in ["relay", "fallback", "continue", "next"]:
            val = params.get(key, [""])[0]
            if val:
                decoded = unquote(val)
                # Chercher qwiklabs-gcp-XX-XXXXXXXX
                m = re.search(r'project[=:]([a-z][a-z0-9\-]{4,28})', decoded)
                if m:
                    result["project_id"] = m.group(1)
                    break

        # Aussi directement dans l'URL
        if not result.get("project_id"):
            m = re.search(r'project[=:]([a-z][a-z0-9\-]{4,28})', url)
            if m:
                result["project_id"] = m.group(1)

        # 3. Email — dans le fragment (#Email=xxx) ou dans fallback
        full = unquote(url)
        m = re.search(r'[Ee]mail[=:]([^\s&\'"<>#]+@(?:qwiklabs\.net|cloudskillsboost\.goog))', full)
        if m:
            result["email"] = m.group(1).strip()

    except Exception as e:
        log.warning(f"Parse SSO link: {e}")

    log.info(f"SSO parse: token={'OK' if result.get('sso_token') else 'X'} "
             f"project={'OK' if result.get('project_id') else 'X'} "
             f"email={'OK' if result.get('email') else 'X'}")
    return result

# ══════════════════════════════════════════
#  AUTHENTIFICATION SSO → COOKIES GOOGLE
#  On suit la redirection du lien SSO pour
#  obtenir une session Google valide
# ══════════════════════════════════════════
def _sso_login(sso_url):
    """
    Suit le lien SSO Qwiklabs complet pour obtenir une session Google.
    Retourne (session_requests, access_token).
    Le lien redirige : skills.google → accounts.google.com → console.cloud.google.com
    On suit toutes les redirections et on récupère les cookies + token.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    session = req.Session()
    session.headers.update(headers)

    access_token = None
    final_url = None

    try:
        # Suivre toutes les redirections
        resp = session.get(sso_url, allow_redirects=True, timeout=30)
        final_url = resp.url
        log.info(f"SSO final URL: {final_url[:100]}")
        log.info(f"SSO status: {resp.status_code}")

        # Chercher un access_token dans la réponse ou les cookies
        # 1. Dans l'URL finale (parfois le token est dans le fragment ou les params)
        if "access_token" in resp.url:
            m = re.search(r'access_token=([^&\s]+)', resp.url)
            if m: access_token = m.group(1)

        # 2. Dans le contenu HTML de la page
        if not access_token and resp.text:
            m = re.search(r'ya29\.[A-Za-z0-9_\-]{20,}', resp.text)
            if m: access_token = m.group(0)

        # 3. Dans les cookies Google
        for cookie in session.cookies:
            log.info(f"Cookie: {cookie.name}={cookie.value[:20] if cookie.value else 'None'}...")

        # 4. Tenter d'obtenir le token via l'API tokeninfo avec la session active
        if not access_token:
            # Essayer d'appeler l'API Cloud Run avec la session (cookies)
            # pour vérifier si on est authentifié
            test_url = "https://console.cloud.google.com/m/cloudstorage/b"
            tr = session.get(test_url, timeout=15, allow_redirects=True)
            log.info(f"Test auth: {tr.status_code} {tr.url[:80]}")

        return session, access_token

    except Exception as e:
        log.error(f"SSO login error: {e}")
        raise RuntimeError(f"Erreur de connexion SSO: {e}")

# ══════════════════════════════════════════
#  OBTENIR LE TOKEN OAUTH VIA LA SESSION
# ══════════════════════════════════════════
def _get_oauth_token_from_session(session, project_id):
    """
    Utilise la session Google active pour récupérer un OAuth token.
    Méthode : appeler un endpoint Google Cloud qui retourne le token
    en parsant les headers de requête envoyés.
    """
    # Méthode 1 : endpoint de token via la session
    try:
        # L'endpoint /_/CloudConsolePortalUi/data/batchexecute retourne souvent un token
        resp = session.post(
            "https://console.cloud.google.com/m/gcr/imageslist",
            params={"project": project_id},
            timeout=20
        )
        log.info(f"gcr test: {resp.status_code}")
        m = re.search(r'ya29\.[A-Za-z0-9_\-]{20,}', resp.text)
        if m:
            return m.group(0)
    except Exception as e:
        log.warning(f"Method 1: {e}")

    # Méthode 2 : endpoint OAuth2 token via session cookies
    try:
        resp = session.get(
            f"https://console.cloud.google.com/run?project={project_id}",
            timeout=20, allow_redirects=True
        )
        log.info(f"Console run: {resp.status_code} {resp.url[:80]}")
        m = re.search(r'ya29\.[A-Za-z0-9_\-]{20,}', resp.text)
        if m:
            return m.group(0)
    except Exception as e:
        log.warning(f"Method 2: {e}")

    # Méthode 3 : essayer l'API Cloud Run directement avec les cookies
    # Si ça passe (status < 400), les cookies suffisent → retourner flag spécial
    try:
        resp = session.get(
            f"https://{_REGION}-run.googleapis.com/apis/serving.knative.dev/v1"
            f"/namespaces/{project_id}/services",
            timeout=20
        )
        log.info(f"Run API direct: {resp.status_code}")
        if resp.status_code < 400:
            return "__SESSION_COOKIES__"
        # Chercher le token dans la réponse
        m = re.search(r'ya29\.[A-Za-z0-9_\-]{20,}', resp.text)
        if m: return m.group(0)
    except Exception as e:
        log.warning(f"Method 3: {e}")

    return None

# ══════════════════════════════════════════
#  DÉPLOIEMENT VIA API REST
# ══════════════════════════════════════════
def _api_call(session, url, method="GET", token=None, json_body=None, use_cookies=False):
    """Appel API Google Cloud avec token ou cookies."""
    headers = {"Content-Type": "application/json"}
    if token and token != "__SESSION_COOKIES__":
        headers["Authorization"] = f"Bearer {token}"

    try:
        if method == "GET":
            r = session.get(url, headers=headers, timeout=30) if use_cookies \
                else req.get(url, headers=headers, timeout=30)
        else:
            r = session.post(url, headers=headers, json=json_body, timeout=60) if use_cookies \
                else req.post(url, headers=headers, json=json_body, timeout=60)

        log.info(f"API {method} {url[:80]}: {r.status_code}")
        if r.text:
            try: return r.json()
            except Exception: return {"__raw": r.text, "__status": r.status_code}
        return {"__status": r.status_code}
    except Exception as e:
        log.warning(f"API call error: {e}")
        return {"__error": str(e)}


def _deploy_service(session, token, project_id, svc_name, uid_conn, xray_cfg_json,
                    on_step, cancelled, send_msg):
    """Déploie le service Cloud Run via API REST."""

    use_cookies = (token == "__SESSION_COOKIES__")
    auth_headers = {"Content-Type": "application/json"}
    if not use_cookies and token:
        auth_headers["Authorization"] = f"Bearer {token}"

    def api_get(url):
        r = (session if use_cookies else req).get(url, headers=auth_headers, timeout=30)
        log.info(f"GET {url[:80]}: {r.status_code}")
        try: return r.json()
        except Exception: return {}

    def api_post(url, body):
        r = (session if use_cookies else req).post(
            url, headers=auth_headers, json=body, timeout=60)
        log.info(f"POST {url[:80]}: {r.status_code}")
        try: return r.json()
        except Exception: return {}

    if cancelled[0]: raise RuntimeError("Annulé")

    # Activer les APIs
    on_step(3)
    send_msg("🔧 *Activation des APIs Google Cloud...*")
    for api in ["run.googleapis.com", "containerregistry.googleapis.com"]:
        try:
            api_post(
                f"https://serviceusage.googleapis.com/v1/projects/{project_id}/services/{api}:enable",
                {}
            )
        except Exception as e:
            log.warning(f"API enable {api}: {e}")
    time.sleep(12)

    if cancelled[0]: raise RuntimeError("Annulé")

    # Déployer le service
    on_step(4)
    send_msg(f"🚀 *Déploiement du service `{svc_name}`...*\n\n⏳ Cela prend 2-5 minutes...")

    svc_def = {
        "apiVersion": "serving.knative.dev/v1",
        "kind": "Service",
        "metadata": {"name": svc_name, "namespace": project_id},
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
                    "resources": {"limits": {"memory":"256Mi","cpu":"1000m"}},
                    "env": [{"name":"XCFG","value":xray_cfg_json}],
                    "command": ["sh","-c"],
                    "args": ["echo $XCFG>/etc/xray/config.json&&xray run -config /etc/xray/config.json"],
                }],
            },
        }},
    }

    resp = api_post(
        f"https://{_REGION}-run.googleapis.com/apis/serving.knative.dev/v1"
        f"/namespaces/{project_id}/services",
        svc_def
    )

    if "error" in resp:
        code = resp["error"].get("code", "?")
        msg  = resp["error"].get("message", "Erreur inconnue")
        raise RuntimeError(f"Déploiement refusé ({code}): {msg}")

    if cancelled[0]: raise RuntimeError("Annulé")

    # Polling
    on_step(5)
    svc_url = None
    status_url = (
        f"https://{_REGION}-run.googleapis.com/apis/serving.knative.dev/v1"
        f"/namespaces/{project_id}/services/{svc_name}"
    )

    for attempt in range(60):
        if cancelled[0]: raise RuntimeError("Annulé")
        time.sleep(7)
        if attempt % 4 == 0:
            send_msg(f"⏳ *Démarrage du service...* ({attempt*7//60}m{attempt*7%60}s)")
        try:
            st = api_get(status_url)
            u = st.get("status", {}).get("url")
            if u:
                svc_url = u
                break
            for c in st.get("status", {}).get("conditions", []):
                if c.get("type") == "Ready" and c.get("status") == "False":
                    raise RuntimeError(f"Échec: {c.get('message','?')}")
        except RuntimeError: raise
        except Exception as e: log.warning(f"Poll {attempt+1}: {e}")
        log.info(f"Attente service ({attempt+1}/60)...")

    if not svc_url:
        raise RuntimeError("Timeout: service non démarré après 7 minutes.")

    # Rendre public
    on_step(6)
    try:
        api_post(
            f"https://{_REGION}-run.googleapis.com/v1/projects/{project_id}"
            f"/locations/{_REGION}/services/{svc_name}:setIamPolicy",
            {"policy": {"bindings": [{"role":"roles/run.invoker","members":["allUsers"]}]}}
        )
        log.info("IAM public OK")
    except Exception as e:
        log.warning(f"IAM: {e}")

    return svc_url

# ══════════════════════════════════════════
#  DÉPLOIEMENT PRINCIPAL
# ══════════════════════════════════════════
def _deploy(url, svc_name, on_step, cancelled, send_msg):
    if cancelled[0]: raise RuntimeError("Annulé")

    # Étape 1 : Parser le lien SSO
    on_step(1)
    send_msg("🔍 *Analyse du lien SSO...*")

    info = _parse_sso_link(url)

    if not info.get("sso_token"):
        raise RuntimeError(
            "Token SSO introuvable dans le lien.\n\n"
            "Envoyez exactement le lien du bouton "
            "*Open Google Console* avec le lab démarré."
        )
    if not info.get("project_id"):
        raise RuntimeError(
            "Project ID introuvable dans le lien.\n\n"
            "Vérifiez que le lab est démarré et renvoyez le lien."
        )

    sso_token  = info["sso_token"]
    project_id = info["project_id"]
    email      = info.get("email", "inconnu")

    send_msg(
        f"✅ *Lien analysé*\n\n"
        f"📧 `{email}`\n"
        f"🔖 `{project_id}`"
    )

    if cancelled[0]: raise RuntimeError("Annulé")

    # Étape 2 : Authentification SSO
    on_step(2)
    send_msg("🔐 *Authentification Google Cloud...*")

    session, access_token = _sso_login(url)

    # Si pas de token dans les redirections, chercher via la session
    if not access_token:
        send_msg("🔎 *Récupération du token OAuth...*")
        access_token = _get_oauth_token_from_session(session, project_id)

    if not access_token:
        raise RuntimeError(
            "Token OAuth non obtenu après connexion SSO.\n\n"
            "Le lab est peut-être expiré ou le lien a déjà été utilisé.\n"
            "Renvoyez un nouveau lien depuis un lab actif."
        )

    use_cookies = (access_token == "__SESSION_COOKIES__")
    send_msg(
        f"✅ *Authentification réussie !*\n"
        f"{'(via cookies de session)' if use_cookies else '(via token OAuth2)'}"
    )

    if cancelled[0]: raise RuntimeError("Annulé")

    # Étapes 3-6 : Déploiement
    uid_conn      = str(uuid.uuid4())
    xray_cfg      = _build_xray_cfg(uid_conn)
    xray_cfg_json = json.dumps(xray_cfg)

    svc_url = _deploy_service(
        session, access_token, project_id, svc_name,
        uid_conn, xray_cfg_json, on_step, cancelled, send_msg
    )

    return {
        "host": svc_url,
        "link": _make_conn_link(svc_url, uid_conn),
        "ref":  svc_name,
    }

# ══════════════════════════════════════════
#  FILE D'ATTENTE
# ══════════════════════════════════════════
def process_queue():
    global processing
    with proc_lock:
        if processing: return
        processing = True
    try:
        while job_queue:
            job       = job_queue[0]
            chat_id   = job["chat_id"]
            svc       = job["svc"]
            url       = job["url"]
            uname     = job.get("username", "Anonyme")
            cancelled = job["cancelled"]

            bot.send_message(chat_id, cfg["wait_msg"], reply_markup=cancel_kb())
            info = {"step": 0}

            def on_step(n, info=info): info["step"] = n
            def send_msg(text, chat_id=chat_id, cancelled=cancelled):
                if cancelled[0]: return
                try: bot.send_message(chat_id, text, parse_mode="Markdown",
                                      reply_markup=cancel_kb())
                except Exception: pass

            try:
                result = _deploy(url, svc, on_step, cancelled, send_msg)
                if cancelled[0]:
                    bot.send_message(chat_id, "❌ *Opération annulée*", parse_mode="Markdown")
                else:
                    bot.send_message(
                        chat_id,
                        f"🎉 *Votre lien VLESS est prêt !*\n\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🌐 *Adresse :*\n`{result['host']}`\n\n"
                        f"🔑 *Lien VLESS :*\n`{result['link']}`\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"1️⃣ Copiez le lien VLESS\n"
                        f"2️⃣ Ouvrez votre app VPN\n"
                        f"3️⃣ Importez & Connectez 🚀\n\n"
                        f"🏷️ *PPS\_TECH* 🇨🇲",
                        parse_mode="Markdown", reply_markup=main_menu_kb(chat_id)
                    )
                    if chat_id in users_db: users_db[chat_id]["uses"] += 1
                    try:
                        bot.send_message(ADMIN_ID,
                            f"✅ Déployé\n👤 `{uname}`\n🔖 `{svc}`\n🌐 `{result['host']}`",
                            parse_mode="Markdown")
                    except Exception: pass

            except Exception as e:
                log.error(f"Erreur [{svc}]: {e}")
                if not cancelled[0]:
                    bot.send_message(
                        chat_id,
                        f"❌ *Échec*\n\n{str(e)[:500]}\n\n"
                        f"💡 *Vérifiez :*\n"
                        f"• Lab bien démarré\n"
                        f"• Lien *Open Google Console* (bouton bleu)\n"
                        f"• Lien non expiré",
                        parse_mode="Markdown", reply_markup=main_menu_kb(chat_id)
                    )
                    try:
                        bot.send_message(ADMIN_ID,
                            f"❌ Échec\n👤 `{uname}`\n❗ `{str(e)[:500]}`",
                            parse_mode="Markdown")
                    except Exception: pass
            finally:
                job_queue.popleft()
                for i, j in enumerate(job_queue):
                    try:
                        bot.send_message(j["chat_id"],
                            f"📊 *File d\'attente*\n📍 Position : *{i+1}/{len(job_queue)}*",
                            parse_mode="Markdown")
                    except Exception: pass
    finally:
        with proc_lock: processing = False

# ══════════════════════════════════════════
#  HANDLER LIEN
# ══════════════════════════════════════════
def _handle_url(uid, text, first_name, username):
    if not is_admin(uid):
        ok, _ = is_subscribed(uid)
        if not ok:
            bot.send_message(uid, "🔒 *Accès restreint*\n\nRejoignez nos canaux.",
                             reply_markup=join_kb(), parse_mode="Markdown"); return
    if any(j["chat_id"]==uid for j in job_queue):
        pos = next(i+1 for i,j in enumerate(job_queue) if j["chat_id"]==uid)
        bot.send_message(uid, f"⏳ *Déjà en cours !*\n\n📍 Position : *{pos}*",
                         parse_mode="Markdown"); return
    if len(job_queue) >= _MAX_Q:
        bot.send_message(uid, "🚫 *Service surchargé*\n\nRéessayez dans quelques minutes.",
                         parse_mode="Markdown"); return
    user_sessions.pop(uid, None)
    svc = next_svc()
    cancelled = [False]
    job_queue.append({"chat_id":uid,"username":first_name or username or "Anonyme",
                      "url":text,"svc":svc,"cancelled":cancelled})
    pos = len(job_queue)
    bot.send_message(uid,
        "✅ *Lien reçu !*\n\n⚡ Traitement immédiat..." if pos==1
        else f"✅ *Lien reçu !*\n\n📍 Position : *{pos}*",
        parse_mode="Markdown")
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
            bot.send_message(uid,
                f"🔒 *Accès restreint — PPS\_TECH*\n\nBonjour {name} 👋\n\n"
                f"Rejoignez nos canaux officiels 📡\n\n❌ Manquants :\n"
                + "\n".join(f"• {ch}" for ch in missing),
                reply_markup=join_kb(), parse_mode="Markdown"); return
    role = "👑 Administrateur" if is_admin(uid) else "👤 Utilisateur"
    bot.send_message(uid, cfg["welcome_msg"].format(role=role,name=name),
                     reply_markup=main_menu_kb(uid), parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text and (
    "http://" in m.text or "https://" in m.text or "www." in m.text))
def handle_link(msg):
    register_user(msg.from_user)
    _handle_url(msg.from_user.id, msg.text.strip(),
                msg.from_user.first_name, msg.from_user.username)

@bot.message_handler(func=lambda m: m.from_user.id in user_sessions)
def handle_session_msg(msg):
    uid     = msg.from_user.id
    session = user_sessions.get(uid,{})
    state   = session.get("state")
    if msg.text and ("http://" in msg.text or "https://" in msg.text):
        _handle_url(uid,msg.text.strip(),msg.from_user.first_name,msg.from_user.username); return
    if state == "contact":
        user_sessions.pop(uid,None)
        name = msg.from_user.first_name or msg.from_user.username or "Anonyme"
        try:
            bot.send_message(ADMIN_ID,f"💬 *Message de {name}*\n\n{msg.text}",parse_mode="Markdown")
            bot.send_message(uid,"✅ *Message envoyé !*",parse_mode="Markdown",reply_markup=main_menu_kb(uid))
        except Exception: bot.send_message(uid,"❌ Erreur.",parse_mode="Markdown")
    elif state == "cfg_wait":
        user_sessions.pop(uid,None); cfg["wait_msg"] = msg.text
        bot.send_message(uid,"✅ *Mis à jour !*",parse_mode="Markdown",reply_markup=main_menu_kb(uid))
    elif state == "cfg_welcome":
        user_sessions.pop(uid,None); cfg["welcome_msg"] = msg.text
        bot.send_message(uid,"✅ *Mis à jour !*",parse_mode="Markdown",reply_markup=main_menu_kb(uid))
    elif state == "cfg_add_btn":
        user_sessions.pop(uid,None)
        try:
            parts = msg.text.strip().split("|")
            cfg["extra_buttons"].append({
                "label":parts[0].strip(),"url":parts[1].strip(),
                "position":parts[2].strip() if len(parts)>2 else "bottom"})
            bot.send_message(uid,f"✅ *Bouton ajouté !*",parse_mode="Markdown",reply_markup=main_menu_kb(uid))
        except Exception: bot.send_message(uid,"❌ Format : `Nom | https://lien.com | top`",parse_mode="Markdown")
    elif state == "cfg_broadcast":
        user_sessions.pop(uid,None)
        count = 0
        for u in users_db:
            try: bot.send_message(u,msg.text,parse_mode="Markdown"); count+=1
            except Exception: pass
        bot.send_message(uid,f"📢 *Diffusé à {count} utilisateurs*",parse_mode="Markdown",reply_markup=main_menu_kb(uid))

# ══════════════════════════════════════════
#  CALLBACKS
# ══════════════════════════════════════════
@bot.callback_query_handler(func=lambda c: c.data=="check_sub")
def cb_check_sub(call):
    uid = call.from_user.id
    ok, _ = is_subscribed(uid)
    if ok:
        bot.answer_callback_query(call.id,"✅ Accès accordé !")
        try: bot.delete_message(call.message.chat.id,call.message.message_id)
        except Exception: pass
        call.message.from_user = call.from_user
        cmd_start(call.message)
    else:
        bot.answer_callback_query(call.id,"❌ Pas encore abonné.",show_alert=True)

@bot.callback_query_handler(func=lambda c: c.data=="cancel")
def cb_cancel(call):
    uid = call.from_user.id
    for j in list(job_queue):
        if j["chat_id"]==uid: j["cancelled"][0]=True; break
    user_sessions.pop(uid,None)
    bot.answer_callback_query(call.id,"❌ Annulé")
    bot.send_message(uid,"❌ *Opération annulée*\n\nQue voulez-vous faire ?",
                     parse_mode="Markdown",reply_markup=main_menu_kb(uid))

@bot.callback_query_handler(func=lambda c: c.data=="help")
def cb_help(call):
    uid = call.from_user.id
    if not cfg["help_content"]:
        bot.send_message(uid,"📖 *Aide — PPS\_TECH*\n\nAucun contenu disponible.",parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data=="position")
def cb_position(call):
    uid = call.from_user.id
    pos = next((i+1 for i,j in enumerate(job_queue) if j["chat_id"]==uid),None)
    if pos: bot.send_message(uid,f"📊 *Position : {pos}/{len(job_queue)}*",parse_mode="Markdown")
    else:   bot.send_message(uid,"📭 *Pas de demande en cours.*",parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data=="contact")
def cb_contact(call):
    uid = call.from_user.id
    user_sessions[uid] = {"state":"contact"}
    bot.send_message(uid,"💬 *Contacter PPS\_TECH*\n\nÉcrivez votre message :",
                     parse_mode="Markdown",reply_markup=cancel_kb())

@bot.callback_query_handler(func=lambda c: c.data=="admin_users" and is_admin(c.from_user.id))
def cb_users(call):
    if not users_db: bot.send_message(ADMIN_ID,"📭 *Aucun utilisateur.*",parse_mode="Markdown"); return
    lines = [f"👥 *Utilisateurs — {len(users_db)}*\n"]
    for i,(uid,info) in enumerate(users_db.items(),1):
        lines.append(f"{i}. `{uid}` | {info['username']} | {info['first_name']} | 🔁{info['uses']}x")
    full = "\n".join(lines)
    for chunk in [full[i:i+3800] for i in range(0,len(full),3800)]:
        bot.send_message(ADMIN_ID,chunk,parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data=="admin_config" and is_admin(c.from_user.id))
def cb_config(call):
    bot.send_message(ADMIN_ID,"⚙️ *Configuration*\n\nChoisissez :",parse_mode="Markdown",reply_markup=config_kb())

@bot.callback_query_handler(func=lambda c: c.data=="cfg_wait" and is_admin(c.from_user.id))
def cb_cfg_wait(call):
    user_sessions[ADMIN_ID] = {"state":"cfg_wait"}
    bot.send_message(ADMIN_ID,f"✏️ *Actuel :*\n\n{cfg['wait_msg']}\n\nEnvoyez le nouveau :",
                     parse_mode="Markdown",reply_markup=cancel_kb())

@bot.callback_query_handler(func=lambda c: c.data=="cfg_welcome" and is_admin(c.from_user.id))
def cb_cfg_welcome(call):
    user_sessions[ADMIN_ID] = {"state":"cfg_welcome"}
    bot.send_message(ADMIN_ID,"✏️ Envoyez le nouveau message.\nVariables : `{role}` `{name}`",
                     parse_mode="Markdown",reply_markup=cancel_kb())

@bot.callback_query_handler(func=lambda c: c.data=="cfg_add_btn" and is_admin(c.from_user.id))
def cb_cfg_add_btn(call):
    user_sessions[ADMIN_ID] = {"state":"cfg_add_btn"}
    bot.send_message(ADMIN_ID,"➕ Format : `Nom | https://lien.com | top`",
                     parse_mode="Markdown",reply_markup=cancel_kb())

@bot.callback_query_handler(func=lambda c: c.data=="cfg_del_btn" and is_admin(c.from_user.id))
def cb_cfg_del_btn(call):
    cfg["extra_buttons"] = []
    bot.send_message(ADMIN_ID,"🗑️ *Boutons supprimés.*",parse_mode="Markdown",reply_markup=main_menu_kb(ADMIN_ID))

@bot.callback_query_handler(func=lambda c: c.data=="cfg_back")
def cb_cfg_back(call):
    bot.send_message(call.from_user.id,"🏠 *Menu principal*",
                     parse_mode="Markdown",reply_markup=main_menu_kb(call.from_user.id))

@bot.callback_query_handler(func=lambda c: c.data=="admin_broadcast" and is_admin(c.from_user.id))
def cb_broadcast(call):
    user_sessions[ADMIN_ID] = {"state":"cfg_broadcast"}
    bot.send_message(ADMIN_ID,f"📢 *Diffusion* — {len(users_db)} utilisateur(s)\n\nEnvoyez le message :",
                     parse_mode="Markdown",reply_markup=cancel_kb())

@bot.callback_query_handler(func=lambda c: c.data=="admin_messages" and is_admin(c.from_user.id))
def cb_messages(call):
    bot.send_message(ADMIN_ID,"💬 Les messages sont transférés directement.",parse_mode="Markdown")

# ══════════════════════════════════════════
#  LANCEMENT
# ══════════════════════════════════════════
if __name__ == "__main__":
    print("╔══════════════════════════════════════╗")
    print("║  ⚡  PPS CLOUD RUN BOT  🇨🇲          ║")
    print("║  Architecture : SSO Token Direct     ║")
    print("╚══════════════════════════════════════╝")
    log.info("Démarrage...")
    bot.infinity_polling(timeout=30, long_polling_timeout=20)
