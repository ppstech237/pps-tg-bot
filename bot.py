# ================================================================
#  PPS_TECH BOT — Code confidentiel
#  Tolérance maximale aux erreurs — Multi-stratégies
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
_TIMEOUT   = 600

BRAND = "PPS_TECH"

# ════════════════════════════════════════════════════════════
#  LOGGING
# ════════════════════════════════════════════════════════════
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("PPS_TECH")

# ════════════════════════════════════════════════════════════
#  ÉTAT GLOBAL
# ════════════════════════════════════════════════════════════
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")
job_queue = deque()
processing = False
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
#  DÉPLOIEMENT — MULTI-STRATÉGIES
# ════════════════════════════════════════════════════════════

def _extract_from_url(url: str) -> dict:
    """Stratégie 1 : Parser l'URL directement"""
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        
        for key in ['fallback', 'relay', 'continue']:
            encoded = params.get(key, [''])[0]
            if encoded:
                decoded = unquote(encoded)
                email_m = re.search(r'Email=([^&\s]+)', decoded)
                proj_m = re.search(r'project=([a-z0-9\-]+)', decoded)
                if email_m and proj_m:
                    return {
                        "email": unquote(email_m.group(1)),
                        "project_id": proj_m.group(1),
                        "method": "url_parse"
                    }
        
        proj_m = re.search(r'project=([a-z0-9\-]+)', url)
        if proj_m:
            return {"project_id": proj_m.group(1), "method": "url_direct"}
    except Exception as e:
        log.warning(f"URL parse failed: {e}")
    return {}

def _deploy(url: str, svc_name: str, on_step) -> dict:
    """Déploiement avec fallbacks multiples"""
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    
    info = _extract_from_url(url)
    
    with sync_playwright() as p:
        on_step(1, "Init")
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
        )
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
        )
        page = ctx.new_page()
        
        try:
            on_step(2, "Chargement")
            
            # STRATÉGIE A : Si on a déjà le project ID, aller direct à la console
            if info.get("project_id") and info.get("email"):
                proj_id = info["project_id"]
                email = info["email"]
                log.info(f"Stratégie A : URL parse OK → {email} / {proj_id}")
                
                # Chercher le password dans le lab
                try:
                    page.goto("https://www.cloudskillsboost.google/focuses", timeout=30_000)
                    page.wait_for_timeout(3_000)
                    body = page.inner_text("body")
                    pass_m = re.search(r"(?:Password|password)\s*[:\-=]\s*(\S+)", body)
                    password = pass_m.group(1) if pass_m else None
                except Exception:
                    password = None
                
                if not password:
                    # Fallback : ouvrir le lien SSO et extraire
                    page.goto(url, wait_until="load", timeout=60_000)
                    page.wait_for_timeout(5_000)
                    body = page.inner_text("body")
                    pass_m = re.search(r"(?:Password|password)\s*[:\-=]\s*(\S+)", body)
                    password = pass_m.group(1) if pass_m else None
                
            # STRATÉGIE B : Suivre le lien SSO normalement
            else:
                log.info("Stratégie B : Chargement du lien SSO")
                page.goto(url, wait_until="load", timeout=90_000)
                
                # Attendre redirection vers console OU rester sur la page lab
                try:
                    page.wait_for_url("**/console.cloud.google.com/**", timeout=15_000)
                    log.info("Redirigé vers console directement")
                    proj_m = re.search(r'project=([a-z0-9\-]+)', page.url)
                    proj_id = proj_m.group(1) if proj_m else None
                    email = password = None
                except PWTimeout:
                    log.info("Pas de redirection, extraction depuis page courante")
                    page.wait_for_timeout(7_000)
                    body = page.inner_text("body")
                    
                    email_m = re.search(r"[\w.+-]+@qwiklabs\.net", body, re.I)
                    pass_m = re.search(r"(?:Password|password)\s*[:\-=]\s*(\S+)", body)
                    proj_m = re.search(r"(?:Project ID|project[_\s-]?id)\s*[:\-=]\s*([a-z][a-z0-9\-]{4,28})", body, re.I)
                    
                    email = email_m.group(0) if email_m else None
                    password = pass_m.group(1) if pass_m else None
                    proj_id = proj_m.group(1) if proj_m else None
            
            if not proj_id:
                raise RuntimeError("Project ID introuvable. Vérifiez que le lab est bien démarré.")
            
            # Si on a email + password, se connecter
            if email and password:
                on_step(3, "Auth")
                page.goto("https://accounts.google.com/signin", wait_until="load")
                page.wait_for_selector("input[type=email]", timeout=20_000)
                page.fill("input[type=email]", email)
                page.click("#identifierNext")
                page.wait_for_timeout(3_500)
                
                page.wait_for_selector("input[type=password]", state="visible", timeout=15_000)
                page.fill("input[type=password]", password)
                page.click("#passwordNext")
                page.wait_for_timeout(7_000)
            
            on_step(4, "Préparation")
            uid_conn = str(uuid.uuid4())
            cfg = _build_cfg(uid_conn)
            
            on_step(5, "Déploiement")
            page.goto(f"https://console.cloud.google.com/run?project={proj_id}", 
                     wait_until="load", timeout=60_000)
            page.wait_for_timeout(10_000)
            
            token = page.evaluate("""async () => {
                try {
                    const r = await fetch('/_/cloudconsole/rpc/OAuthTokenService.GetAccessToken',
                        {method:'POST', credentials:'include', headers:{'Content-Type':'application/json'}, body:'{}'});
                    const t = await r.text();
                    const m = t.match(/ya29\\.[A-Za-z0-9_\\-]+/);
                    return m ? m[0] : null;
                } catch(e) { return null; }
            }""")
            
            if not token:
                raise RuntimeError("Token d'accès introuvable. Session expirée.")
            
            for api in ["run.googleapis.com", "cloudbuild.googleapis.com", "containerregistry.googleapis.com"]:
                page.evaluate(f"""async () => {{
                    await fetch('https://serviceusage.googleapis.com/v1/projects/{proj_id}/services/{api}:enable',
                        {{method:'POST', headers:{{'Authorization':'Bearer {token}', 'Content-Type':'application/json'}}}});
                }}""")
            page.wait_for_timeout(15_000)
            
            svc_def = {
                "apiVersion": "serving.knative.dev/v1",
                "kind": "Service",
                "metadata": {"name": svc_name, "namespace": proj_id},
                "spec": {
                    "template": {
                        "metadata": {"annotations": {"autoscaling.knative.dev/maxScale": "1"}},
                        "spec": {
                            "containerConcurrency": 250,
                            "timeoutSeconds": 3600,
                            "containers": [{
                                "image": _IMG,
                                "ports": [{"containerPort": 8080}],
                                "resources": {"limits": {"memory": "256Mi", "cpu": "1000m"}},
                                "env": [{"name": "PPS_CFG", "value": json.dumps(cfg)}],
                                "command": ["sh", "-c"],
                                "args": ["echo $PPS_CFG > /etc/xray/config.json && xray run -config /etc/xray/config.json"]
                            }]
                        }
                    }
                }
            }
            
            resp = page.evaluate(f"""async () => {{
                const r = await fetch('https://{_REGION}-run.googleapis.com/apis/serving.knative.dev/v1/namespaces/{proj_id}/services',
                    {{method:'POST', headers:{{'Authorization':'Bearer {token}', 'Content-Type':'application/json'}},
                      body: JSON.stringify({json.dumps(svc_def)})}});
                return await r.json();
            }}""")
            
            if "error" in resp:
                raise RuntimeError(f"Déploiement échoué : {resp['error'].get('message', 'Erreur inconnue')}")
            
            svc_url = None
            for attempt in range(40):
                page.wait_for_timeout(8_000)
                st = page.evaluate(f"""async () => {{
                    const r = await fetch('https://{_REGION}-run.googleapis.com/apis/serving.knative.dev/v1/namespaces/{proj_id}/services/{svc_name}',
                        {{headers:{{'Authorization':'Bearer {token}'}}}});
                    return await r.json();
                }}""")
                u = st.get("status", {}).get("url")
                if u:
                    svc_url = u
                    break
                log.info(f"Attente ({attempt+1}/40)...")
            
            if not svc_url:
                raise RuntimeError("Timeout : le service n'a pas démarré dans les délais.")
            
            page.evaluate(f"""async () => {{
                await fetch('https://{_REGION}-run.googleapis.com/v1/projects/{proj_id}/locations/{_REGION}/services/{svc_name}:setIamPolicy',
                    {{method:'POST', headers:{{'Authorization':'Bearer {token}', 'Content-Type':'application/json'}},
                      body:JSON.stringify({{policy:{{bindings:[{{role:'roles/run.invoker',members:['allUsers']}}]}}}})}});
            }}""")
            
            on_step(6, "Génération")
            return {
                "host": svc_url,
                "link": _make_conn_link(svc_url, uid_conn),
                "ref": svc_name,
            }
        
        finally:
            browser.close()

# ════════════════════════════════════════════════════════════
#  FILE D'ATTENTE
# ════════════════════════════════════════════════════════════

def process_queue():
    global processing
    if processing or not job_queue:
        return
    processing = True
    
    while job_queue:
        job = job_queue[0]
        chat_id = job["chat_id"]
        svc = job["svc"]
        url = job["url"]
        uname = job.get("username", "Anonyme")
        
        sent = bot.send_message(chat_id, bot_config["wait_message"], reply_markup=cancel_kb())
        msg_id = sent.message_id
        
        step_info = {"n": 0, "tick": 0}
        stop_flag = [False]
        cancelled = [False]
        
        def ticker():
            while not stop_flag[0] and not cancelled[0]:
                time.sleep(5)
                if stop_flag[0] or cancelled[0]:
                    break
                step_info["tick"] += 1
                msg = _PROGRESS[step_info["tick"] % len(_PROGRESS)]
                bar = progress_bar(step_info["n"])
                try:
                    bot.edit_message_text(
                        f"⚡ *PPS_TECH* — Traitement\n\n{bar}\n\n{msg}",
                        chat_id, msg_id, parse_mode="Markdown", reply_markup=cancel_kb()
                    )
                except Exception:
                    pass
        
        threading.Thread(target=ticker, daemon=True).start()
        
        def on_step(n, _):
            step_info["n"] = n
        
        try:
            result = _deploy(url, svc, on_step)
            stop_flag[0] = True
            time.sleep(0.5)
            
            if cancelled[0]:
                bot.edit_message_text("❌ *Opération annulée*", chat_id, msg_id, parse_mode="Markdown")
            else:
                bot.edit_message_text(
                    f"✅ *Terminé !*\n\n{progress_bar(6)}\n\n🎯 *PPS_TECH* 🇨🇲",
                    chat_id, msg_id, parse_mode="Markdown"
                )
                
                bot.send_message(chat_id,
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
                    parse_mode="Markdown", reply_markup=main_menu_kb(chat_id)
                )
                
                if chat_id in users_db:
                    users_db[chat_id]["uses"] += 1
                
                try:
                    bot.send_message(ADMIN_ID, f"✅ Déployé\n👤 `{uname}`\n🔖 `{svc}`\n🌐 `{result['host']}`", parse_mode="Markdown")
                except Exception:
                    pass
        
        except Exception as e:
            stop_flag[0] = True
            log.error(f"Erreur [{svc}]: {e}")
            bot.edit_message_text(
                f"❌ *Échec*\n\n{str(e)[:300]}\n\n💡 Vérifiez que le lab est bien démarré.",
                chat_id, msg_id, parse_mode="Markdown", reply_markup=main_menu_kb(chat_id)
            )
            try:
                bot.send_message(ADMIN_ID, f"❌ Échec\n👤 `{uname}`\n❗ `{str(e)[:400]}`", parse_mode="Markdown")
            except Exception:
                pass
        
        finally:
            job_queue.popleft()
            for i, j in enumerate(job_queue):
                try:
                    bot.send_message(j["chat_id"], f"📊 *File PPS_TECH*\n\n📍 Position : *{i+1}/{len(job_queue)}*", parse_mode="Markdown")
                except Exception:
                    pass
    
    processing = False

# ════════════════════════════════════════════════════════════
#  HANDLERS
# ════════════════════════════════════════════════════════════

@bot.message_handler(commands=["start"])
def cmd_start(msg):
    register_user(msg.from_user)
    uid = msg.from_user.id
    name = msg.from_user.first_name or "Utilisateur"
    
    if not is_admin(uid):
        ok, missing = is_subscribed(uid)
        if not ok:
            bot.send_message(uid,
                f"🔒 *Accès restreint — PPS_TECH*\n\n"
                f"Bonjour {name} 👋\n\n"
                f"Rejoignez nos canaux officiels 📡\n\n"
                f"❌ Manquants :\n{chr(10).join(f'• {ch}' for ch in missing)}",
                reply_markup=join_kb(), parse_mode="Markdown"
            )
            return
    
    bot.send_message(uid,
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
        reply_markup=main_menu_kb(uid), parse_mode="Markdown"
    )

@bot.callback_query_handler(func=lambda c: c.data == "check_sub")
def cb_check_sub(call):
    ok, _ = is_subscribed(call.from_user.id)
    if ok:
        bot.answer_callback_query(call.id, "✅ Accès accordé !")
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        cmd_start(call.message)
    else:
        bot.answer_callback_query(call.id, "❌ Vous n'êtes pas encore abonné aux deux canaux.", show_alert=True)

@bot.callback_query_handler(func=lambda c: c.data == "cancel")
def cb_cancel(call):
    uid = call.from_user.id
    user_sessions.pop(uid, None)
    bot.answer_callback_query(call.id, "❌ Annulé")
    bot.edit_message_text("❌ *Opération annulée*", call.message.chat.id, call.message.message_id, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data == "help")
def cb_help(call):
    if not bot_config["help_content"]:
        bot.send_message(call.from_user.id, "📖 *Aide — PPS_TECH*\n\nAucun contenu disponible.", parse_mode="Markdown")
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
    pos = next((i+1 for i, j in enumerate(job_queue) if j["chat_id"] == call.from_user.id), None)
    if pos:
        bot.send_message(call.from_user.id, f"📊 *Position : {pos}/{len(job_queue)}*", parse_mode="Markdown")
    else:
        bot.send_message(call.from_user.id, "📭 *Pas dans la file*", parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data == "contact")
def cb_contact(call):
    user_sessions[call.from_user.id] = {"state": "contact"}
    bot.send_message(call.from_user.id,
        "💬 *Contacter PPS_TECH*\n\nÉcrivez votre message en un seul bloc :",
        parse_mode="Markdown", reply_markup=cancel_kb()
    )

@bot.message_handler(func=lambda m: m.from_user.id in user_sessions and user_sessions[m.from_user.id].get("state") == "contact")
def handle_contact_msg(msg):
    user_sessions.pop(msg.from_user.id, None)
    name = msg.from_user.first_name or msg.from_user.username or "Anonyme"
    try:
        bot.send_message(ADMIN_ID,
            f"💬 *Message de {name}*\n\n{msg.text}",
            parse_mode="Markdown"
        )
        bot.send_message(msg.chat.id, "✅ *Message envoyé à PPS_TECH !*", parse_mode="Markdown", reply_markup=main_menu_kb(msg.from_user.id))
    except Exception:
        bot.send_message(msg.chat.id, "❌ Erreur d'envoi.", parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text and ("http" in m.text.lower() or "www." in m.text.lower()))
def handle_link(msg):
    register_user(msg.from_user)
    uid = msg.from_user.id
    
    if not is_admin(uid):
        ok, _ = is_subscribed(uid)
        if not ok:
            bot.send_message(uid, "🔒 *Accès restreint*\n\nRejoignez nos canaux.", reply_markup=join_kb(), parse_mode="Markdown")
            return
    
    text = msg.text.strip()
    
    if any(j["chat_id"] == uid for j in job_queue):
        pos = next(i+1 for i, j in enumerate(job_queue) if j["chat_id"] == uid)
        bot.send_message(uid, f"⏳ *Déjà en cours !*\n\n📍 Position : *{pos}*", parse_mode="Markdown")
        return
    
    if len(job_queue) >= _MAX_Q:
        bot.send_message(uid, "🚫 *Service surchargé*\n\nRéessayez dans quelques minutes.", parse_mode="Markdown")
        return
    
    svc = next_svc()
    job_queue.append({
        "chat_id": uid,
        "user_id": uid,
        "username": msg.from_user.first_name or msg.from_user.username or "Anonyme",
        "url": text,
        "svc": svc,
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
        lines.append(f"{i}. 🆔 `{uid}` | 👤 {info['username']} | 📛 {info['first_name']} | 🔁 {info['uses']}x")
    full = "\n".join(lines)
    for chunk in [full[i:i+3800] for i in range(0, len(full), 3800)]:
        bot.send_message(ADMIN_ID, chunk, parse_mode="Markdown")

# ════════════════════════════════════════════════════════════
#  LANCEMENT
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════╗
║  ⚡  PPS_TECH BOT                       ║
║  🇨🇲  Made in Cameroon                   ║
╚══════════════════════════════════════════╝
    """)
    log.info("Bot démarré — En attente...")
    bot.infinity_polling(timeout=30, long_polling_timeout=20)
