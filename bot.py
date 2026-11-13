"""
🌐 URL ➜ HTML — Pro Bot (Termux Compatible)
═══════════════════════════════════════════
URL dile bot full webpage er HTML source file
download kore pathiye dibe. Lightweight —
playwright/browser lagbe na, Termux-e 100% kaaj korbe.

Setup (Termux):
    pip install python-telegram-bot requests --upgrade

Run:
    python test.py
"""

import asyncio
import html as html_lib
import json
import logging
import os
import re
import time
import threading
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
import requests
from flask import Flask
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from telegram.error import TelegramError, RetryAfter

import config
import turso_db
import api_client

# ═══════════════════════ CONFIG ═══════════════════════
# User-specific values are kept in config.py; never commit live credentials.
BOT_TOKEN = str(config.BOT_TOKEN).strip()
ADMIN_IDS = {int(value) for value in config.ADMIN_IDS}
LOGS_GROUP_ID = config.LOGS_GROUP_ID
DATA_FILE = config.DATA_FILE
PORT = int(os.environ.get("PORT", getattr(config, "PORT", 8080)))

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.WARNING,
)
for _noisy in ("httpx", "httpcore", "telegram", "apscheduler"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

BOT_START_TIME = time.time()

# ═══════════════════════ HEALTH SERVER (Render) ═══════════════════════
flask_app = Flask(__name__)

@flask_app.route("/health")
def health():
    uptime_seconds = int(time.time() - BOT_START_TIME)
    hours, rem = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    uptime_str = f"{hours}h {minutes}m {seconds}s"
    return {
        "status": "ok",
        "uptime": uptime_str,
        "uptime_seconds": uptime_seconds,
    }

def run_flask():
    port = PORT
    flask_app.run(host="0.0.0.0", port=port, use_reloader=False, threaded=True)


# ═══════════════════════ PERSISTENT STORAGE ═══════════════════════
def load_data():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                data.setdefault("users", {})
                data.setdefault("stats", {"total_conversions": 0})
                data.setdefault("force_join_channels", [])
                return data
        except Exception:
            pass
    return {"users": {}, "stats": {"total_conversions": 0}, "force_join_channels": []}


_save_state = {"dirty": False, "ref": None}
_save_lock = threading.Lock()


def _flush_to_disk():
    """Write BOT_DATA to disk atomically. Runs only in the flusher thread
    (or at exit), never in the event loop."""
    ref = _save_state["ref"]
    if ref is None:
        return
    try:
        payload = json.dumps(ref, indent=2)
    except RuntimeError:
        # dict changed size while serialising — retry on next tick
        _save_state["dirty"] = True
        return
    try:
        tmp = DATA_FILE + ".tmp"
        with _save_lock:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(payload)
            os.replace(tmp, DATA_FILE)
    except Exception as e:
        print(f"⚠️ Failed to save data: {e}")


def _flusher_loop():
    while True:
        time.sleep(5)
        if _save_state["dirty"]:
            _save_state["dirty"] = False
            _flush_to_disk()


def save_data(data):
    """Non-blocking: marks data dirty; a background thread writes it to
    disk at most every few seconds (instead of on EVERY user message)."""
    _save_state["ref"] = data
    _save_state["dirty"] = True


threading.Thread(target=_flusher_loop, daemon=True).start()
import atexit
atexit.register(_flush_to_disk)


BOT_DATA = load_data()


def sync_with_turso():
    """On startup: load users/stats/force-join from Turso (source of truth),
    and push any users that only exist in the local JSON file (one-time
    migration). Falls back to the local JSON if Turso is unreachable."""
    if not turso_db.ENABLED:
        print("⚠️ Turso not configured — using local JSON only.")
        return
    try:
        turso_db.init_schema()
        db_users, db_stats, db_channels = turso_db.load_all()

        local_only = [
            u for uid, u in BOT_DATA["users"].items() if uid not in db_users
        ]
        if local_only:
            turso_db.upsert_users_bulk(local_only)
            print(f"⬆️ Migrated {len(local_only)} local users to Turso")

        # merge: Turso users + local-only users
        merged = dict(db_users)
        for u in local_only:
            merged[str(u["id"])] = u
        BOT_DATA["users"] = merged

        if db_stats is not None:
            BOT_DATA["stats"] = db_stats
        else:
            turso_db.set_kv("stats", BOT_DATA["stats"])

        if db_channels is not None:
            BOT_DATA["force_join_channels"] = db_channels
        else:
            turso_db.set_kv("force_join_channels", BOT_DATA["force_join_channels"])

        save_data(BOT_DATA)
        print(f"✅ Turso connected — {len(BOT_DATA['users'])} users loaded")
    except Exception as e:
        print(f"❌ Turso sync failed, using local JSON: {e}")


sync_with_turso()


def get_force_join_channels() -> list:
    return BOT_DATA["force_join_channels"]


_LAST_TURSO_PUSH: dict = {}


def register_user(user) -> bool:
    """Registers a user if new. Returns True if this user is new."""
    uid = str(user.id)
    is_new = uid not in BOT_DATA["users"]
    BOT_DATA["users"][uid] = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "joined": BOT_DATA["users"].get(uid, {}).get(
            "joined", time.strftime("%Y-%m-%d %H:%M:%S")
        ),
        "last_seen": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    save_data(BOT_DATA)
    now = time.time()
    if is_new or now - _LAST_TURSO_PUSH.get(uid, 0) > 300:
        _LAST_TURSO_PUSH[uid] = now
        turso_db.upsert_user(BOT_DATA["users"][uid])
    return is_new


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def send_log(context: ContextTypes.DEFAULT_TYPE, text: str):
    """Send a message to the logs group. Fails silently if not configured."""
    if not LOGS_GROUP_ID:
        return
    try:
        await context.bot.send_message(
            chat_id=LOGS_GROUP_ID, text=text, parse_mode="HTML"
        )
    except Exception as e:
        print(f"⚠️ Failed to send log: {e}")


async def log_activity(
    context: ContextTypes.DEFAULT_TYPE,
    user,
    action: str,
    detail: str = "",
    result: str = "",
):
    """Unified activity logger: logs every user action and the bot's
    response/outcome to the logs group in a consistent format."""
    username_part = f"@{user.username}" if user.username else "no username"
    lines = [
        f"📌 <b>{action}</b>",
        "",
        f"👤 <b>User:</b> {user.first_name} ({username_part})",
        f"🆔 <b>ID:</b> <code>{user.id}</code>",
    ]
    if detail:
        lines.append(f"💬 <b>Input:</b> <code>{detail[:500]}</code>")
    if result:
        lines.append(f"📤 <b>Bot Response:</b> {result}")
    lines.append(f"🕒 <b>Time:</b> {time.strftime('%Y-%m-%d %H:%M:%S')}")
    await send_log(context, "\n".join(lines))


# ═══════════════════════ FORCE JOIN CHECK ═══════════════════════
def _normalize_chat_id(raw: str) -> str:
    """Accepts '@name', 'name', or numeric '-100...' and normalizes to
    a format get_chat_member accepts."""
    raw = raw.strip()
    if raw.lstrip("-").isdigit():
        return raw
    return raw if raw.startswith("@") else f"@{raw}"


_JOIN_OK: dict = {}


async def check_force_join(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Returns True if user has joined all required channels (or none configured)."""
    channels = get_force_join_channels()
    if not channels:
        return True

    user_id = update.effective_user.id
    if time.time() - _JOIN_OK.get(user_id, 0) < 120:
        return True
    not_joined = []

    for channel in channels:
        chat_id = _normalize_chat_id(str(channel["chat_id"]))
        try:
            member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            if member.status in ("left", "kicked"):
                not_joined.append(channel)
        except TelegramError as e:
            # IMPORTANT: this almost always means the bot itself is not an
            # admin in the target channel, so it can't see membership status.
            # Treat as "not verified yet" but log the real reason so the
            # admin can fix it (instead of silently always failing).
            print(f"⚠️ check_force_join: couldn't verify {chat_id} for user {user_id}: {e}")
            if LOGS_GROUP_ID:
                await send_log(
                    context,
                    "⚠️ <b>Force-Join Check Failed</b>\n\n"
                    f"Channel: <code>{chat_id}</code>\n"
                    f"Reason: <code>{str(e)[:200]}</code>\n"
                    "<i>Likely cause: the bot is not an admin in this channel.</i>",
                )
            not_joined.append(channel)

    if not_joined:
        buttons = [
            [InlineKeyboardButton(f"📢 {ch['title']}", url=ch["url"])]
            for ch in not_joined
        ]
        buttons.append(
            [SBtn("✅ I've Joined", style="success", callback_data="check_join")]
        )
        text = (
            "🔒 <b>Join Required</b>\n\n"
            "<blockquote>Please join the channel(s) below to use this bot, "
            "then tap <b>I've Joined</b>.</blockquote>"
        )
        if update.message:
            await update.message.reply_text(
                text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons)
            )
        elif update.callback_query:
            await update.callback_query.edit_message_text(
                text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons)
            )
        return False

    _JOIN_OK[user_id] = time.time()
    return True
class SBtn(InlineKeyboardButton):
    """InlineKeyboardButton with optional 'style' (primary/success/danger)
    and 'icon_custom_emoji_id' (premium custom emoji icon) fields,
    for Bot API 9.4+ clients. Falls back silently on older clients."""

    def __init__(self, text, style=None, icon_custom_emoji_id=None, **kwargs):
        super().__init__(text, **kwargs)
        self._style = style
        self._icon_custom_emoji_id = icon_custom_emoji_id

    def to_dict(self, recursive=True):
        d = super().to_dict(recursive=recursive)
        if self._style:
            d["style"] = self._style
        if self._icon_custom_emoji_id:
            d["icon_custom_emoji_id"] = self._icon_custom_emoji_id
        return d


# ═══════════════════════ KEYBOARDS ═══════════════════════
def main_menu_keyboard():
    return InlineKeyboardMarkup(
        [
            [SBtn("Convert URL", style="success", icon_custom_emoji_id="6228590336055186198", callback_data="convert")],
            [
                SBtn("SEO Report", style="primary", callback_data="seo"),
                SBtn("🧩 Tech Stack", style="primary", icon_custom_emoji_id="6228978596803774375", callback_data="tech"),
            ],
            [SBtn("Offline ZIP Package", style="primary", callback_data="zip")],
            [
                SBtn("📸 Screenshot", style="primary", callback_data="shot"),
                SBtn("🗜 Single-File Snapshot", style="primary", callback_data="snap"),
            ],
            [
                SBtn("Help", style="primary", icon_custom_emoji_id="5298805275967376925", callback_data="help"),
                SBtn("About", style="primary", icon_custom_emoji_id="5298805275967376925", callback_data="about"),
            ],
            [SBtn("Updates Channel", style="primary", icon_custom_emoji_id="6084902351196396155", url="https://t.me/SHUVOMODS6")],
        ]
    )


def back_keyboard():
    return InlineKeyboardMarkup(
        [[SBtn("🔙 Back to Menu", style="danger", callback_data="back")]]
    )


def result_keyboard():
    return InlineKeyboardMarkup(
        [
            [SBtn("🔁 Convert Another URL", style="success", callback_data="convert")],
            [SBtn("🔙 Main Menu", style="danger", callback_data="back")],
        ]
    )


def admin_panel_keyboard():
    return InlineKeyboardMarkup(
        [
            [
                SBtn("📊 Stats", style="primary", callback_data="admin_stats"),
                SBtn("👥 Users", style="primary", callback_data="admin_users"),
            ],
            [SBtn("📢 Broadcast", style="success", callback_data="admin_broadcast")],
            [SBtn("🔗 Force Join Channels", style="primary", callback_data="admin_forcejoin")],
            [SBtn("➕ Add Force Join", style="success", callback_data="admin_addforcejoin")],
            [SBtn("🔙 Close Panel", style="danger", callback_data="admin_close")],
        ]
    )


def admin_back_keyboard():
    return InlineKeyboardMarkup(
        [[SBtn("🔙 Back to Admin Panel", style="danger", callback_data="admin_panel")]]
    )


# ═══════════════════════ START / MENU ═══════════════════════
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    # Save the user FIRST, regardless of force-join status, so broadcasts
    # and stats always include everyone who has ever pressed /start.
    is_new = register_user(user)
    if is_new:
        username_part = f"@{user.username}" if user.username else "no username"
        await send_log(
            context,
            "🆕 <b>New User Started The Bot</b>\n\n"
            f"👤 <b>Name:</b> {user.first_name}\n"
            f"🔗 <b>Username:</b> {username_part}\n"
            f"🆔 <b>User ID:</b> <code>{user.id}</code>\n"
            f"📅 <b>Joined:</b> {BOT_DATA['users'][str(user.id)]['joined']}",
        )

    if not await check_force_join(update, context):
        return

    text = (
        '<tg-emoji emoji-id="6136525109315245846">✨</tg-emoji> '
        "<b>Welcome to URL ➜ HTML Pro Bot</b> "
        '<tg-emoji emoji-id="6136525109315245846">✨</tg-emoji>\n\n'
        f'<tg-emoji emoji-id="6095851644468072524">👋</tg-emoji> '
        f"Hello, <b>{user.first_name}</b>!\n\n"
        "<blockquote>"
        '<tg-emoji emoji-id="6253414379442670769">✅</tg-emoji> '
        "I can fetch the full HTML source code of any website "
        "and turn it into a downloadable <b>.html</b> file!"
        "</blockquote>\n\n"
        '<tg-emoji emoji-id="6183532431153304141">👇</tg-emoji> '
        "<b>Choose an option below:</b>"
    )
    if update.message:
        await update.message.reply_text(
            text, parse_mode="HTML", reply_markup=main_menu_keyboard()
        )
    else:
        await update.callback_query.edit_message_text(
            text, parse_mode="HTML", reply_markup=main_menu_keyboard()
        )


# ═══════════════════════ BUTTON HANDLER ═══════════════════════
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "convert":
        context.user_data["mode"] = "html"
        text = (
            '<tg-emoji emoji-id="6204235659578712061">😂</tg-emoji> '
            "<b>Send me the URL now</b>\n\n"
            "<blockquote>"
            '<tg-emoji emoji-id="6203763122981837713">👷</tg-emoji> Example:\n'
            "<code>https://example.com</code>"
            "</blockquote>\n\n"
            '<tg-emoji emoji-id="6339296880500936916">⚡️</tg-emoji> '
            "<b>Output:</b> Full website, every internal page + assets, "
            "bundled as one offline ZIP. JavaScript-only pages may be incomplete; "
            "access challenges are reported, not bypassed."
        )
        await query.edit_message_text(
            text, parse_mode="HTML", reply_markup=back_keyboard()
        )

    elif query.data == "seo":
        context.user_data["mode"] = "seo"
        text = (
            '<tg-emoji emoji-id="5188311512791393083">🔎</tg-emoji> '
            "<b>SEO / Page Report</b>\n\n"
            "<blockquote>"
            "Send me a URL — I'll check:\n"
            "• Title &amp; meta description\n"
            "• Heading structure (H1-H6)\n"
            "• Load time\n"
            "• Image &amp; link count"
            "</blockquote>"
        )
        await query.edit_message_text(
            text, parse_mode="HTML", reply_markup=back_keyboard()
        )

    elif query.data == "tech":
        context.user_data["mode"] = "tech"
        text = (
            '<tg-emoji emoji-id="6228978596803774375">🧩</tg-emoji> '
            "<b>Tech Stack Detector</b>\n\n"
            "<blockquote>"
            "Send me a URL — I'll try to detect the platform/framework "
            "behind it (WordPress, Shopify, React, Next.js, etc.)"
            "</blockquote>"
        )
        await query.edit_message_text(
            text, parse_mode="HTML", reply_markup=back_keyboard()
        )

    elif query.data == "zip":
        context.user_data["mode"] = "zip"
        text = (
            '<tg-emoji emoji-id="5773788281917412000">❤️</tg-emoji> '
            "<b>Offline ZIP Package</b>\n\n"
            "<blockquote>"
            "Send me a URL — I'll bundle the page's HTML along with its "
            "linked CSS, JS, and images into a single ZIP you can open "
            "offline in a browser."
            "</blockquote>\n\n"
            '<tg-emoji emoji-id="5280533308269155503">📝</tg-emoji> '
            "<i>Note: only same-page linked assets are fetched; "
            "very heavy sites may take longer.</i>"
        )
        await query.edit_message_text(
            text, parse_mode="HTML", reply_markup=back_keyboard()
        )

    elif query.data == "shot":
        context.user_data["mode"] = "screenshot"
        text = (
            "📸 <b>Full-Page Screenshot</b>\n\n"
            "<blockquote>"
            "Send me a URL — I'll render the page in a real browser and send you "
            "back a full-page screenshot (JPEG)."
            "</blockquote>"
        )
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=back_keyboard())

    elif query.data == "snap":
        context.user_data["mode"] = "snapshot"
        text = (
            "🗜 <b>Single-File Snapshot</b>\n\n"
            "<blockquote>"
            "Send me a URL — I'll bundle the whole rendered page (CSS, images, fonts) "
            "into <b>one</b> self-contained .html file you can open offline, no folder needed."
            "</blockquote>"
        )
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=back_keyboard())

    elif query.data == "help":
        text = (
            '<tg-emoji emoji-id="6334834495379736183">📖</tg-emoji> '
            "<b>How To Use</b>\n\n"
            "<blockquote>"
            '<tg-emoji emoji-id="5305763715692377402">1️⃣</tg-emoji> '
            "Tap the <b>Convert URL</b> button\n"
            '<tg-emoji emoji-id="5307907239380528763">2️⃣</tg-emoji> '
            "Send the target website link (with http/https)\n"
            '<tg-emoji emoji-id="6241992364890527679">3️⃣</tg-emoji> '
            "The bot will automatically download the full page HTML "
            "and send it as a file "
            '<tg-emoji emoji-id="6100395724162210221">✅</tg-emoji>'
            "</blockquote>\n\n"
            '<tg-emoji emoji-id="6028090581094240774">🥸</tg-emoji> '
            "<b>Note:</b> Source may be limited for JS-heavy or login-protected sites."
        )
        await query.edit_message_text(
            text, parse_mode="HTML", reply_markup=back_keyboard()
        )

    elif query.data == "about":
        text = (
            '<tg-emoji emoji-id="6113971389935391397">👨‍💻</tg-emoji> '
            "<b>About This Bot</b>\n\n"
            "<blockquote>"
            '<tg-emoji emoji-id="6253414379442670769">✅</tg-emoji> '
            "<b>Built with:</b> python-telegram-bot + requests\n"
            '<tg-emoji emoji-id="6253414379442670769">✅</tg-emoji> '
            "<b>Function:</b> URL ➜ Full HTML source ➜ .html file\n"
            '<tg-emoji emoji-id="6253414379442670769">✅</tg-emoji> '
            "<b>Status:</b> "
            '<tg-emoji emoji-id="6332517244559430568">🟢</tg-emoji> '
            "Online &amp; Active"
            "</blockquote>\n\n"
            '<tg-emoji emoji-id="6253263677630188904">💡</tg-emoji>'
            '<tg-emoji emoji-id="6111809101535123919">🌩</tg-emoji>'
            '<tg-emoji emoji-id="6256026737465887830">💡</tg-emoji>'
            '<tg-emoji emoji-id="6255603215035797822">💡</tg-emoji>'
            '<tg-emoji emoji-id="6253795282912283121">💡</tg-emoji>'
            '<tg-emoji emoji-id="6255948350017768971">💡</tg-emoji>'
            '<tg-emoji emoji-id="6255801663999708585">💡</tg-emoji>\n\n'
            '<tg-emoji emoji-id="6082113830794567558">⚡️</tg-emoji> '
            "<i>Powered by your custom Telegram Bot</i>"
        )
        await query.edit_message_text(
            text, parse_mode="HTML", reply_markup=back_keyboard()
        )

    elif query.data == "check_join":
        if await check_force_join(update, context):
            await start(update, context)

    elif query.data == "back":
        context.user_data["mode"] = None
        await start(update, context)


# ═══════════════════════ URL VALIDATION ═══════════════════════
URL_PATTERN = re.compile(
    r"^https?://"
    r"(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+"
    r"[a-zA-Z]{2,}"
    r"(?::\d{1,5})?"
    r"(?:/[^\s]*)?$"
)


def is_valid_url(text: str) -> bool:
    """Strictly checks if text is a real, well-formed http(s) URL —
    not just plain text or a sentence that happens to contain 'http'."""
    if not text:
        return False
    text = text.strip()
    if " " in text or "\n" in text:
        return False
    if not URL_PATTERN.match(text):
        return False
    try:
        parsed = urlparse(text)
        if not parsed.scheme or not parsed.netloc:
            return False
        if "." not in parsed.netloc:
            return False
    except Exception:
        return False
    return True


# ═══════════════════════ API-BACKED HANDLERS ═══════════════════════
# The heavy work (fetching, crawling, ZIP packaging, screenshots, SEO/tech
# analysis) all runs on the url2src MAX API. The bot only starts jobs,
# shows live progress from the API's own event stream, and delivers results.
TELEGRAM_UPLOAD_LIMIT = 49 * 1024 * 1024
TELEGRAM_PHOTO_LIMIT = 9 * 1024 * 1024
FULL_SITE_MAX_PAGES = 15

# Options sent to the API per bot feature. engine=auto + speed=balanced lets
# the API pick the fastest path (plain HTTP mirror vs a real browser) on its
# own, and auto_unlock=1 clicks through simple "Continue / Enter / Unlock"
# gate pages by itself.
JOB_OPTS = {
    "full":       dict(crawl=1, max_pages=FULL_SITE_MAX_PAGES, depth=3, engine="auto",
                       speed="balanced", auto_unlock=1, assets=1, screenshot=1, snapshot=0, sourcemaps=0),
    "single":     dict(crawl=0, engine="auto", speed="balanced", auto_unlock=1,
                       assets=1, screenshot=1, snapshot=1, sourcemaps=1),
    "screenshot": dict(crawl=0, engine="auto", speed="balanced", auto_unlock=1,
                       assets=0, screenshot=1, snapshot=0, sourcemaps=0),
    "snapshot":   dict(crawl=0, engine="auto", speed="balanced", auto_unlock=1,
                       assets=1, screenshot=0, snapshot=1, sourcemaps=0),
    "seo":        dict(crawl=0, engine="auto", speed="turbo", auto_unlock=1,
                       assets=0, screenshot=0, snapshot=0, sourcemaps=0),
    "tech":       dict(crawl=0, engine="auto", speed="turbo", auto_unlock=1,
                       assets=0, screenshot=0, snapshot=0, sourcemaps=0),
}

PHASE_LABEL = {
    "http_fetch": "🌐 Fetching page (fast mode, no browser needed)",
    "scrolling": "🖱 Scrolling to load lazy content",
    "collecting_assets": "🧩 Collecting CSS / JS / images / fonts",
    "crawling": "🕸 Crawling internal pages",
    "screenshot": "📸 Taking a screenshot",
    "packing": "📦 Packing the result",
    "fallback_to_browser": "🔁 Page needs JavaScript — switching to a full browser",
}


def _api_error_text(e: Exception) -> str:
    return html_lib.escape(str(e)[:300])


def _host_of(url: str) -> str:
    try:
        return (urlparse(url).hostname or "site").replace(".", "_")
    except Exception:
        return "site"


async def _reject_invalid_url(update: Update) -> bool:
    url = update.message.text.strip()
    if is_valid_url(url):
        return False
    await update.message.reply_text(
        "⚠️ <b>Invalid URL!</b> Please send a link starting with http:// or https://",
        parse_mode="HTML",
        reply_markup=back_keyboard(),
    )
    return True


async def _api_not_configured(update: Update) -> bool:
    if api_client.configured():
        return False
    await update.message.reply_text(
        "🛠 <b>Service is being set up.</b>\n"
        "<blockquote>The processing server is not connected yet. Please try again later.</blockquote>",
        parse_mode="HTML",
        reply_markup=back_keyboard(),
    )
    print("❌ API_BASE_URL / API_KEY missing in config.py")
    return True


def _render_progress(state: dict) -> str:
    tick = state.get("tick")
    lines = []
    if tick:
        lines.append(f"⏱ <b>Elapsed:</b> {tick.get('elapsed_s', '?')}s")
    lines.append(PHASE_LABEL.get(state.get("phase"), "⏳ Working..."))
    if state.get("loader_open"):
        lines.append(f"🕳 <b>Loading screen visible:</b> <code>{html_lib.escape(state['loader_open'])}</code>")
    elif tick and tick.get("loading_screen_total_ms"):
        lines.append(f"✅ <b>Loading screen closed</b> — {tick['loading_screen_total_ms']}ms so far")
    if state.get("unlocks"):
        chain = " ➜ ".join(html_lib.escape(x) for x in state["unlocks"])
        lines.append(f"🔓 <b>Auto-clicked through:</b> {chain}")
    if state.get("pages"):
        lines.append(f"📄 <b>Pages captured:</b> {len(state['pages'])}")
    if tick:
        lines.append(f"🧩 <b>Files captured so far:</b> {tick.get('resources_captured', 0)}")
        if tick.get("requests_in_flight"):
            lines.append(f"📡 <b>Requests still loading:</b> {tick['requests_in_flight']}")
    return "<blockquote>" + "\n".join(lines) + "</blockquote>"


def _format_result_summary(result: dict) -> str:
    t = result.get("timings") or {}
    lines = [
        f"⚙️ <b>Engine:</b> {t.get('engine', '?')}  |  🏁 <b>Status:</b> {t.get('engine') and result.get('finish_reason', '?')}",
        f"⏱ <b>Total time:</b> {t.get('total_ms', '?')} ms",
    ]
    if t.get("loading_screen_detected"):
        lines.append(f"🕳 <b>Loading screen measured:</b> {t.get('loading_screen_total_ms', 0)} ms")
    if result.get("blocked"):
        lines.append("⚠️ <b>Heads up:</b> the site may be bot-protected — result could be incomplete")
    if result.get("auto_unlock_clicks"):
        chain = " ➜ ".join(html_lib.escape(x) for x in result["auto_unlock_clicks"])
        lines.append(f"🔓 <b>Auto-clicked through:</b> {chain}")
    tech = result.get("tech") or []
    if tech:
        lines.append(f"🧩 <b>Tech detected:</b> {html_lib.escape(', '.join(tech[:6]))}")
    pages = result.get("pages") or []
    if len(pages) > 1:
        lines.append(f"📄 <b>Pages captured:</b> {len(pages)}")
        for p in pages[:6]:
            path = urlparse(p.get("url", "")).path or "/"
            loader = f", loader {p['loader_ms']}ms" if p.get("loader_ms") else ""
            lines.append(f"   • <code>{html_lib.escape(path[:40])}</code> — {p.get('status')} — {p.get('total_ms')}ms{loader}")
        if len(pages) > 6:
            lines.append(f"   …and {len(pages) - 6} more")
    res = result.get("resources") or {}
    if res:
        by = res.get("by_type") or {}
        by_str = ", ".join(f"{k}:{v}" for k, v in sorted(by.items(), key=lambda x: -x[1])[:6])
        tail = f" ({by_str})" if by_str else ""
        lines.append(f"🧩 <b>Assets bundled:</b> {res.get('count', 0)}{tail}")
    if t.get("dynamic_hint"):
        lines.append(f"ℹ️ <i>{html_lib.escape(str(t['dynamic_hint']))}</i>")
    return "\n".join(lines)


async def _run_job_with_live_status(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                    header: str, kind: str, log_label: str):
    """Starts a job on the API, live-edits the status message with real
    progress (phase, loading-screen state, pages, files captured) from the
    API's own event stream, then returns (status_msg, result) once done —
    or None (already reported) on failure."""
    if await _reject_invalid_url(update) or await _api_not_configured(update):
        return None
    url = update.message.text.strip()
    user = update.effective_user
    username_part = f"@{user.username}" if user.username else "no username"

    status_msg = await update.message.reply_text(
        f"{header}\n<blockquote>Sending your request to the server...</blockquote>",
        parse_mode="HTML",
    )
    job_start = time.time()
    await send_log(
        context,
        f"🚀 <b>{log_label} STARTED</b>\n\n"
        f"👤 <b>User:</b> {user.first_name} ({username_part})\n"
        f"🆔 <b>ID:</b> <code>{user.id}</code>\n"
        f"🔗 <b>URL:</b> <code>{html_lib.escape(url)}</code>",
    )

    async def fail(text: str):
        await status_msg.edit_text(f"❌ {text}", parse_mode="HTML", reply_markup=result_keyboard())
        await send_log(
            context,
            f"❌ <b>{log_label} FAILED</b>\n\n"
            f"👤 <b>User:</b> {user.first_name} ({username_part}) | <code>{user.id}</code>\n"
            f"🔗 <b>URL:</b> <code>{html_lib.escape(url)}</code>\n"
            f"⚠️ <b>Error:</b> <code>{text[:300]}</code>",
        )

    try:
        job = await asyncio.to_thread(api_client.start_job, url, **JOB_OPTS[kind])
    except Exception as e:
        await fail(f"<b>Could not start the job:</b> <code>{_api_error_text(e)}</code>")
        return None
    job_id = job["job_id"]

    since, errors = 0, 0
    state = {"phase": "queued", "tick": None, "pages": [], "unlocks": [], "loader_open": None}
    last_text, last_edit = "", 0.0
    while True:
        await asyncio.sleep(1.5)
        try:
            st = await asyncio.to_thread(api_client.job_status, job_id, since)
            errors = 0
        except Exception as e:
            if getattr(e, "status", None) == 404:
                await fail("<b>Job expired or was lost.</b> Please send the URL again.")
                return None
            errors += 1
            if errors >= 8:
                await fail(f"<b>Lost connection to the server:</b> <code>{_api_error_text(e)}</code>")
                return None
            continue

        events = st.get("events") or []
        since += len(events)
        for ev in events:
            et = ev.get("type")
            if et == "tick":
                state["tick"] = ev
            elif et == "phase":
                state["phase"] = ev.get("phase")
            elif et == "page":
                state["pages"].append(ev)
            elif et == "auto_unlock_click":
                state["unlocks"].append(ev.get("text", ""))
            elif et == "loader_start":
                state["loader_open"] = ev.get("selector")
            elif et in ("loader_end", "loader_ignored"):
                state["loader_open"] = None

        if st["status"] == "error":
            await fail(f"<b>Server could not process this site:</b> <code>{_api_error_text(st.get('error') or 'unknown error')}</code>")
            return None
        if st["status"] == "done":
            return status_msg, st["result"], job_id, job_start, log_label, user, username_part

        text = f"{header}\n{_render_progress(state)}"
        now = time.time()
        if text != last_text and now - last_edit >= 1.5:
            last_text, last_edit = text, now
            try:
                await status_msg.edit_text(text, parse_mode="HTML")
            except Exception:
                pass


async def handle_seo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    out = await _run_job_with_live_status(update, context, "🔎 <b>Analyzing page...</b>", "seo", "SEO Report")
    if not out:
        return
    status_msg, result, job_id, job_start, log_label, user, username_part = out
    url = update.message.text.strip()

    try:
        meta_buf = await asyncio.to_thread(api_client.download_file, job_id, "meta.json", 2 * 1024 * 1024)
        meta = json.loads(meta_buf.read().decode("utf-8"))
    except Exception as e:
        meta = {}
        print(f"⚠️ could not fetch meta.json: {e}")

    m = meta.get("meta") or {}
    heads = meta.get("headings") or {}
    counts = meta.get("counts") or {}
    heading_line = "  ".join(f"{k.upper()}:{len(v)}" for k, v in heads.items() if v) or "None found"
    size_kb = round((result.get("html_length") or 0) / 1024, 1)

    text = (
        "🔎 <b>SEO / Page Report</b>\n\n"
        "<blockquote>"
        f"🔗 <b>URL:</b> <code>{html_lib.escape(url)}</code>\n"
        f"⏱ <b>Load Time:</b> {round((result.get('timings') or {}).get('total_ms', 0) / 1000, 2)}s\n"
        f"📦 <b>Page Size:</b> {size_kb} KB\n"
        f"🌎 <b>Status:</b> {result.get('status')}"
        "</blockquote>\n\n"
        f"📝 <b>Title:</b>\n<i>{html_lib.escape(m.get('title') or result.get('title') or '')}</i>\n\n"
        f"📋 <b>Meta Description:</b>\n<i>{html_lib.escape(m.get('description') or 'None found')}</i>\n\n"
        "<blockquote>"
        f"🔢 <b>Headings:</b> {heading_line}\n"
        f"🖼 <b>Images found:</b> {counts.get('images', '?')}\n"
        f"📝 <b>Forms found:</b> {counts.get('forms', '?')}\n"
        f"🔳 <b>Iframes found:</b> {counts.get('iframes', '?')}"
        "</blockquote>\n\n"
        f"{_format_result_summary(result)}"
    )
    await status_msg.edit_text(text, parse_mode="HTML", reply_markup=result_keyboard())

    duration = round(time.time() - job_start, 1)
    await log_activity(
        context, user, action="🔎 SEO Report", detail=url,
        result=f"Title: {(m.get('title') or '')[:80]} | Size: {size_kb} KB | Took: {duration}s",
    )


async def handle_tech(update: Update, context: ContextTypes.DEFAULT_TYPE):
    out = await _run_job_with_live_status(update, context, "⏳ <b>Detecting tech stack...</b>", "tech", "Tech Detect")
    if not out:
        return
    status_msg, result, job_id, job_start, log_label, user, username_part = out
    url = update.message.text.strip()

    detected = result.get("tech") or []
    detected_lines = "\n".join(f"• {html_lib.escape(item)}" for item in detected) or "No specific framework signatures found."
    text = (
        "🧩 <b>Tech Stack Detection</b>\n\n"
        f"🔗 <b>URL:</b> <code>{html_lib.escape(url)}</code>\n\n"
        f"<blockquote>{detected_lines}</blockquote>\n\n"
        f"{_format_result_summary(result)}\n\n"
        "<i>Detection is based on HTML/script signatures and may not be 100% accurate.</i>"
    )
    await status_msg.edit_text(text, parse_mode="HTML", reply_markup=result_keyboard())

    duration = round(time.time() - job_start, 1)
    await log_activity(context, user, action="🧩 Tech Stack Detection", detail=url,
                       result=", ".join(detected) + f" | Took: {duration}s")


async def _finish_with_document(update, context, status_msg, header, result, buf, filename, caption, action, url,
                                job_start, user, username_part):
    try:
        await status_msg.edit_text(
            f"{header}\n<blockquote>📥 Downloading the finished file...</blockquote>",
            parse_mode="HTML",
        )
    except Exception:
        pass
    await update.message.reply_document(
        document=buf, filename=filename, caption=caption, parse_mode="HTML",
        reply_markup=result_keyboard(),
    )
    try:
        await status_msg.delete()
    except Exception:
        pass
    BOT_DATA["stats"]["total_conversions"] = BOT_DATA["stats"].get("total_conversions", 0) + 1
    save_data(BOT_DATA)
    turso_db.set_kv("stats", BOT_DATA["stats"])
    duration = round(time.time() - job_start, 1)
    await log_activity(context, user, action=action, detail=url,
                       result=f"{_format_result_summary(result).splitlines()[1]} | Took: {duration}s")


async def _run_download_job(update: Update, context: ContextTypes.DEFAULT_TYPE, kind: str,
                            header: str, log_label: str, file_in_zip: str | None,
                            caption_title: str, action_label: str):
    """Shared flow for jobs whose result the user downloads as a file:
    kind='full'/'single' -> the whole ZIP; kind='screenshot'/'snapshot' ->
    one file pulled straight out of the job (no need to fetch the full ZIP)."""
    out = await _run_job_with_live_status(update, context, header, kind, log_label)
    if not out:
        return
    status_msg, result, job_id, job_start, _, user, username_part = out
    url = update.message.text.strip()

    # Full-page screenshot as a bonus preview photo, whenever one was made and
    # it isn't the main deliverable itself.
    if file_in_zip != "screenshot.jpg":
        try:
            shot = await asyncio.to_thread(api_client.download_file, job_id, "screenshot.jpg", TELEGRAM_PHOTO_LIMIT)
            await update.message.reply_photo(photo=shot, caption="📸 Preview")
        except Exception:
            pass

    try:
        if file_in_zip:
            limit = TELEGRAM_PHOTO_LIMIT if file_in_zip.endswith((".jpg", ".jpeg", ".png")) else TELEGRAM_UPLOAD_LIMIT
            buf = await asyncio.to_thread(api_client.download_file, job_id, file_in_zip, limit)
            ext = file_in_zip.rsplit(".", 1)[-1]
            filename = f"{_host_of(result.get('final_url') or url)}.{ext}"
        else:
            buf = await asyncio.to_thread(api_client.download_zip, job_id, TELEGRAM_UPLOAD_LIMIT)
            filename = f"{_host_of(result.get('final_url') or url)}.zip"
    except Exception as e:
        await status_msg.edit_text(
            f"❌ <b>Could not download the result:</b> <code>{_api_error_text(e)}</code>",
            parse_mode="HTML", reply_markup=result_keyboard(),
        )
        return

    caption = (
        f"<b>{caption_title}</b>\n\n"
        f"🔗 <b>URL:</b> <code>{html_lib.escape(url)}</code>\n\n"
        f"{_format_result_summary(result)}"
    )
    if file_in_zip == "screenshot.jpg":
        try:
            await status_msg.delete()
        except Exception:
            pass
        await update.message.reply_photo(photo=buf, caption=caption, parse_mode="HTML",
                                         reply_markup=result_keyboard())
        BOT_DATA["stats"]["total_conversions"] = BOT_DATA["stats"].get("total_conversions", 0) + 1
        save_data(BOT_DATA)
        turso_db.set_kv("stats", BOT_DATA["stats"])
        duration = round(time.time() - job_start, 1)
        await log_activity(context, user, action=action_label, detail=url,
                           result=f"Took: {duration}s")
        return

    await _finish_with_document(update, context, status_msg, header, result, buf, filename, caption, action_label,
                               url, job_start, user, username_part)


async def handle_fullsite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_download_job(
        update, context, "full",
        "🚀 <b>MAX MODE: Crawling full website...</b>", "Full Site Crawl", None,
        "💥 Full Website Downloaded!", "🚀 Full Website Crawl DONE",
    )


async def handle_zip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_download_job(
        update, context, "single",
        "⏳ <b>Building offline package...</b>", "Offline ZIP", None,
        "💥 Offline Package Ready!", "📦 Offline ZIP Package",
    )


async def handle_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_download_job(
        update, context, "screenshot",
        "📸 <b>Capturing screenshot...</b>", "Screenshot", "screenshot.jpg",
        "📸 Screenshot Ready!", "📸 Screenshot",
    )


async def handle_snapshot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _run_download_job(
        update, context, "snapshot",
        "🗜 <b>Building single-file snapshot...</b>", "Single-File Snapshot", "snapshot.html",
        "🗜 Single-File Snapshot Ready!", "🗜 Single-File Snapshot",
    )
    await _run_api_job(update, context, "zip")


# ═══════════════════════ ADMIN PANEL ═══════════════════════
async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text(
            "🚫 <b>Access Denied</b>\n\n"
            "<blockquote>You are not authorized to use the admin panel.</blockquote>",
            parse_mode="HTML",
        )
        return

    await update.message.reply_text(
        "🛠 <b>Admin Panel</b>\n\n"
        "<blockquote>Manage bot users, broadcast messages, "
        "and configure force-join channels.</blockquote>",
        parse_mode="HTML",
        reply_markup=admin_panel_keyboard(),
    )


async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user

    if not is_admin(user.id):
        await query.answer("🚫 Not authorized.", show_alert=True)
        return

    await query.answer()

    if query.data == "admin_panel":
        context.user_data["admin_mode"] = None
        await query.edit_message_text(
            "🛠 <b>Admin Panel</b>\n\n"
            "<blockquote>Manage bot users, broadcast messages, "
            "and configure force-join channels.</blockquote>",
            parse_mode="HTML",
            reply_markup=admin_panel_keyboard(),
        )

    elif query.data == "admin_stats":
        total_users = len(BOT_DATA["users"])
        total_conversions = BOT_DATA["stats"].get("total_conversions", 0)
        text = (
            "📊 <b>Bot Statistics</b>\n\n"
            "<blockquote>"
            f"👥 <b>Total Users:</b> {total_users}\n"
            f"🔁 <b>Total Conversions:</b> {total_conversions}\n"
            f"🔗 <b>Force-Join Channels:</b> {len(get_force_join_channels())}"
            "</blockquote>"
        )
        await query.edit_message_text(
            text, parse_mode="HTML", reply_markup=admin_back_keyboard()
        )

    elif query.data == "admin_users":
        users = list(BOT_DATA["users"].values())[-20:]
        if not users:
            lines = "No users yet."
        else:
            lines = "\n".join(
                f"• {u['first_name']} (@{u['username'] or 'N/A'}) — <code>{u['id']}</code>"
                for u in users
            )
        text = (
            "👥 <b>Recent Users (last 20)</b>\n\n"
            f"<blockquote>{lines}</blockquote>"
        )
        await query.edit_message_text(
            text, parse_mode="HTML", reply_markup=admin_back_keyboard()
        )

    elif query.data == "admin_broadcast":
        context.user_data["admin_mode"] = "broadcast"
        await query.edit_message_text(
            "📢 <b>Broadcast Message</b>\n\n"
            "<blockquote>Send me anything now — text (bold/italic/quote "
            "formatting works), a photo, video, sticker, GIF, voice note, "
            "document, or message with premium emoji.\n\n"
            "It will be sent to all registered users exactly as you sent it.</blockquote>",
            parse_mode="HTML",
            reply_markup=admin_back_keyboard(),
        )

    elif query.data == "admin_forcejoin":
        channels = get_force_join_channels()
        if channels:
            lines = "\n".join(
                f"• {ch['title']} (<code>{ch['chat_id']}</code>)"
                for ch in channels
            )
        else:
            lines = "No force-join channels configured."
        text = (
            "🔗 <b>Force Join Channels</b>\n\n"
            f"<blockquote>{lines}</blockquote>\n\n"
            "<i>Use ➕ Add Force Join to add a channel, or tap a ❌ below to remove one.</i>"
        )
        buttons = [
            [SBtn(f"❌ Remove: {ch['title']}", style="danger", callback_data=f"admin_rmfj_{i}")]
            for i, ch in enumerate(channels)
        ]
        buttons.append([SBtn("🔙 Back to Admin Panel", style="danger", callback_data="admin_panel")])
        await query.edit_message_text(
            text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif query.data.startswith("admin_rmfj_"):
        idx = int(query.data.replace("admin_rmfj_", ""))
        channels = get_force_join_channels()
        if 0 <= idx < len(channels):
            removed = channels.pop(idx)
            save_data(BOT_DATA)
            turso_db.set_kv("force_join_channels", BOT_DATA["force_join_channels"])
            await query.answer(f"Removed: {removed['title']}", show_alert=True)
        else:
            await query.answer("Already removed.", show_alert=True)
        # Re-render the updated list
        channels = get_force_join_channels()
        lines = "\n".join(
            f"• {ch['title']} (<code>{ch['chat_id']}</code>)" for ch in channels
        ) or "No force-join channels configured."
        text = (
            "🔗 <b>Force Join Channels</b>\n\n"
            f"<blockquote>{lines}</blockquote>\n\n"
            "<i>Use ➕ Add Force Join to add a channel, or tap a ❌ below to remove one.</i>"
        )
        buttons = [
            [SBtn(f"❌ Remove: {ch['title']}", style="danger", callback_data=f"admin_rmfj_{i}")]
            for i, ch in enumerate(channels)
        ]
        buttons.append([SBtn("🔙 Back to Admin Panel", style="danger", callback_data="admin_panel")])
        await query.edit_message_text(
            text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif query.data == "admin_addforcejoin":
        context.user_data["admin_mode"] = "addforcejoin_title"
        await query.edit_message_text(
            "➕ <b>Add Force Join Channel</b>\n\n"
            "<blockquote>Step 1/3: Send the <b>button title</b> that users will see.\n"
            "Example: <code>JOIN SHUVO</code></blockquote>",
            parse_mode="HTML",
            reply_markup=admin_back_keyboard(),
        )

    elif query.data == "admin_close":
        context.user_data["admin_mode"] = None
        await query.edit_message_text("🛠 Admin panel closed.")


async def handle_addforcejoin_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handles the multi-step Add Force Join flow."""
    user = update.effective_user
    if not is_admin(user.id):
        return False

    mode = context.user_data.get("admin_mode", "")

    if mode == "addforcejoin_title":
        # Step 1: got the button title
        context.user_data["fj_title"] = update.message.text.strip()
        context.user_data["admin_mode"] = "addforcejoin_link"
        await update.message.reply_text(
            "➕ <b>Add Force Join Channel</b>\n\n"
            "<blockquote>Step 2/3: Send the <b>channel invite link</b> (https://t.me/...).\n"
            "This will be the URL on the button.</blockquote>",
            parse_mode="HTML",
            reply_markup=admin_back_keyboard(),
        )
        return True

    elif mode == "addforcejoin_link":
        # Step 2: got the link
        context.user_data["fj_link"] = update.message.text.strip()
        context.user_data["admin_mode"] = "addforcejoin_chatid"
        await update.message.reply_text(
            "➕ <b>Add Force Join Channel</b>\n\n"
            "<blockquote>Step 3/3: Send the <b>channel username or chat ID</b>.\n"
            "Example: <code>@SHUVOMODS6</code> or <code>-1001234567890</code></blockquote>",
            parse_mode="HTML",
            reply_markup=admin_back_keyboard(),
        )
        return True

    elif mode == "addforcejoin_chatid":
        # Step 3: got the chat_id — add to list
        chat_id = _normalize_chat_id(update.message.text.strip())
        title = context.user_data.pop("fj_title", "Channel")
        link = context.user_data.pop("fj_link", "")
        context.user_data["admin_mode"] = None

        get_force_join_channels().append({
            "chat_id": chat_id,
            "title": title,
            "url": link,
        })
        save_data(BOT_DATA)
        turso_db.set_kv("force_join_channels", BOT_DATA["force_join_channels"])

        await update.message.reply_text(
            "✅ <b>Force Join Channel Added!</b>\n\n"
            f"<blockquote>📢 <b>Button:</b> {title}\n"
            f"🔗 <b>Link:</b> {link}\n"
            f"🆔 <b>Chat ID:</b> <code>{chat_id}</code></blockquote>\n\n"
            "⚠️ <b>Important:</b> make sure this bot is added as an "
            "<b>admin</b> in that channel/group, otherwise it can't "
            "verify whether users joined.",
            parse_mode="HTML",
            reply_markup=admin_panel_keyboard(),
        )
        return True

    return False


async def handle_broadcast_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Called when an admin is in broadcast mode and sends ANY message —
    text (with bold/italic/blockquote formatting), photo, video, sticker,
    GIF, voice note, document, or premium emoji. Uses copy_message so the
    exact content/formatting is replicated to every user, without a
    'Forwarded from' tag."""
    user = update.effective_user
    if not is_admin(user.id):
        return False

    if context.user_data.get("admin_mode") != "broadcast":
        return False

    context.user_data["admin_mode"] = None
    source_chat_id = update.effective_chat.id
    source_message_id = update.message.message_id

    status_msg = await update.message.reply_text(
        "📤 <b>Broadcasting...</b>", parse_mode="HTML"
    )

    sent, failed = 0, 0
    dead_uids = []
    uids = list(BOT_DATA["users"].keys())

    async def _send_one(uid):
        nonlocal sent, failed
        for _attempt in range(3):
            try:
                # copy_message preserves formatting, media and premium emoji
                await context.bot.copy_message(
                    chat_id=int(uid),
                    from_chat_id=source_chat_id,
                    message_id=source_message_id,
                )
                sent += 1
                return
            except RetryAfter as e:
                await asyncio.sleep(float(e.retry_after) + 1)
            except Exception as e:
                failed += 1
                err = str(e).lower()
                if "blocked" in err or "not found" in err or "deactivated" in err:
                    dead_uids.append(uid)
                return
        failed += 1

    # ~25 users per second in parallel (Telegram limit is ~30/sec)
    CHUNK = 25
    for i in range(0, len(uids), CHUNK):
        t0 = time.time()
        await asyncio.gather(*[_send_one(u) for u in uids[i:i + CHUNK]])
        elapsed = time.time() - t0
        if elapsed < 1.0:
            await asyncio.sleep(1.0 - elapsed)

    for uid in dead_uids:
        BOT_DATA["users"].pop(uid, None)
    if dead_uids:
        save_data(BOT_DATA)
        turso_db.delete_users(dead_uids)

    await status_msg.edit_text(
        "✅ <b>Broadcast Complete</b>\n\n"
        f"<blockquote>📨 Sent: {sent}\n❌ Failed: {failed}\n"
        f"🧹 Removed inactive: {len(dead_uids)}</blockquote>",
        parse_mode="HTML",
        reply_markup=admin_back_keyboard(),
    )

    await send_log(
        context,
        "📢 <b>Broadcast Sent</b>\n\n"
        f"👤 <b>By:</b> {user.first_name} (<code>{user.id}</code>)\n"
        f"📨 <b>Sent:</b> {sent} | ❌ <b>Failed:</b> {failed}",
    )
    return True



# ═══════════════════════ LOAD CONTROL ═══════════════════════
MAX_CONCURRENT_HEAVY_JOBS = 25     # jobs this bot polls at once (the API queues the real work)
HEAVY_JOB_TIMEOUT = 15 * 60        # seconds before a stuck job is killed
USER_COOLDOWN = 1.5                # seconds between a user's requests

HEAVY_SEM = asyncio.Semaphore(MAX_CONCURRENT_HEAVY_JOBS)
ACTIVE_JOBS: set = set()
_WAITING = 0
_USER_LAST: dict = {}


async def run_heavy(handler, update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Runs a heavy job with: one job per user, a global concurrency cap
    (others wait in a queue), and a hard timeout."""
    global _WAITING
    uid = update.effective_user.id

    if uid in ACTIVE_JOBS:
        await update.message.reply_text(
            "⏳ <b>You already have a job running.</b>\n"
            "<blockquote>Please wait for it to finish, then send the next URL.</blockquote>",
            parse_mode="HTML",
        )
        return

    ACTIVE_JOBS.add(uid)
    try:
        queued_msg = None
        if HEAVY_SEM.locked():
            _WAITING += 1
            position = _WAITING
            try:
                queued_msg = await update.message.reply_text(
                    "🕒 <b>Server is busy</b>\n"
                    f"<blockquote>You're in the queue (≈ #{position}). "
                    "Your job starts automatically.</blockquote>",
                    parse_mode="HTML",
                )
            except Exception:
                pass
            try:
                await HEAVY_SEM.acquire()
            finally:
                _WAITING -= 1
        else:
            await HEAVY_SEM.acquire()

        try:
            if queued_msg:
                try:
                    await queued_msg.delete()
                except Exception:
                    pass
            await asyncio.wait_for(handler(update, context), timeout=HEAVY_JOB_TIMEOUT)
        except asyncio.TimeoutError:
            try:
                await update.message.reply_text(
                    "⌛ <b>Job timed out.</b>\n"
                    "<blockquote>The site is too big or too slow. Try a smaller page.</blockquote>",
                    parse_mode="HTML",
                    reply_markup=result_keyboard(),
                )
            except Exception:
                pass
        finally:
            HEAVY_SEM.release()
    finally:
        ACTIVE_JOBS.discard(uid)


async def fallback_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    # Admin add-force-join flow takes priority (text-only steps)
    if update.message.text and await handle_addforcejoin_text(update, context):
        return

    # Admin broadcast takes priority — works for ANY message type
    # (formatted text, photo, video, sticker, GIF, premium emoji, etc.)
    if await handle_broadcast_text(update, context):
        return

    # Everything below this point only makes sense for plain text messages
    if not update.message.text:
        return

    text = update.message.text.strip()
    is_url = is_valid_url(text)
    mode = context.user_data.get("mode")

    # Ignore plain conversation if not a URL and no active mode
    if not is_url and not mode:
        return

    # Per-user cooldown against spam (admins exempt)
    uid_ = update.effective_user.id
    now_ = time.time()
    if not is_admin(uid_):
        if now_ - _USER_LAST.get(uid_, 0) < USER_COOLDOWN:
            return
    _USER_LAST[uid_] = now_

    register_user(update.effective_user)

    if not await check_force_join(update, context):
        return

    if mode == "seo":
        await handle_seo(update, context)
    elif mode == "tech":
        await handle_tech(update, context)
    elif mode == "html":
        # "Convert URL" now does a full-site conversion (all internal pages
        # + assets, bundled offline) instead of a single raw HTML file.
        await run_heavy(handle_fullsite, update, context)
    elif mode == "zip":
        await run_heavy(handle_zip, update, context)
    elif mode == "screenshot":
        await run_heavy(handle_screenshot, update, context)
    elif mode == "snapshot":
        await run_heavy(handle_snapshot, update, context)
    else:
        # default / MAX mode: crawl the entire site (all internal pages)
        await run_heavy(handle_fullsite, update, context)


# ═══════════════════════ MAIN ═══════════════════════
async def main():
    print("🔧 Initializing bot...")
    if not BOT_TOKEN or ":" not in BOT_TOKEN or BOT_TOKEN.startswith("PASTE_"):
        print("❌ Add a newly issued bot token to config.py before starting.")
        return

    # Quick connectivity check before starting polling, so failures are visible
    try:
        test_resp = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getMe", timeout=15
        )
        print(f"🔍 getMe check -> status {test_resp.status_code}: {test_resp.text}")
        if not test_resp.ok or not test_resp.json().get("ok"):
            print("❌ Bot token invalid or Telegram unreachable. Stopping here.")
            return
    except Exception as e:
        print(f"❌ Could not reach Telegram API: {e}")
        return

    if api_client.configured():
        print(f"🔌 API: {api_client.API_BASE_URL} (waking it up...)")
        print("   API reachable" if await asyncio.to_thread(api_client.wake) else "   ⚠️ API did not answer yet (it may still be starting)")
        api_client.start_keepalive()
    else:
        print("⚠️ API_BASE_URL / API_KEY not set in config.py — crawl/ZIP/SEO/tech will show 'service being set up'.")

    # Start Flask health server in background thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    print(f"🌐 Health server started on port {os.environ.get('PORT', 8080)}")

    # Bigger worker-thread pool: every blocking API call runs in a thread,
    # and the default pool is tiny (cpu+4) on small hosts like Render free.
    asyncio.get_running_loop().set_default_executor(
        ThreadPoolExecutor(max_workers=64, thread_name_prefix="worker")
    )

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(256)        # handle many users AT THE SAME TIME
        .connection_pool_size(256)
        .pool_timeout(30)
        .connect_timeout(30)
        .read_timeout(60)
        .write_timeout(180)             # big ZIP uploads
        .build()
    )

    async def _on_error(update, context):
        print(f"⚠️ Handler error: {context.error}")

    app.add_error_handler(_on_error)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(CallbackQueryHandler(admin_callback_handler, pattern=r"^admin_"))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, fallback_text))

    print("🤖 Bot is running... (press Ctrl+C to stop)")
    async with app:
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        # Keep running until interrupted
        try:
            await asyncio.Event().wait()
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
