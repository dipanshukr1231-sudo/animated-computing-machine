import asyncio
import html
import json
import logging
import os
import shutil
import signal
import sqlite3
import threading
import time
import traceback
import uuid
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiosqlite
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TelegramError, TimedOut
from telegram.ext import (
    AIORateLimiter,
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

TOKEN = os.environ.get("BOT_TOKEN", "").strip()
DATABASE_PATH = Path(os.environ.get("DATABASE_PATH", "bot.db")).expanduser().resolve()
BACKUP_DIR = Path(os.environ.get("BACKUP_DIR", "backups")).expanduser().resolve()
ACTIVE_DAYS = max(1, int(os.environ.get("ACTIVE_DAYS", "30")))
BROADCAST_WORKERS = max(1, min(20, int(os.environ.get("BROADCAST_WORKERS", "8"))))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
INITIAL_OWNER_IDS = {
    int(value.strip())
    for value in os.environ.get("OWNER_IDS", "8753914631").split(",")
    if value.strip().lstrip("-").isdigit()
}
# Backward-compatible first-run fallback: if OWNER_IDS is omitted, ADMIN_IDS become owners.
INITIAL_ADMIN_IDS = {
    int(value.strip())
    for value in os.environ.get("ADMIN_IDS", "").split(",")
    if value.strip().lstrip("-").isdigit()
}
if not INITIAL_OWNER_IDS:
    INITIAL_OWNER_IDS = set(INITIAL_ADMIN_IDS)
DEFAULT_START = os.environ.get("DEFAULT_START_MESSAGE", "Hello, Welcome to our bot!")
DEFAULT_MAINTENANCE = os.environ.get(
    "DEFAULT_MAINTENANCE_MESSAGE", "The bot is temporarily under maintenance. Please try again later."
)
WEB_HOST = os.environ.get("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.environ.get("PORT", os.environ.get("WEB_PORT", "10000")))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("livegram_bot")

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=10000;
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    language_code TEXT,
    is_banned INTEGER NOT NULL DEFAULT 0,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    last_seen TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_users_last_seen ON users(last_seen);
CREATE INDEX IF NOT EXISTS idx_users_banned ON users(is_banned);
CREATE TABLE IF NOT EXISTS owners (
    user_id INTEGER PRIMARY KEY,
    added_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS admins (
    user_id INTEGER PRIMARY KEY,
    added_at TEXT NOT NULL,
    added_by INTEGER
);
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS message_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    direction TEXT NOT NULL,
    user_id INTEGER,
    admin_id INTEGER,
    message_type TEXT NOT NULL,
    telegram_chat_id INTEGER,
    telegram_message_id INTEGER,
    related_message_id INTEGER,
    created_at TEXT NOT NULL,
    UNIQUE(direction, telegram_chat_id, telegram_message_id)
);
CREATE INDEX IF NOT EXISTS idx_message_log_user ON message_log(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_message_log_date ON message_log(created_at);
CREATE TABLE IF NOT EXISTS forward_map (
    admin_chat_id INTEGER NOT NULL,
    admin_message_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    user_message_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(admin_chat_id, admin_message_id)
);
CREATE TABLE IF NOT EXISTS processed_updates (
    update_id INTEGER PRIMARY KEY,
    processed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS broadcasts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_id INTEGER NOT NULL,
    source_chat_id INTEGER NOT NULL,
    source_message_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    total INTEGER NOT NULL DEFAULT 0,
    success INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    error TEXT
);
CREATE TABLE IF NOT EXISTS broadcast_deliveries (
    broadcast_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    error TEXT,
    delivered_at TEXT,
    PRIMARY KEY(broadcast_id, user_id),
    FOREIGN KEY(broadcast_id) REFERENCES broadcasts(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_broadcast_deliveries_status ON broadcast_deliveries(broadcast_id, status);
CREATE TABLE IF NOT EXISTS daily_stats (
    day TEXT PRIMARY KEY,
    new_users INTEGER NOT NULL DEFAULT 0,
    incoming INTEGER NOT NULL DEFAULT 0,
    outgoing INTEGER NOT NULL DEFAULT 0,
    admin_replies INTEGER NOT NULL DEFAULT 0,
    broadcast_success INTEGER NOT NULL DEFAULT 0,
    broadcast_failed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    context TEXT NOT NULL,
    error TEXT NOT NULL,
    traceback TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_errors_date ON errors(created_at DESC);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def esc(value: object) -> str:
    return html.escape(str(value or ""))


def human_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    if minutes or hours or days:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)


def message_kind(message) -> str:
    for name in (
        "text", "photo", "video", "document", "voice", "audio", "sticker",
        "animation", "video_note", "contact", "location", "venue", "poll", "dice"
    ):
        if getattr(message, name, None) is not None:
            return name
    return "other"


def owner_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛡 Manage Admins", callback_data="admins")],
        [InlineKeyboardButton("⚙️ Open Admin Panel", callback_data="panel")],
    ])


def main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Statistics", callback_data="stats"), InlineKeyboardButton("📣 Broadcast", callback_data="broadcast")],
        [InlineKeyboardButton("👥 Users", callback_data="users"), InlineKeyboardButton("✏️ Messages", callback_data="messages")],
        [InlineKeyboardButton("🛠 Maintenance", callback_data="maintenance"), InlineKeyboardButton("💾 Backup / Restore", callback_data="backup")],
        [InlineKeyboardButton("🩺 System", callback_data="system")],
    ])


def back_keyboard(target: str = "panel") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=target), InlineKeyboardButton("❌ Cancel", callback_data="cancel")]])


class Database:
    def __init__(self, path: Path):
        self.path = path
        self.write_lock = asyncio.Lock()

    @asynccontextmanager
    async def connect(self):
        db = await aiosqlite.connect(self.path, timeout=30)
        db.row_factory = aiosqlite.Row
        try:
            await db.execute("PRAGMA foreign_keys=ON")
            await db.execute("PRAGMA busy_timeout=10000")
            yield db
        finally:
            await db.close()

    async def init(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        async with self.connect() as db:
            await db.executescript(SCHEMA)
            now = utcnow()
            defaults = {
                "start_message": DEFAULT_START,
                "maintenance_mode": "0",
                "maintenance_message": DEFAULT_MAINTENANCE,
            }
            for key, value in defaults.items():
                await db.execute(
                    "INSERT OR IGNORE INTO settings(key,value,updated_at) VALUES(?,?,?)",
                    (key, value, now),
                )
            for owner_id in INITIAL_OWNER_IDS:
                await db.execute(
                    "INSERT OR IGNORE INTO owners(user_id,added_at) VALUES(?,?)",
                    (owner_id, now),
                )
            for admin_id in INITIAL_ADMIN_IDS - INITIAL_OWNER_IDS:
                await db.execute(
                    "INSERT OR IGNORE INTO admins(user_id,added_at,added_by) VALUES(?,?,?)",
                    (admin_id, now, next(iter(INITIAL_OWNER_IDS))),
                )
            await db.commit()

    async def execute(self, sql: str, params=()):
        async with self.write_lock:
            async with self.connect() as db:
                cur = await db.execute(sql, params)
                await db.commit()
                return cur.lastrowid, cur.rowcount

    async def executescript(self, sql: str):
        async with self.write_lock:
            async with self.connect() as db:
                await db.executescript(sql)
                await db.commit()

    async def fetchone(self, sql: str, params=()):
        async with self.connect() as db:
            async with db.execute(sql, params) as cur:
                return await cur.fetchone()

    async def fetchall(self, sql: str, params=()):
        async with self.connect() as db:
            async with db.execute(sql, params) as cur:
                return await cur.fetchall()

    @asynccontextmanager
    async def transaction(self):
        await self.write_lock.acquire()
        try:
            async with self.connect() as db:
                try:
                    await db.execute("BEGIN IMMEDIATE")
                    yield db
                    await db.commit()
                except Exception:
                    await db.rollback()
                    raise
        finally:
            self.write_lock.release()

    async def is_owner(self, user_id: int) -> bool:
        return bool(await self.fetchone("SELECT 1 FROM owners WHERE user_id=?", (user_id,)))

    async def is_admin(self, user_id: int) -> bool:
        return bool(await self.fetchone(
            "SELECT 1 FROM owners WHERE user_id=? UNION SELECT 1 FROM admins WHERE user_id=?",
            (user_id, user_id),
        ))

    async def owner_ids(self):
        return [r["user_id"] for r in await self.fetchall("SELECT user_id FROM owners ORDER BY added_at")]

    async def admin_ids(self):
        rows = await self.fetchall(
            "SELECT user_id, added_at FROM owners UNION ALL SELECT user_id, added_at FROM admins ORDER BY added_at"
        )
        return [r["user_id"] for r in rows]

    async def setting(self, key: str, default="") -> str:
        row = await self.fetchone("SELECT value FROM settings WHERE key=?", (key,))
        return row["value"] if row else default

    async def set_setting(self, key: str, value: str):
        await self.execute(
            "INSERT INTO settings(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, value, utcnow()),
        )

    async def claim_update(self, update_id: int) -> bool:
        if update_id is None:
            return True
        try:
            _, changed = await self.execute(
                "INSERT OR IGNORE INTO processed_updates(update_id,processed_at) VALUES(?,?)",
                (update_id, utcnow()),
            )
            return changed == 1
        except Exception:
            logger.exception("Could not claim update")
            return True

    async def upsert_user(self, user) -> bool:
        now = utcnow()
        async with self.transaction() as db:
            row = await (await db.execute("SELECT 1 FROM users WHERE user_id=?", (user.id,))).fetchone()
            is_new = row is None
            await db.execute(
                """INSERT INTO users(user_id,username,first_name,last_name,language_code,created_at,last_seen)
                VALUES(?,?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username, first_name=excluded.first_name, last_name=excluded.last_name,
                language_code=excluded.language_code, last_seen=excluded.last_seen, is_active=1""",
                (user.id, user.username, user.first_name, user.last_name, user.language_code, now, now),
            )
            if is_new:
                await self._daily_inc(db, "new_users")
            return is_new

    async def _daily_inc(self, db, column: str, amount: int = 1):
        allowed = {"new_users", "incoming", "outgoing", "admin_replies", "broadcast_success", "broadcast_failed"}
        if column not in allowed:
            raise ValueError("Invalid counter")
        await db.execute("INSERT OR IGNORE INTO daily_stats(day) VALUES(?)", (today(),))
        await db.execute(f"UPDATE daily_stats SET {column}={column}+? WHERE day=?", (amount, today()))

    async def log_message(self, direction: str, user_id: Optional[int], admin_id: Optional[int], message_type: str,
                          chat_id: Optional[int], message_id: Optional[int], related_message_id: Optional[int] = None) -> bool:
        async with self.transaction() as db:
            try:
                await db.execute(
                    """INSERT INTO message_log(direction,user_id,admin_id,message_type,telegram_chat_id,telegram_message_id,related_message_id,created_at)
                    VALUES(?,?,?,?,?,?,?,?)""",
                    (direction, user_id, admin_id, message_type, chat_id, message_id, related_message_id, utcnow()),
                )
            except sqlite3.IntegrityError:
                return False
            counter = {"incoming": "incoming", "outgoing": "outgoing", "admin_reply": "admin_replies"}.get(direction)
            if counter:
                await self._daily_inc(db, counter)
            if direction == "admin_reply":
                await self._daily_inc(db, "outgoing")
            return True

    async def mark_delivery(self, broadcast_id: int, user_id: int, success: bool, error: Optional[str]):
        status = "success" if success else "failed"
        async with self.transaction() as db:
            current = await (await db.execute(
                "SELECT status FROM broadcast_deliveries WHERE broadcast_id=? AND user_id=?", (broadcast_id, user_id)
            )).fetchone()
            if current and current["status"] == "success":
                return
            await db.execute(
                """INSERT INTO broadcast_deliveries(broadcast_id,user_id,status,error,delivered_at)
                VALUES(?,?,?,?,?) ON CONFLICT(broadcast_id,user_id) DO UPDATE SET
                status=excluded.status,error=excluded.error,delivered_at=excluded.delivered_at""",
                (broadcast_id, user_id, status, error, utcnow()),
            )
            await db.execute(
                f"UPDATE broadcasts SET {'success' if success else 'failed'}={'success' if success else 'failed'}+1 WHERE id=?",
                (broadcast_id,),
            )
            await self._daily_inc(db, "broadcast_success" if success else "broadcast_failed")
            if success:
                await self._daily_inc(db, "outgoing")

    async def record_error(self, context: str, error: BaseException):
        text = f"{type(error).__name__}: {error}"[:4000]
        tb = "".join(traceback.format_exception(type(error), error, error.__traceback__))[-12000:]
        try:
            await self.execute(
                "INSERT INTO errors(context,error,traceback,created_at) VALUES(?,?,?,?)",
                (context[:250], text, tb, utcnow()),
            )
        except Exception:
            logger.exception("Could not persist error")


db = Database(DATABASE_PATH)
START_MONOTONIC = time.monotonic()
BROADCAST_TASKS: dict[int, asyncio.Task] = {}


async def ensure_owner(update: Update) -> bool:
    user = update.effective_user
    if not user or not await db.is_owner(user.id):
        if update.callback_query:
            await update.callback_query.answer("Owner access required.", show_alert=True)
        elif update.effective_message:
            await update.effective_message.reply_text("Owner access required.")
        return False
    return True


async def ensure_admin(update: Update) -> bool:
    user = update.effective_user
    if not user or not await db.is_admin(user.id):
        if update.callback_query:
            await update.callback_query.answer("Not authorized.", show_alert=True)
        elif update.effective_message:
            await update.effective_message.reply_text("Not authorized.")
        return False
    return True


async def safe_edit(query, text: str, keyboard=None, parse_mode=ParseMode.HTML):
    try:
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode=parse_mode)
    except BadRequest as exc:
        if "Message is not modified" not in str(exc):
            raise


async def send_counted(bot, user_id: int, method: str, **kwargs):
    result = await getattr(bot, method)(chat_id=user_id, **kwargs)
    await db.log_message("outgoing", user_id, None, method, result.chat_id, result.message_id)
    return result


async def panel_text() -> str:
    total = (await db.fetchone("SELECT COUNT(*) n FROM users"))["n"]
    active = (await db.fetchone(
        "SELECT COUNT(*) n FROM users WHERE last_seen>=?",
        ((datetime.now(timezone.utc) - timedelta(days=ACTIVE_DAYS)).isoformat(timespec="seconds"),),
    ))["n"]
    return (
        "<b>⚙️ Administration Panel</b>\n\n"
        f"Users: <b>{total:,}</b>\n"
        f"Active ({ACTIVE_DAYS}d): <b>{active:,}</b>\n\n"
        "Choose a section:"
    )


async def owner_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_owner(update):
        return
    context.user_data.clear()
    owners = await db.owner_ids()
    admins = await db.fetchall("SELECT user_id FROM admins ORDER BY added_at")
    text = (
        "<b>👑 Owner Panel</b>\n\n"
        f"Owners: <b>{len(owners)}</b>\n"
        f"Managed admins: <b>{len(admins)}</b>\n\n"
        "Use this panel to add or remove administrators."
    )
    await update.effective_message.reply_text(text, reply_markup=owner_keyboard(), parse_mode=ParseMode.HTML)


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_admin(update):
        return
    context.user_data.clear()
    await update.effective_message.reply_text(await panel_text(), reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.effective_message:
        return
    await db.upsert_user(update.effective_user)
    if await db.is_admin(update.effective_user.id):
        await update.effective_message.reply_text(await panel_text(), reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)
        return
    banned = await db.fetchone("SELECT is_banned FROM users WHERE user_id=?", (update.effective_user.id,))
    if banned and banned["is_banned"]:
        return
    if await db.setting("maintenance_mode") == "1":
        text = await db.setting("maintenance_message", DEFAULT_MAINTENANCE)
    else:
        text = await db.setting("start_message", DEFAULT_START)
    sent = await update.effective_message.reply_text(text)
    await db.log_message("outgoing", update.effective_user.id, None, "start", sent.chat_id, sent.message_id)


async def statistics_text() -> str:
    now = datetime.now(timezone.utc)
    periods = {
        "Today": now.date().isoformat(),
        "7 days": (now - timedelta(days=6)).date().isoformat(),
        "30 days": (now - timedelta(days=29)).date().isoformat(),
    }
    user_row = await db.fetchone(
        "SELECT COUNT(*) total, SUM(CASE WHEN is_banned=1 THEN 1 ELSE 0 END) banned, "
        "SUM(CASE WHEN last_seen>=? THEN 1 ELSE 0 END) active FROM users",
        ((now - timedelta(days=ACTIVE_DAYS)).isoformat(timespec="seconds"),),
    )
    all_msg = await db.fetchone("""SELECT
        SUM(CASE WHEN direction='incoming' THEN 1 ELSE 0 END) incoming,
        SUM(CASE WHEN direction IN ('outgoing','admin_reply') THEN 1 ELSE 0 END) outgoing,
        SUM(CASE WHEN direction='admin_reply' THEN 1 ELSE 0 END) replies,
        COUNT(*) stored FROM message_log""")
    broadcasts = await db.fetchone(
        "SELECT COUNT(*) total, COALESCE(SUM(success),0) success, "
        "COALESCE(SUM(failed),0) failed FROM broadcasts"
    )
    lines = [
        "<b>📊 Advanced Statistics</b>", "",
        f"👥 Total users: <b>{user_row['total']:,}</b>",
        f"🟢 Active users ({ACTIVE_DAYS}d): <b>{(user_row['active'] or 0):,}</b>",
        f"🚫 Banned users: <b>{(user_row['banned'] or 0):,}</b>",
        f"📥 Messages received: <b>{(all_msg['incoming'] or 0):,}</b>",
        f"📤 Messages sent: <b>{(all_msg['outgoing'] or 0):,}</b>",
        f"💬 Admin replies: <b>{(all_msg['replies'] or 0):,}</b>",
        f"📣 Broadcasts: <b>{broadcasts['total']:,}</b>",
        f"✅ Broadcast deliveries: <b>{broadcasts['success']:,}</b>",
        f"❌ Failed deliveries: <b>{broadcasts['failed']:,}</b>", "",
        "<b>Period breakdown</b>",
    ]
    for label, start_day in periods.items():
        r = await db.fetchone("""SELECT COALESCE(SUM(new_users),0) new_users,
            COALESCE(SUM(incoming),0) incoming, COALESCE(SUM(outgoing),0) outgoing,
            COALESCE(SUM(broadcast_success),0) bsuccess
            FROM daily_stats WHERE day>=?""", (start_day,))
        lines.append(
            f"{label}: +{r['new_users']} users | ↓{r['incoming']} | "
            f"↑{r['outgoing']} | 📣{r['bsuccess']}"
        )
    db_size = DATABASE_PATH.stat().st_size if DATABASE_PATH.exists() else 0
    errors = (await db.fetchone("SELECT COUNT(*) n FROM errors"))["n"]
    lines += [
        "", "<b>System</b>",
        f"⏱ Uptime: <b>{human_duration(time.monotonic() - START_MONOTONIC)}</b>",
        f"🗃 Stored users/messages: <b>{user_row['total']:,} / {all_msg['stored']:,}</b>",
        f"💾 Database size: <b>{db_size / 1024 / 1024:.2f} MB</b>",
        f"⚠️ Logged errors: <b>{errors:,}</b>",
    ]
    return "\n".join(lines)


async def users_text() -> str:
    row = await db.fetchone("""SELECT COUNT(*) total,
        SUM(CASE WHEN is_banned=1 THEN 1 ELSE 0 END) banned,
        SUM(CASE WHEN is_active=1 THEN 1 ELSE 0 END) reachable FROM users""")
    recent = await db.fetchall(
        "SELECT user_id,username,first_name,last_seen,is_banned "
        "FROM users ORDER BY last_seen DESC LIMIT 8"
    )
    lines = [
        "<b>👥 User Management</b>", "",
        f"Total: <b>{row['total']}</b> | Reachable: <b>{row['reachable'] or 0}</b> | "
        f"Banned: <b>{row['banned'] or 0}</b>",
        "", "<b>Recently active</b>",
    ]
    for r in recent:
        label = f"@{r['username']}" if r['username'] else (r['first_name'] or "Unknown")
        lines.append(f"{'🚫' if r['is_banned'] else '👤'} <code>{r['user_id']}</code> - {esc(label)}")
    return "\n".join(lines)


async def user_detail_text(user_id: int) -> Optional[str]:
    u = await db.fetchone("SELECT * FROM users WHERE user_id=?", (user_id,))
    if not u:
        return None
    m = await db.fetchone("""SELECT COUNT(*) total,
        SUM(CASE WHEN direction='incoming' THEN 1 ELSE 0 END) incoming,
        SUM(CASE WHEN direction IN ('outgoing','admin_reply') THEN 1 ELSE 0 END) outgoing
        FROM message_log WHERE user_id=?""", (user_id,))
    return (
        "<b>👤 User Details</b>\n\n"
        f"ID: <code>{u['user_id']}</code>\n"
        f"Name: {esc((u['first_name'] or '') + ' ' + (u['last_name'] or ''))}\n"
        f"Username: {('@' + esc(u['username'])) if u['username'] else '-'}\n"
        f"Language: {esc(u['language_code'] or '-')}\n"
        f"Status: <b>{'Banned' if u['is_banned'] else 'Active'}</b>\n"
        f"Joined: {esc(u['created_at'])}\n"
        f"Last seen: {esc(u['last_seen'])}\n"
        f"Messages: {m['total']} ({m['incoming'] or 0} in / {m['outgoing'] or 0} out)"
    )


async def backup_database() -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    db_copy = BACKUP_DIR / f"bot_{stamp}.db"
    archive = BACKUP_DIR / f"bot_backup_{stamp}.zip"
    async with db.write_lock:
        source = sqlite3.connect(DATABASE_PATH)
        target = sqlite3.connect(db_copy)
        try:
            source.backup(target)
        finally:
            target.close()
            source.close()
    manifest = {"created_at": utcnow(), "schema": 1, "database": db_copy.name}
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(db_copy, db_copy.name)
        zf.writestr("manifest.json", json.dumps(manifest, indent=2))
    db_copy.unlink(missing_ok=True)
    return archive


async def restore_database(archive_path: Path):
    temp_dir = BACKUP_DIR / f"restore_{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(archive_path) as zf:
            names = zf.namelist()
            manifest = json.loads(zf.read("manifest.json"))
            db_name = manifest.get("database")
            if not db_name or db_name not in names or Path(db_name).name != db_name:
                raise ValueError("Invalid backup manifest")
            zf.extract(db_name, temp_dir)
        candidate = temp_dir / db_name
        check = sqlite3.connect(candidate)
        try:
            integrity = check.execute("PRAGMA integrity_check").fetchone()[0]
            tables = {r[0] for r in check.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            required = {"users", "admins", "settings", "message_log", "broadcasts"}
            if integrity != "ok" or not required.issubset(tables):
                raise ValueError("Backup database failed validation")
        finally:
            check.close()
        safety = await backup_database()
        async with db.write_lock:
            shutil.copy2(candidate, DATABASE_PATH)
        return safety
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.callback_query:
        return
    q = update.callback_query
    data = q.data
    owner_only = data in {"admins", "admin_add", "admin_remove"}
    if owner_only:
        if not await ensure_owner(update):
            return
    elif not await ensure_admin(update):
        return
    await q.answer()
    if data in {"panel", "cancel"}:
        context.user_data.clear()
        await safe_edit(q, await panel_text(), main_keyboard())
    elif data == "stats":
        await safe_edit(q, await statistics_text(), InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh Statistics", callback_data="stats")],
            [InlineKeyboardButton("⬅️ Back", callback_data="panel")],
        ]))
    elif data == "users":
        context.user_data.clear()
        await safe_edit(q, await users_text(), InlineKeyboardMarkup([
            [InlineKeyboardButton("🔎 Search", callback_data="user_search"), InlineKeyboardButton("🚫 Ban / Unban", callback_data="ban_prompt")],
            [InlineKeyboardButton("📤 Export Users", callback_data="export_users")],
            [InlineKeyboardButton("⬅️ Back", callback_data="panel")],
        ]))
    elif data == "user_search":
        context.user_data["state"] = "user_search"
        await safe_edit(q, "<b>🔎 Search Users</b>\n\nSend a Telegram ID or exact username.", back_keyboard("users"))
    elif data == "ban_prompt":
        context.user_data["state"] = "ban_target"
        await safe_edit(q, "<b>🚫 Ban / Unban User</b>\n\nSend the user's Telegram ID.", back_keyboard("users"))
    elif data.startswith("toggle_ban:"):
        uid = int(data.split(":", 1)[1])
        u = await db.fetchone("SELECT is_banned FROM users WHERE user_id=?", (uid,))
        if not u:
            await q.answer("User not found.", show_alert=True)
            return
        new = 0 if u["is_banned"] else 1
        await db.execute("UPDATE users SET is_banned=? WHERE user_id=?", (new, uid))
        detail = await user_detail_text(uid)
        await safe_edit(q, detail, InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Unban" if new else "🚫 Ban", callback_data=f"toggle_ban:{uid}")],
            [InlineKeyboardButton("⬅️ Back", callback_data="users")],
        ]))
    elif data == "export_users":
        rows = await db.fetchall("SELECT * FROM users ORDER BY created_at")
        path = BACKUP_DIR / f"users_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        import csv
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(rows[0].keys() if rows else ["user_id","username","first_name","last_name","language_code","is_banned","is_active","created_at","last_seen"])
            writer.writerows([tuple(r) for r in rows])
        await context.bot.send_document(q.from_user.id, document=path.open("rb"), caption=f"Exported {len(rows)} users.")
        path.unlink(missing_ok=True)
    elif data == "messages":
        await safe_edit(q, "<b>✏️ Configurable Messages</b>\n\nChoose the message to edit.", InlineKeyboardMarkup([
            [InlineKeyboardButton("👋 Welcome Message", callback_data="edit_start")],
            [InlineKeyboardButton("🛠 Maintenance Notice", callback_data="edit_maintenance")],
            [InlineKeyboardButton("⬅️ Back", callback_data="panel")],
        ]))
    elif data in {"edit_start", "edit_maintenance"}:
        key = "start_message" if data == "edit_start" else "maintenance_message"
        context.user_data["state"] = key
        current = await db.setting(key)
        await safe_edit(q, f"<b>Edit Message</b>\n\nCurrent:\n<blockquote>{esc(current)}</blockquote>\n\nSend the new message.", back_keyboard("messages"))
    elif data == "maintenance":
        enabled = await db.setting("maintenance_mode") == "1"
        await safe_edit(q, f"<b>🛠 Maintenance Mode</b>\n\nStatus: <b>{'ON' if enabled else 'OFF'}</b>", InlineKeyboardMarkup([
            [InlineKeyboardButton("🔴 Disable" if enabled else "🟢 Enable", callback_data="maintenance_toggle")],
            [InlineKeyboardButton("✏️ Edit Notice", callback_data="edit_maintenance")],
            [InlineKeyboardButton("⬅️ Back", callback_data="panel")],
        ]))
    elif data == "maintenance_toggle":
        enabled = await db.setting("maintenance_mode") == "1"
        context.user_data["confirm_action"] = "maintenance_toggle"
        await safe_edit(q, f"Confirm turning maintenance mode <b>{'OFF' if enabled else 'ON'}</b>?", InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm", callback_data="confirm_maintenance"), InlineKeyboardButton("❌ Cancel", callback_data="maintenance")]
        ]))
    elif data == "confirm_maintenance":
        enabled = await db.setting("maintenance_mode") == "1"
        await db.set_setting("maintenance_mode", "0" if enabled else "1")
        await safe_edit(q, f"✅ Maintenance mode is now <b>{'OFF' if enabled else 'ON'}</b>.", back_keyboard("panel"))
    elif data == "admins":
        owners = await db.owner_ids()
        admins = [r["user_id"] for r in await db.fetchall("SELECT user_id FROM admins ORDER BY added_at")]
        lines = ["<b>🛡 Admin Management</b>", "", "<b>Owners</b>"]
        lines += [f"👑 <code>{x}</code>" for x in owners]
        lines += ["", "<b>Admins</b>"]
        lines += [f"🛡 <code>{x}</code>" for x in admins] or ["No managed admins."]
        await safe_edit(q, "\n".join(lines), InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Admin", callback_data="admin_add"), InlineKeyboardButton("➖ Remove Admin", callback_data="admin_remove")],
            [InlineKeyboardButton("⬅️ Back", callback_data="panel")],
        ]))
    elif data in {"admin_add", "admin_remove"}:
        context.user_data["state"] = data
        await safe_edit(q, f"<b>{'Add' if data == 'admin_add' else 'Remove'} Admin</b>\n\nSend the Telegram user ID.", back_keyboard("admins"))
    elif data == "broadcast":
        context.user_data["state"] = "broadcast_content"
        await safe_edit(q, "<b>📣 New Broadcast</b>\n\nSend or forward one message containing the text/media to broadcast. Formatting, captions, premium emoji entities, and supported media are preserved by Telegram's copy operation.", back_keyboard("panel"))
    elif data == "broadcast_confirm":
        payload = context.user_data.get("broadcast_payload")
        if not payload:
            await safe_edit(q, "The broadcast draft expired.", back_keyboard("panel"))
            return
        await safe_edit(q, "Preparing broadcast...", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Panel", callback_data="panel")]]))
        bid, _ = await db.execute(
            "INSERT INTO broadcasts(admin_id,source_chat_id,source_message_id,status,started_at) VALUES(?,?,?,?,?)",
            (q.from_user.id, payload["chat_id"], payload["message_id"], "queued", utcnow()),
        )
        task = context.application.create_task(run_broadcast(context.application, bid, payload["chat_id"], payload["message_id"], q.from_user.id, q.message.chat_id, q.message.message_id))
        BROADCAST_TASKS[bid] = task
        context.user_data.clear()
    elif data == "broadcast_discard":
        context.user_data.clear()
        await safe_edit(q, "Broadcast cancelled before sending.", back_keyboard("panel"))
    elif data.startswith("broadcast_cancel:"):
        bid = int(data.split(":", 1)[1])
        await db.execute("UPDATE broadcasts SET status='cancelling' WHERE id=? AND status IN ('queued','running')", (bid,))
        await q.answer("Cancellation requested.", show_alert=True)
    elif data == "backup":
        recent = await db.fetchall("SELECT id,status,success,failed,started_at FROM broadcasts ORDER BY id DESC LIMIT 3")
        suffix = "\n".join(f"#{r['id']} {r['status']} ✅{r['success']} ❌{r['failed']}" for r in recent) or "No broadcasts yet."
        await safe_edit(q, f"<b>💾 Backup / Restore</b>\n\nRecent broadcasts:\n{suffix}", InlineKeyboardMarkup([
            [InlineKeyboardButton("💾 Create Backup", callback_data="backup_create")],
            [InlineKeyboardButton("♻️ Restore Backup", callback_data="restore_prompt")],
            [InlineKeyboardButton("⬅️ Back", callback_data="panel")],
        ]))
    elif data == "backup_create":
        await safe_edit(q, "Creating a consistent backup...", back_keyboard("backup"))
        path = await backup_database()
        with path.open("rb") as f:
            await context.bot.send_document(q.from_user.id, document=f, caption="✅ Database and settings backup")
        await safe_edit(q, "✅ Backup created and sent.", back_keyboard("backup"))
    elif data == "restore_prompt":
        context.user_data["state"] = "restore_upload"
        await safe_edit(q, "<b>♻️ Restore Backup</b>\n\nUpload a backup ZIP created by this bot. It will be validated before replacement, and a safety backup will be created.", back_keyboard("backup"))
    elif data == "restore_confirm":
        path = context.user_data.get("restore_path")
        if not path or not Path(path).exists():
            await safe_edit(q, "The uploaded backup is no longer available.", back_keyboard("backup"))
            return
        await safe_edit(q, "Validating and restoring backup...", back_keyboard("backup"))
        safety = await restore_database(Path(path))
        Path(path).unlink(missing_ok=True)
        context.user_data.clear()
        await safe_edit(q, f"✅ Restore completed. Safety backup: <code>{esc(safety.name)}</code>\nRestart the process before further administrative changes.", None)
        context.application.stop_running()
    elif data == "system":
        errors = await db.fetchall("SELECT context,error,created_at FROM errors ORDER BY id DESC LIMIT 5")
        active_b = await db.fetchall("SELECT id,status,success,failed,total FROM broadcasts WHERE status IN ('queued','running','cancelling') ORDER BY id DESC LIMIT 5")
        lines = ["<b>🩺 System Status</b>", "", f"Uptime: <b>{human_duration(time.monotonic()-START_MONOTONIC)}</b>", f"Database: <b>{DATABASE_PATH.stat().st_size/1024/1024:.2f} MB</b>", f"Running broadcasts: <b>{len(active_b)}</b>", "", "<b>Recent errors</b>"]
        lines += [f"{esc(r['created_at'])} - {esc(r['context'])}: {esc(r['error'][:120])}" for r in errors] or ["No recorded errors."]
        await safe_edit(q, "\n".join(lines), InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="system")],
            [InlineKeyboardButton("⬅️ Back", callback_data="panel")],
        ]))


async def admin_state_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not await db.is_admin(update.effective_user.id):
        return False
    state = context.user_data.get("state")
    if state in {"admin_add", "admin_remove"} and not await db.is_owner(update.effective_user.id):
        context.user_data.clear()
        await update.effective_message.reply_text("Owner access required.")
        return True
    if not state:
        return False
    message = update.effective_message
    text = message.text.strip() if message.text else ""
    if state in {"start_message", "maintenance_message"}:
        if not text:
            await message.reply_text("Send a text message.", reply_markup=back_keyboard("messages"))
            return True
        await db.set_setting(state, text)
        context.user_data.clear()
        await message.reply_text("✅ Message updated.", reply_markup=back_keyboard("messages"))
    elif state == "user_search":
        if text.startswith("@"):
            row = await db.fetchone("SELECT user_id FROM users WHERE lower(username)=lower(?)", (text[1:],))
            uid = row["user_id"] if row else None
        else:
            uid = int(text) if text.lstrip("-").isdigit() else None
        detail = await user_detail_text(uid) if uid is not None else None
        if not detail:
            await message.reply_text("User not found. Send an exact ID or username.", reply_markup=back_keyboard("users"))
        else:
            context.user_data.clear()
            u = await db.fetchone("SELECT is_banned FROM users WHERE user_id=?", (uid,))
            await message.reply_text(detail, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Unban" if u["is_banned"] else "🚫 Ban", callback_data=f"toggle_ban:{uid}")],
                [InlineKeyboardButton("⬅️ Back", callback_data="users")],
            ]))
    elif state == "ban_target":
        if not text.lstrip("-").isdigit():
            await message.reply_text("Send a numeric Telegram ID.")
            return True
        uid = int(text)
        u = await db.fetchone("SELECT is_banned FROM users WHERE user_id=?", (uid,))
        if not u:
            await message.reply_text("User not found.", reply_markup=back_keyboard("users"))
        else:
            context.user_data.clear()
            await message.reply_text(f"Confirm {'unbanning' if u['is_banned'] else 'banning'} <code>{uid}</code>?", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Confirm", callback_data=f"toggle_ban:{uid}"), InlineKeyboardButton("❌ Cancel", callback_data="users")]
            ]))
    elif state in {"admin_add", "admin_remove"}:
        if not text.lstrip("-").isdigit():
            await message.reply_text("Send a numeric Telegram ID.")
            return True
        uid = int(text)
        if state == "admin_add":
            await db.execute("INSERT OR IGNORE INTO admins(user_id,added_at,added_by) VALUES(?,?,?)", (uid, utcnow(), update.effective_user.id))
            result = "✅ Admin added."
        else:
            if await db.is_owner(uid):
                await message.reply_text("Owners cannot be removed from the admin manager.", reply_markup=back_keyboard("admins"))
                return True
            await db.execute("DELETE FROM admins WHERE user_id=?", (uid,))
            result = "✅ Admin removed."
        context.user_data.clear()
        await message.reply_text(result, reply_markup=back_keyboard("admins"))
    elif state == "broadcast_content":
        context.user_data["broadcast_payload"] = {"chat_id": message.chat_id, "message_id": message.message_id}
        context.user_data.pop("state", None)
        count = (await db.fetchone("SELECT COUNT(*) n FROM users WHERE is_banned=0 AND is_active=1"))["n"]
        await message.reply_text(f"<b>Confirm Broadcast</b>\n\nRecipients: <b>{count:,}</b>\nThe broadcast runs in the background and can be cancelled.", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Start Broadcast", callback_data="broadcast_confirm")],
            [InlineKeyboardButton("❌ Discard", callback_data="broadcast_discard")],
        ]))
    elif state == "restore_upload":
        if not message.document or not (message.document.file_name or "").lower().endswith(".zip"):
            await message.reply_text("Upload a ZIP backup created by this bot.", reply_markup=back_keyboard("backup"))
            return True
        if message.document.file_size and message.document.file_size > 100 * 1024 * 1024:
            await message.reply_text("Backup is too large (maximum 100 MB).")
            return True
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        path = BACKUP_DIR / f"uploaded_{uuid.uuid4().hex}.zip"
        tg_file = await message.document.get_file()
        await tg_file.download_to_drive(path)
        try:
            with zipfile.ZipFile(path) as zf:
                if "manifest.json" not in zf.namelist():
                    raise ValueError
        except Exception:
            path.unlink(missing_ok=True)
            await message.reply_text("Invalid backup archive.", reply_markup=back_keyboard("backup"))
            return True
        context.user_data["restore_path"] = str(path)
        context.user_data.pop("state", None)
        await message.reply_text("⚠️ Restoring replaces current data. A safety backup will be created first. Confirm?", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("♻️ Restore Now", callback_data="restore_confirm"), InlineKeyboardButton("❌ Cancel", callback_data="cancel")]
        ]))
    return True


async def run_broadcast(application: Application, bid: int, source_chat: int, source_message: int, admin_id: int, progress_chat: int, progress_message: int):
    try:
        users = await db.fetchall("SELECT user_id FROM users WHERE is_banned=0 AND is_active=1 ORDER BY user_id")
        total = len(users)
        await db.execute("UPDATE broadcasts SET status='running',total=? WHERE id=?", (total, bid))
        queue = asyncio.Queue()
        for row in users:
            queue.put_nowait(row["user_id"])
        for _ in range(BROADCAST_WORKERS):
            queue.put_nowait(None)

        async def worker():
            while True:
                uid = await queue.get()
                try:
                    if uid is None:
                        return
                    status = await db.fetchone("SELECT status FROM broadcasts WHERE id=?", (bid,))
                    if not status or status["status"] == "cancelling":
                        return
                    existing = await db.fetchone("SELECT status FROM broadcast_deliveries WHERE broadcast_id=? AND user_id=?", (bid, uid))
                    if existing and existing["status"] == "success":
                        continue
                    try:
                        await application.bot.copy_message(chat_id=uid, from_chat_id=source_chat, message_id=source_message)
                        await db.mark_delivery(bid, uid, True, None)
                    except RetryAfter as exc:
                        await asyncio.sleep(float(exc.retry_after) + 0.5)
                        try:
                            await application.bot.copy_message(chat_id=uid, from_chat_id=source_chat, message_id=source_message)
                            await db.mark_delivery(bid, uid, True, None)
                        except Exception as retry_exc:
                            await db.mark_delivery(bid, uid, False, str(retry_exc)[:500])
                    except Forbidden as exc:
                        await db.execute("UPDATE users SET is_active=0 WHERE user_id=?", (uid,))
                        await db.mark_delivery(bid, uid, False, str(exc)[:500])
                    except (TimedOut, NetworkError) as exc:
                        await asyncio.sleep(2)
                        try:
                            await application.bot.copy_message(chat_id=uid, from_chat_id=source_chat, message_id=source_message)
                            await db.mark_delivery(bid, uid, True, None)
                        except Exception as retry_exc:
                            await db.mark_delivery(bid, uid, False, str(retry_exc)[:500])
                    except TelegramError as exc:
                        await db.mark_delivery(bid, uid, False, str(exc)[:500])
                finally:
                    queue.task_done()

        async def progress():
            while True:
                await asyncio.sleep(2)
                row = await db.fetchone("SELECT total,success,failed,status FROM broadcasts WHERE id=?", (bid,))
                done = row["success"] + row["failed"]
                remaining = max(0, row["total"] - done)
                text = f"<b>📣 Broadcast #{bid}</b>\n\nTotal: {row['total']:,}\n✅ Success: {row['success']:,}\n❌ Failed: {row['failed']:,}\n⏳ Remaining: {remaining:,}\nStatus: <b>{esc(row['status'])}</b>"
                try:
                    await application.bot.edit_message_text(text, chat_id=progress_chat, message_id=progress_message, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Cancel", callback_data=f"broadcast_cancel:{bid}")]]))
                except BadRequest as exc:
                    if "Message is not modified" not in str(exc):
                        logger.warning("Progress edit failed: %s", exc)
                if row["status"] not in {"queued", "running", "cancelling"}:
                    return

        ptask = asyncio.create_task(progress())
        workers = [asyncio.create_task(worker()) for _ in range(BROADCAST_WORKERS)]
        await asyncio.gather(*workers)
        row = await db.fetchone("SELECT status FROM broadcasts WHERE id=?", (bid,))
        final_status = "cancelled" if row and row["status"] == "cancelling" else "completed"
        await db.execute("UPDATE broadcasts SET status=?,finished_at=? WHERE id=?", (final_status, utcnow(), bid))
        await ptask
        row = await db.fetchone("SELECT total,success,failed,status FROM broadcasts WHERE id=?", (bid,))
        await application.bot.edit_message_text(
            f"<b>📣 Broadcast #{bid} {esc(row['status'])}</b>\n\nTotal: {row['total']:,}\n✅ Success: {row['success']:,}\n❌ Failed: {row['failed']:,}\nNot attempted: {max(0, row['total'] - row['success'] - row['failed']):,}",
            chat_id=progress_chat, message_id=progress_message, parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Panel", callback_data="panel")]])
        )
    except asyncio.CancelledError:
        await db.execute("UPDATE broadcasts SET status='interrupted',finished_at=? WHERE id=?", (utcnow(), bid))
        raise
    except Exception as exc:
        await db.record_error(f"broadcast:{bid}", exc)
        await db.execute("UPDATE broadcasts SET status='failed',finished_at=?,error=? WHERE id=?", (utcnow(), str(exc)[:1000], bid))
        try:
            await application.bot.edit_message_text(f"❌ Broadcast #{bid} failed: {esc(exc)}", chat_id=progress_chat, message_id=progress_message, parse_mode=ParseMode.HTML)
        except Exception:
            pass
    finally:
        BROADCAST_TASKS.pop(bid, None)


async def forward_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message:
        return
    if await admin_state_message(update, context):
        return
    if await db.is_admin(user.id):
        await admin_reply(update, context)
        return
    await db.upsert_user(user)
    row = await db.fetchone("SELECT is_banned FROM users WHERE user_id=?", (user.id,))
    if row and row["is_banned"]:
        return
    if await db.setting("maintenance_mode") == "1":
        sent = await message.reply_text(await db.setting("maintenance_message", DEFAULT_MAINTENANCE))
        await db.log_message("outgoing", user.id, None, "maintenance", sent.chat_id, sent.message_id)
        return
    if not await db.log_message("incoming", user.id, None, message_kind(message), message.chat_id, message.message_id):
        return
    admins = await db.admin_ids()
    name = " ".join(x for x in [user.first_name, user.last_name] if x) or "Unknown"
    username = f"@{user.username}" if user.username else "no username"
    header = f"👤 <b>{esc(name)}</b> ({esc(username)})\nID: <code>{user.id}</code>"
    delivered = 0
    for admin_id in admins:
        try:
            info = await context.bot.send_message(admin_id, header, parse_mode=ParseMode.HTML)
            copied = await context.bot.copy_message(admin_id, message.chat_id, message.message_id, reply_to_message_id=info.message_id)
            await db.execute(
                "INSERT OR REPLACE INTO forward_map(admin_chat_id,admin_message_id,user_id,user_message_id,created_at) VALUES(?,?,?,?,?)",
                (admin_id, info.message_id, user.id, message.message_id, utcnow()),
            )
            await db.execute(
                "INSERT OR REPLACE INTO forward_map(admin_chat_id,admin_message_id,user_id,user_message_id,created_at) VALUES(?,?,?,?,?)",
                (admin_id, copied.message_id, user.id, message.message_id, utcnow()),
            )
            delivered += 1
        except TelegramError as exc:
            await db.record_error(f"forward_to_admin:{admin_id}", exc)
    if delivered == 0:
        logger.error("Message from %s could not reach any admin", user.id)


async def admin_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message or not message.reply_to_message:
        return
    mapping = await db.fetchone(
        "SELECT user_id,user_message_id FROM forward_map WHERE admin_chat_id=? AND admin_message_id=?",
        (message.chat_id, message.reply_to_message.message_id),
    )
    if not mapping:
        return
    uid = mapping["user_id"]
    try:
        sent = await context.bot.copy_message(uid, message.chat_id, message.message_id)
        await db.log_message("admin_reply", uid, update.effective_user.id, message_kind(message), uid, sent.message_id, mapping["user_message_id"])
        await message.reply_text("✅ Delivered.")
    except Forbidden as exc:
        await db.execute("UPDATE users SET is_active=0 WHERE user_id=?", (uid,))
        await db.record_error(f"admin_reply:{uid}", exc)
        await message.reply_text("❌ Delivery failed: the user blocked or stopped the bot.")
    except TelegramError as exc:
        await db.record_error(f"admin_reply:{uid}", exc)
        await message.reply_text(f"❌ Delivery failed: {esc(exc)}", parse_mode=ParseMode.HTML)


async def message_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_message or not update.effective_user:
        return
    if not await db.claim_update(update.update_id):
        return
    await forward_user_message(update, context)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    exc = context.error or RuntimeError("Unknown error")
    logger.error("Unhandled error", exc_info=exc)
    await db.record_error("update_handler", exc)
    if isinstance(update, Update) and update.effective_message and update.effective_user and await db.is_admin(update.effective_user.id):
        try:
            await update.effective_message.reply_text("❌ The operation failed. Details were logged; no success was recorded.")
        except Exception:
            pass


async def post_init(application: Application):
    await db.init()
    await db.execute("UPDATE broadcasts SET status='interrupted',finished_at=? WHERE status IN ('queued','running','cancelling')", (utcnow(),))
    await db.execute("DELETE FROM processed_updates WHERE processed_at<?", ((datetime.now(timezone.utc)-timedelta(days=14)).isoformat(timespec="seconds"),))
    me = await application.bot.get_me()
    logger.info("Started @%s (%s)", me.username, me.id)


async def post_shutdown(application: Application):
    tasks = list(BROADCAST_TASKS.values())
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Shutdown complete")


def build_application() -> Application:
    if not TOKEN:
        raise RuntimeError("BOT_TOKEN environment variable is required")
    if not INITIAL_OWNER_IDS and not DATABASE_PATH.exists():
        raise RuntimeError("OWNER_IDS must contain at least one numeric Telegram ID on first startup")
    application = (
        ApplicationBuilder()
        .token(TOKEN)
        .rate_limiter(AIORateLimiter(max_retries=2))
        .concurrent_updates(32)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    application.add_handler(CommandHandler("start", start_command), group=0)
    application.add_handler(CommandHandler("dkboss", owner_command), group=0)
    application.add_handler(CommandHandler("admin", admin_command), group=0)
    application.add_handler(CallbackQueryHandler(callback_router), group=0)
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, message_router), group=1)
    application.add_error_handler(error_handler)
    return application


def start_health_server():
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in {"/", "/health", "/healthz"}:
                payload = json.dumps({
                    "status": "ok",
                    "service": "telegram-bot",
                    "uptime_seconds": int(time.monotonic() - START_MONOTONIC),
                }).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            logger.debug("Health server: " + format, *args)

    server = ThreadingHTTPServer((WEB_HOST, WEB_PORT), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, name="health-server", daemon=True)
    thread.start()
    logger.info("Health server listening on %s:%s", WEB_HOST, WEB_PORT)
    return server


def main():
    health_server = start_health_server()
    application = build_application()
    try:
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=False,
            close_loop=True,
            stop_signals=(signal.SIGINT, signal.SIGTERM),
        )
    finally:
        health_server.shutdown()
        health_server.server_close()


if __name__ == "__main__":
    main()
