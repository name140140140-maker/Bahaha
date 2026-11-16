"""
Telegram монітор v3: userbot (твій акаунт) + бот-сповіщувач. Дані - у Supabase, працює 24/7.

pip install telethon httpx     (Python 3.10+)

Потрібно заповнити SUPABASE_KEY (service_role ключ з Supabase -> Project Settings -> API).
Усе інше вже вписано; будь-що можна перекрити змінними середовища.

Перший запуск: напиши боту /reg, прийде код в Telegram, відправ його боту (1-2-3-4-5).
Сесія зберігається в Supabase, повторно вводити код не треба.
"""
import asyncio
import csv
import logging
import os
import re
import sys
from collections import Counter, OrderedDict, defaultdict, deque
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from telethon import TelegramClient, events, Button
from telethon.errors import (FloodWaitError, UsernameNotOccupiedError, UsernameInvalidError,
                             SessionPasswordNeededError, PhoneCodeInvalidError,
                             PhoneCodeExpiredError, PasswordHashInvalidError)
from telethon.sessions import StringSession
from telethon.tl.functions.account import UpdateProfileRequest
from telethon.tl.functions.messages import GetCommonChatsRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import UserStatusOnline, UserStatusOffline


def _env(k, d):
    return os.getenv(k) or d


# ===== КОНФІДЕНЦІЙНІ ДАНІ: лише зі змінних середовища (GitHub Secrets) =====
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])
PHONE = os.environ["PHONE"]
SUPABASE_URL = _env("SUPABASE_URL", "https://zrpwgavfploiaqrqpcvv.supabase.co")
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
# ==============================

TZ = ZoneInfo(os.getenv("TZ_NAME", "Europe/Kyiv"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
MERGE_GAP = int(os.getenv("MERGE_GAP", "90"))
DIGEST_HOUR = int(os.getenv("DIGEST_HOUR", "9"))
CACHE_MAX = int(os.getenv("CACHE_MAX", "20000"))      # скільки останніх вхідних тримати в пам'яті
MEDIA_KEEP = int(os.getenv("MEDIA_KEEP", "300"))      # скільки останніх файлів тримати на диску
MEDIA_MAX = int(os.getenv("MEDIA_MAX", str(30 * 1024 * 1024)))
MEDIA_DIR = Path("media")
MEDIA_DIR.mkdir(exist_ok=True)
for _f in MEDIA_DIR.iterdir():  # файли з минулого запуску не потрібні
    try:
        _f.unlink()
    except OSError:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[RotatingFileHandler("monitor.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8"),
              logging.StreamHandler()],
)
log = logging.getLogger("monitor")

user = None
bot = None
ME = None
AUTHED = False


# ---------- Supabase (PostgREST) ----------
class Supa:
    def __init__(self, url, key):
        headers = {"apikey": key}
        if not key.startswith("sb_"):  # нові ключі sb_secret_... - не JWT, лише apikey
            headers["Authorization"] = f"Bearer {key}"
        self.c = httpx.AsyncClient(base_url=url.rstrip("/") + "/rest/v1", timeout=30, headers=headers)

    async def _req(self, method, table, params=None, body=None, prefer=None):
        headers = {"Prefer": prefer} if prefer else None
        last = None
        for i in range(4):
            try:
                r = await self.c.request(method, f"/{table}", params=params, json=body, headers=headers)
            except httpx.TransportError as ex:
                last = ex
                await asyncio.sleep(1 + 2 * i)
                continue
            if r.status_code >= 500:
                last = RuntimeError(f"{r.status_code} {r.text[:200]}")
                await asyncio.sleep(1 + 2 * i)
                continue
            if r.status_code >= 400:
                raise RuntimeError(f"supabase {table}: {r.status_code} {r.text[:300]}")
            return r.json() if r.content else None
        raise RuntimeError(f"supabase {table}: {last}")

    async def select(self, table, params=None):
        return await self._req("GET", table, params) or []

    async def select_all(self, table, params=None, max_rows=50000):
        out, off = [], 0
        while off < max_rows:
            p = dict(params or {})
            p.update(limit="1000", offset=str(off))
            rows = await self.select(table, p)
            out.extend(rows)
            if len(rows) < 1000:
                break
            off += 1000
        return out

    async def insert(self, table, rows, returning=False, ignore_dups=False, merge=False, on_conflict=None):
        prefer = []
        if returning:
            prefer.append("return=representation")
        if ignore_dups:
            prefer.append("resolution=ignore-duplicates")
        if merge:
            prefer.append("resolution=merge-duplicates")
        params = {"on_conflict": on_conflict} if on_conflict else None
        return await self._req("POST", table, params, rows, ",".join(prefer) or None)

    async def update(self, table, filters, data):
        return await self._req("PATCH", table, filters, data)

    async def delete(self, table, filters):
        return await self._req("DELETE", table, filters)


sb = None


async def safe(coro, default=None):
    try:
        return await coro
    except Exception:
        log.exception("db error")
        return default


# ---------- стан в пам'яті (дзеркало Supabase) ----------
targets = {}      # user_id -> {"name", "notify"}
muted = set()
kv = {}           # налаштування + сесії
open_sess = {}    # user_id -> (session_id | None, start)
seen = {}         # user_id -> last seen (з poller)
status_lock = asyncio.Lock()


async def load_state():
    kv.clear()
    for r in await sb.select_all("tg_kv", {"order": "key"}):
        kv[r["key"]] = r["value"]
    targets.clear()
    for r in await sb.select_all("tg_targets", {"order": "user_id"}):
        targets[r["user_id"]] = {"name": r["name"], "notify": r["notify"]}
    muted.clear()
    muted.update(r["chat_id"] for r in await sb.select_all("tg_muted", {"order": "chat_id"}))
    open_sess.clear()
    for r in await sb.select_all("tg_sessions", {"stop_at": "is.null", "order": "id"}):
        open_sess[r["user_id"]] = (r["id"], pdt(r["start_at"]))


async def kv_set(key, value):
    kv[key] = str(value)
    await safe(sb.insert("tg_kv", [{"key": key, "value": str(value)}], merge=True, on_conflict="key"))


def get_setting(key, default=None):
    return kv.get(key, default)


# ---------- допоміжне ----------
def now():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def pdt(s):
    if isinstance(s, datetime):
        return s
    s = s.replace("Z", "+00:00").replace(" ", "T", 1)
    s = re.sub(r"\.(\d+)", lambda m: "." + (m.group(1) + "000000")[:6], s, count=1)
    d = datetime.fromisoformat(s)
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def fmt(dt):
    return dt.astimezone(TZ).strftime("%d.%m %H:%M:%S")


def fmt_t(dt):
    return dt.astimezone(TZ).strftime("%H:%M")


def dur(td):
    s = int(td.total_seconds())
    d, r = divmod(s, 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    if d:
        return f"{d}д {h}г {m}хв"
    return f"{h}г {m}хв {s}с" if h else f"{m}хв {s}с"


def display(u):
    name = " ".join(filter(None, [getattr(u, "first_name", None), getattr(u, "last_name", None)]))
    return name or getattr(u, "username", None) or str(u.id)


def media_kind(m):
    if m.photo: return "фото"
    if m.voice: return "голосове"
    if m.video_note: return "кружечок"
    if m.video: return "відео"
    if m.sticker: return "стікер"
    if m.audio: return "аудіо"
    if m.document: return "файл"
    return "медіа" if m.media else ""


def msg_text(m):
    kind = media_kind(m)
    text = m.raw_text or ""
    return f"[{kind}] {text}".strip() if kind else text


async def notify(text):
    try:
        await bot.send_message(OWNER_ID, text[:4000], link_preview=False)
    except Exception:
        log.exception("notify failed")


async def reply_long(e, text):
    chunk = ""
    for line in text.split("\n"):
        if len(chunk) + len(line) + 1 > 3900:
            await e.reply(chunk)
            chunk = ""
        chunk += line + "\n"
    if chunk.strip():
        await e.reply(chunk.rstrip())


def is_muted(chat_id):
    return chat_id in muted


_bg = set()


def spawn(coro):
    t = asyncio.create_task(coro)
    _bg.add(t)
    t.add_done_callback(_bg.discard)


# ---------- тимчасовий кеш (лише в пам'яті; в БД потрапляють тільки зміни та видалення) ----------
CACHE = OrderedDict()   # (chat_id, msg_id) -> dict
IDX = {}                # msg_id -> key
MEDIA_KEYS = deque()


def drop_media(v):
    p = v.get("media")
    if p:
        try:
            os.remove(p)
        except OSError:
            pass
        v["media"] = None


def cache_put(chat_id, msg_id, sender_id, name, text):
    key = (chat_id, msg_id)
    old = CACHE.get(key)
    CACHE[key] = {"sender": sender_id, "name": name, "text": text,
                  "media": old["media"] if old else None,
                  "date": old["date"] if old else iso(now())}
    CACHE.move_to_end(key)
    IDX[msg_id] = key
    while len(CACHE) > CACHE_MAX:
        k, v = CACHE.popitem(last=False)
        IDX.pop(k[1], None)
        drop_media(v)


async def cache_media(m, chat_id):
    try:
        if not (m.photo or m.document):
            return
        if m.document and (getattr(m.document, "size", 0) or 0) > MEDIA_MAX:
            return
        path = await m.download_media(file=str(MEDIA_DIR / f"{chat_id}_{m.id}"))
        key = (chat_id, m.id)
        v = CACHE.get(key)
        if path and v:
            v["media"] = str(path)
            MEDIA_KEYS.append(key)
            while len(MEDIA_KEYS) > MEDIA_KEEP:
                k = MEDIA_KEYS.popleft()
                if k in CACHE:
                    drop_media(CACHE[k])
        elif path:
            os.remove(path)
    except Exception:
        log.exception("cache_media failed")


# ---------- 1-2. зміни, видалення, подарунки ----------
def gift_info(action):
    kind = type(action).__name__.replace("MessageAction", "")
    g = getattr(action, "gift", None)
    title = getattr(g, "title", None) if g is not None else None
    stars = getattr(g, "stars", None) if g is not None else None
    if stars is None:
        stars = getattr(action, "stars", None)
    parts = []
    months = getattr(action, "months", None)
    if months:
        parts.append(f"Premium {months} міс.")
    amount = getattr(action, "amount", None)
    if amount and getattr(action, "currency", None):
        parts.append(f"{amount / 100:g} {action.currency}")
    num = getattr(g, "num", None) if g is not None else None
    if num:
        parts.append(f"#{num}")
    msg = getattr(getattr(action, "message", None), "text", None)
    if msg:
        parts.append(f"«{msg}»")
    return kind, (title or kind), (stars if isinstance(stars, int) else None), " · ".join(parts)


async def handle_gift(e, chat):
    kind, title, stars, details = gift_info(e.message.action)
    direction = "out" if e.out else "in"
    name = display(chat)
    await safe(sb.insert("tg_gifts", [{
        "chat_id": e.chat_id, "msg_id": e.id, "chat_name": name, "direction": direction,
        "kind": kind, "title": title, "stars": stars, "details": details,
        "msg_date": iso(e.message.date)}], ignore_dups=True, on_conflict="chat_id,msg_id"))
    price = f" ({stars} зірок)" if stars else ""
    extra = f"\n{details}" if details else ""
    head = f"Ви подарували {name}" if direction == "out" else f"Подарунок від {name}"
    await notify(f"{head}: {title}{price}{extra}")


async def on_new(e):
    chat = await e.get_chat()
    if getattr(chat, "bot", False):
        return
    m = e.message
    action = getattr(m, "action", None)
    if action is not None:
        if "Gift" in type(action).__name__:
            await handle_gift(e, chat)
        return
    if e.out:
        return
    cache_put(e.chat_id, e.id, e.sender_id, display(chat), msg_text(m))
    if m.media:
        spawn(cache_media(m, e.chat_id))


async def on_edit(e):
    if e.out:
        return
    new = msg_text(e.message)
    v = CACHE.get((e.chat_id, e.id))
    if not v:
        chat = await e.get_chat()
        if getattr(chat, "bot", False):
            return
        cache_put(e.chat_id, e.id, e.sender_id, display(chat), new)
        return
    old = v["text"]
    if old == new:
        return
    await safe(sb.insert("tg_edits", [{"chat_id": e.chat_id, "msg_id": e.id, "chat_name": v["name"],
                                       "old_text": old, "new_text": new}]))
    v["text"] = new
    if not is_muted(e.chat_id):
        await notify(f"{v['name']} змінив(ла) повідомлення\n\nБуло:\n{old}\n\nСтало:\n{new}")


async def on_delete(e):
    items = []
    for mid in e.deleted_ids:
        key = (e.chat_id, mid) if e.chat_id else None
        if key not in CACHE:
            key = IDX.get(mid)
        v = CACHE.pop(key, None) if key else None
        if not v:
            continue
        IDX.pop(mid, None)
        items.append((key[0], mid, v))
    if not items:
        return
    await safe(sb.insert("tg_deletions", [
        {"chat_id": cid, "msg_id": mid, "chat_name": v["name"], "text": v["text"], "msg_date": v["date"]}
        for cid, mid, v in items]))
    by_chat = {}
    for cid, _, v in items:
        by_chat.setdefault((cid, v["name"]), []).append(v)
    for (cid, name), rows in by_chat.items():
        if not is_muted(cid):
            lines = "\n".join(f"• ({fmt(pdt(v['date']))}) {v['text']}" for v in rows)
            await notify(f"{name} видалив(ла) повідомлень: {len(rows)}\n\n{lines}")
            for v in rows:
                p = v.get("media")
                if p and os.path.exists(p):
                    try:
                        await bot.send_file(OWNER_ID, p, caption=f"Файл із видаленого повідомлення від {name}")
                    except Exception:
                        log.exception("send media failed")
        for v in rows:
            drop_media(v)


# ---------- 3. моніторинг онлайну ----------
async def sess_last(uid):
    rows = await sb.select("tg_sessions", {"user_id": f"eq.{uid}", "order": "id.desc", "limit": "1"})
    if not rows:
        return None
    r = rows[0]
    return r["id"], pdt(r["start_at"]), (pdt(r["stop_at"]) if r["stop_at"] else None)


async def set_status(uid, online, last_seen=None):
    t = targets.get(uid)
    if not t:
        return
    async with status_lock:
        name = t["name"]
        loud = kv.get("notify_presence", "0") == "1" and t["notify"]
        row = open_sess.get(uid)
        if online and not row:
            last = await safe(sess_last(uid))
            if last and last[2] and (now() - last[2]).total_seconds() <= MERGE_GAP:
                # короткий розрив (фонова робота застосунку) - продовжуємо ту саму сесію
                await safe(sb.update("tg_sessions", {"id": f"eq.{last[0]}"}, {"stop_at": None}))
                open_sess[uid] = (last[0], last[1])
                return
            start = now()
            res = await safe(sb.insert("tg_sessions", [{"user_id": uid, "start_at": iso(start)}], returning=True))
            open_sess[uid] = (res[0]["id"] if res else None, start)
            if loud:
                await notify(f"{name} онлайн ({fmt(now())})")
        elif online is False and row:
            sid, start = row
            stop = last_seen or now()
            if stop.tzinfo is None:
                stop = stop.replace(tzinfo=timezone.utc)
            stop = max(stop, start)
            if sid is not None:
                await safe(sb.update("tg_sessions", {"id": f"eq.{sid}"}, {"stop_at": iso(stop)}))
            else:
                await safe(sb.insert("tg_sessions", [{"user_id": uid, "start_at": iso(start),
                                                      "stop_at": iso(stop)}]))
            open_sess.pop(uid, None)
            if loud:
                await notify(f"{name} вийшов(ла). Онлайн {dur(stop - start)} ({fmt(start)} - {fmt(stop)})")


async def on_presence(e):
    uid = e.sender_id
    if e.online:
        await set_status(uid, True)
    elif e.online is False:
        await set_status(uid, False, e.last_seen)


async def fetch_users(ids):
    """Пакетний запит статусів; при помилці - по одному."""
    out = []
    for i in range(0, len(ids), 100):
        chunk = ids[i:i + 100]
        try:
            res = await user.get_entity(chunk)
            out.extend(res if isinstance(res, list) else [res])
        except FloodWaitError:
            raise
        except Exception:
            for uid in chunk:
                try:
                    out.append(await user.get_entity(uid))
                except FloodWaitError:
                    raise
                except Exception:
                    log.warning("не вдалося отримати %s", uid)
    return out


async def poller():
    """Запасний канал: раз на хвилину звіряє статуси (на випадок пропущених подій)."""
    while True:
        try:
            if AUTHED and targets:
                for u in await fetch_users(list(targets)):
                    s = u.status
                    if isinstance(s, UserStatusOnline):
                        await set_status(u.id, True)
                    elif isinstance(s, UserStatusOffline):
                        seen[u.id] = s.was_online
                        await set_status(u.id, False, s.was_online)
                    elif u.id in open_sess:  # статус став прихованим - закриваємо сесію
                        await set_status(u.id, False)
        except FloodWaitError as ex:
            log.warning("FloodWait %s c", ex.seconds)
            await asyncio.sleep(ex.seconds + 5)
        except Exception:
            log.exception("poller error")
        await asyncio.sleep(POLL_SECONDS)


async def fetch_sessions(a, b, uid=None):
    p = {"select": "user_id,start_at,stop_at", "start_at": f"lt.{iso(b)}",
         "or": f"(stop_at.is.null,stop_at.gt.{iso(a)})", "order": "start_at"}
    if uid:
        p["user_id"] = f"eq.{uid}"
    out = defaultdict(list)
    for r in await sb.select_all("tg_sessions", p):
        st = max(pdt(r["start_at"]), a)
        sp = min(pdt(r["stop_at"]) if r["stop_at"] else now(), b)
        if sp > st:
            out[r["user_id"]].append((st, sp))
    return out


def minutes_by_hour(ss):
    acc = [0.0] * 24
    for st, sp in ss:
        cur, end = st.astimezone(TZ), sp.astimezone(TZ)
        while cur < end:
            nxt = cur.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            seg = min(nxt, end)
            acc[cur.hour] += (seg - cur).total_seconds() / 60
            cur = seg
    return acc


def spark(vals):
    bars, m = "▁▂▃▄▅▆▇█", max(vals) or 1
    return "".join(bars[min(7, int(v / m * 7.999))] if v > 0 else "·" for v in vals)


# ---------- щоденний звіт ----------
async def send_digest():
    today0 = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    a = (today0 - timedelta(days=1)).astimezone(timezone.utc)
    b = today0.astimezone(timezone.utc)
    lines = [f"Підсумок за {(today0 - timedelta(days=1)).strftime('%d.%m')}"]
    sess = await fetch_sessions(a, b)
    for uid, t in targets.items():
        ss = sess.get(uid)
        if ss:
            total = sum((sp - st for st, sp in ss), timedelta())
            lines.append(f"• {t['name']}: {len(ss)} сес., {dur(total)}, "
                         f"{fmt_t(min(s[0] for s in ss))}-{fmt_t(max(s[1] for s in ss))}")
    if len(lines) == 1:
        lines.append("Активності відстежуваних не було")
    rng = {"created_at": [f"gte.{iso(a)}", f"lt.{iso(b)}"], "select": "id"}
    ne = len(await sb.select_all("tg_edits", rng))
    nd = len(await sb.select_all("tg_deletions", rng))
    ng = len(await sb.select_all("tg_gifts", rng))
    lines.append(f"\nзмін: {ne}, видалень: {nd}, подарунків: {ng}")
    await notify("\n".join(lines))


async def digest_loop():
    if DIGEST_HOUR < 0:
        return
    while True:
        n = datetime.now(TZ)
        nxt = n.replace(hour=DIGEST_HOUR, minute=0, second=0, microsecond=0)
        if nxt <= n:
            nxt += timedelta(days=1)
        await asyncio.sleep((nxt - n).total_seconds())
        try:
            if AUTHED:
                await send_digest()
        except Exception:
            log.exception("digest error")


# ---------- підключення акаунту (/reg) ----------
REG = {}


async def finish_auth():
    global ME, AUTHED
    ME = (await user.get_me()).id
    await kv_set("tg_session", user.session.save())
    AUTHED = True


async def cmd_reg(e, arg):
    if AUTHED:
        return await e.reply("Акаунт уже підключено.")
    if arg and REG.get("stage") == "code":
        return await reg_input(e, arg)
    sent = await user.send_code_request(PHONE)
    REG.clear()
    REG.update(stage="code", hash=sent.phone_code_hash)
    await e.reply("Код надіслано в Telegram на акаунт. Відправ його сюди через тире або пробіли, "
                  "напр. 1-2-3-4-5 (інакше Telegram скасує вхід).")


async def reg_input(e, text):
    stage = REG.get("stage")
    try:
        await e.delete()
    except Exception:
        pass
    try:
        if stage == "code":
            code = re.sub(r"\D", "", text)
            if not code:
                return await bot.send_message(OWNER_ID, "Надішли код цифрами (можна через тире).")
            await user.sign_in(PHONE, code, phone_code_hash=REG["hash"])
        else:
            await user.sign_in(password=text)
    except SessionPasswordNeededError:
        REG["stage"] = "2fa"
        return await bot.send_message(OWNER_ID, "Потрібен пароль 2FA. Надішли його сюди (повідомлення видалю).")
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
        REG.clear()
        return await bot.send_message(OWNER_ID, "Код невірний або прострочений. Надішли /reg ще раз.")
    except PasswordHashInvalidError:
        return await bot.send_message(OWNER_ID, "Невірний пароль 2FA. Спробуй ще раз.")
    except FloodWaitError as ex:
        REG.clear()
        return await bot.send_message(OWNER_ID, f"Забагато спроб. Зачекай {ex.seconds} с і надішли /reg.")
    except Exception as ex:
        REG.clear()
        log.exception("reg error")
        return await bot.send_message(OWNER_ID, f"Помилка: {ex}. Надішли /reg ще раз.")
    REG.clear()
    await finish_auth()
    await bot.send_message(OWNER_ID, "Акаунт підключено. Монітор працює.")


# ---------- 4. команди бота ----------
START_TEXT = "Привіт! Я твій особистий монітор-бот. Команда /list покаже всі доступні команди."

HELP = (
    "Підключення\n"
    "/reg - підключити акаунт (код прийде в Telegram)\n\n"
    "Пошук\n"
    "/find - знайти інформацію про людину (юзернейм або ID)\n\n"
    "Моніторинг онлайну\n"
    "/monadd <@username | ID> - додати людину в стеження\n"
    "/monlist - список людей, за якими стежу\n"
    "/addall - додати всіх, з ким є приватні чати\n"
    "/remove <...> - прибрати зі стеження\n\n"
    "Онлайн\n"
    "/status - хто зараз онлайн / коли був\n"
    "/log <...> [N] - останні N сесій\n"
    "/export <...> - CSV усіх сесій\n\n"
    "Статистика\n"
    "/stats <...> - онлайн за 7 днів + активність по годинах\n"
    "/stats gifts [in|out] [днів] [людина] - подарунки\n"
    "/stats msgs [днів] - зміни й видалення по чатах\n\n"
    "Повідомлення\n"
    "/history [N] - останні зміни й видалення\n"
    "/mute <...> / /unmute <...> - не сповіщати про зміни в чаті\n\n"
    "Годинник у профілі\n"
    "/time name|bio|off - поточний час у прізвищі або в біо\n"
    "/time tz <зона> - часовий пояс (напр. Europe/Kyiv)\n\n"
    "Сповіщення\n"
    "/notify - увімк/вимк сповіщення про вхід і вихід\n"
    "/notify <...> - те саме для однієї людини"
)

# стан діалогу /find для власника: {"stage": ..., "mode": "full"|"short"}
find_state = {}


def esc(v):
    return v if v not in (None, "") else "—"


async def gather_info(entity, mode):
    """Збирає всю доступну публічну інформацію про користувача. Без статистики онлайну."""
    lines = [f"ID: {entity.id}"]
    lines.append(f"Ім'я: {esc(display(entity))}")
    lines.append(f"Юзернейм: {'@' + entity.username if entity.username else '—'}")
    if entity.username:
        lines.append(f"Посилання: https://t.me/{entity.username}")
    flags = []
    if getattr(entity, "premium", False): flags.append("Premium")
    if getattr(entity, "verified", False): flags.append("Верифікований")
    if getattr(entity, "scam", False): flags.append("Позначений як scam")
    if getattr(entity, "fake", False): flags.append("Позначений як fake")
    if getattr(entity, "bot", False): flags.append("Бот")
    if flags:
        lines.append(" " + ", ".join(flags))
    if entity.status is not None:
        s = entity.status
        if isinstance(s, UserStatusOnline):
            lines.append("Зараз онлайн")
        elif isinstance(s, UserStatusOffline):
            lines.append(f"Був(ла) онлайн: {fmt(s.was_online)}")
        else:
            lines.append(f"Статус прихований ({type(s).__name__.replace('UserStatus', '')})")

    if mode == "short":
        return "\n".join(lines)

    try:
        full = await user(GetFullUserRequest(entity))
        fu = full.full_user
        if fu.about:
            lines.append(f"Біо: {fu.about}")
        if getattr(fu, "common_chats_count", None):
            lines.append(f"Спільних чатів: {fu.common_chats_count}")
        if getattr(fu, "phone_calls_available", None) is not None:
            lines.append(f"Дзвінки доступні: {'так' if fu.phone_calls_available else 'ні'}")
        if getattr(entity, "phone", None):
            lines.append(f"Телефон: {entity.phone}")
        if getattr(entity, "restricted", False):
            reason = ", ".join(r.reason for r in (entity.restriction_reason or []))
            lines.append(f"Обмежений: {reason or 'так'}")
        photos = getattr(fu, "profile_photo", None)
        lines.append(f"Фото профілю: {'є' if photos else 'немає'}")
    except Exception:
        log.exception("GetFullUserRequest failed")

    try:
        common = await user(GetCommonChatsRequest(user_id=entity, max_id=0, limit=20))
        if common.chats:
            names = ", ".join(getattr(c, "title", str(c.id)) for c in common.chats[:10])
            lines.append(f"Спільні групи: {names}")
    except Exception:
        pass

    return "\n".join(lines)


async def run_find(e, query, mode):
    try:
        entity = await resolve(query)
    except (UsernameNotOccupiedError, UsernameInvalidError):
        return await e.reply("Такого юзернейма не існує")
    except ValueError:
        return await e.reply("Не вдалося знайти цього користувача (немає спільних чатів або невірний ID)")
    except Exception as ex:
        log.exception("find error")
        return await e.reply(f"Помилка пошуку: {ex}")
    info = await gather_info(entity, mode)
    await e.reply(f"{'Повна інформація' if mode == 'full' else 'Коротка зведення'}\n\n{info}")


async def resolve(arg):
    if arg.lstrip("-").isdigit():
        return await user.get_entity(int(arg))
    if not arg.startswith("@"):
        low = arg.lower()
        for uid, t in targets.items():
            if low in (t["name"] or "").lower():
                return await user.get_entity(uid)
    return await user.get_entity(arg)


def person_and_n(args, default=10):
    if len(args) > 1 and args[-1].isdigit():
        return " ".join(args[:-1]), int(args[-1])
    return " ".join(args), default


async def stats_online(e, who):
    ent = await resolve(who)
    ss = (await fetch_sessions(now() - timedelta(days=7), now(), ent.id)).get(ent.id, [])
    if not ss:
        return await e.reply("Даних поки немає")
    total = sum((sp - st for st, sp in ss), timedelta())
    midnight = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    t_ss = (await fetch_sessions(midnight.astimezone(timezone.utc), now(), ent.id)).get(ent.id, [])
    today = sum((sp - st for st, sp in t_ss), timedelta())
    longest = max(ss, key=lambda x: x[1] - x[0])
    await e.reply(
        f"{display(ent)} за 7 днів\n"
        f"Сесій: {len(ss)}\n"
        f"Всього онлайн: {dur(total)}\n"
        f"Середня сесія: {dur(total / len(ss))}\n"
        f"Найдовша: {dur(longest[1] - longest[0])} ({fmt(longest[0])})\n"
        f"Сьогодні: {dur(today)}\n\n"
        f"Активність по годинах (00:00  23:00):\n{spark(minutes_by_hour(ss))}")


async def stats_gifts(e, args):
    direction, days, who = None, 30, []
    for a in args:
        if a.lower() in ("in", "out"):
            direction = a.lower()
        elif a.isdigit():
            days = int(a)
        else:
            who.append(a)
    p = {"order": "id.desc", "created_at": f"gte.{iso(now() - timedelta(days=days))}"}
    title = f"Подарунки за {days} дн."
    if direction:
        p["direction"] = f"eq.{direction}"
        title += " (отримані)" if direction == "in" else " (подаровані)"
    if who:
        ent = await resolve(" ".join(who))
        p["chat_id"] = f"eq.{ent.id}"
        title += f", {display(ent)}"
    rows = await sb.select_all("tg_gifts", p)
    if not rows:
        return await e.reply(f"{title}\nНічого немає")
    ins = [r for r in rows if r["direction"] == "in"]
    outs = [r for r in rows if r["direction"] == "out"]
    lines = [title]
    if ins:
        lines.append(f"Отримано: {len(ins)} (зірок: {sum(r['stars'] or 0 for r in ins)})")
    if outs:
        lines.append(f"Подаровано: {len(outs)} (зірок: {sum(r['stars'] or 0 for r in outs)})")
    if ins and not who:
        lines.append("Найбільше від: " + ", ".join(f"{n} ({c})" for n, c in
                                                   Counter(r["chat_name"] for r in ins).most_common(5)))
    if outs and not who:
        lines.append("Найбільше кому: " + ", ".join(f"{n} ({c})" for n, c in
                                                    Counter(r["chat_name"] for r in outs).most_common(5)))
    lines.append("\nОстанні:")
    for r in rows[:10]:
        arrow = "<-" if r["direction"] == "in" else "->"
        st = f" ({r['stars']} зірок)" if r["stars"] else ""
        lines.append(f"{fmt(pdt(r['msg_date'] or r['created_at']))} {arrow} {r['chat_name']}: {r['title']}{st}")
    await reply_long(e, "\n".join(lines))


async def stats_msgs(e, args):
    days = int(args[0]) if args and args[0].isdigit() else 7
    p = {"select": "chat_name", "created_at": f"gte.{iso(now() - timedelta(days=days))}"}
    ed = Counter(r["chat_name"] for r in await sb.select_all("tg_edits", p))
    de = Counter(r["chat_name"] for r in await sb.select_all("tg_deletions", p))
    lines = [f"Повідомлення за {days} дн.", f"Змін: {sum(ed.values())}, видалень: {sum(de.values())}"]
    if de:
        lines.append("Найбільше видаляють: " + ", ".join(f"{n} ({c})" for n, c in de.most_common(5)))
    if ed:
        lines.append("Найбільше змінюють: " + ", ".join(f"{n} ({c})" for n, c in ed.most_common(5)))
    await e.reply("\n".join(lines))


async def on_callback(cq):
    data = cq.data.decode()
    st = find_state.get(OWNER_ID, {})
    if data.startswith("find_mode_"):
        st["mode"] = data.split("_", 2)[2]  # full | short
        st["stage"] = "await_type"
        find_state[OWNER_ID] = st
        await cq.edit(
            "Що вводимо для пошуку?",
            buttons=[[Button.inline("1. Юзернейм", b"find_type_username"),
                      Button.inline("2. Telegram ID", b"find_type_id")]])
    elif data.startswith("find_type_"):
        st["type"] = data.split("_", 2)[2]  # username | id
        st["stage"] = "await_value"
        find_state[OWNER_ID] = st
        label = "юзернейм (наприклад, @durov)" if st["type"] == "username" else "Telegram ID (число)"
        await cq.edit(f"Введи {label} повідомленням у цей чат.")
    else:
        await cq.answer()


async def on_cmd(e):
    text = e.raw_text.strip()
    if REG.get("stage") and not text.startswith("/"):
        return await reg_input(e, text)
    st = find_state.get(OWNER_ID)
    if st and st.get("stage") == "await_value" and not text.startswith("/"):
        find_state.pop(OWNER_ID, None)
        query = text.lstrip("@").strip()
        if st["type"] == "id" and not query.lstrip("-").isdigit():
            return await e.reply("Це має бути число (Telegram ID). Спробуй /find ще раз.")
        await run_find(e, query, st["mode"])
        return
    if not text.startswith("/"):
        return
    parts = text.split()
    cmd, args = parts[0].lower().split("@")[0], parts[1:]
    arg = " ".join(args)
    try:
        if cmd == "/start":
            await e.reply(START_TEXT)

        elif cmd in ("/list", "/help"):
            await e.reply(HELP)

        elif cmd == "/reg":
            await cmd_reg(e, arg)

        elif not AUTHED:
            await e.reply("Акаунт не підключено. Надішли /reg")

        elif cmd == "/find":
            find_state[OWNER_ID] = {"stage": "await_mode"}
            await e.reply(
                "Режим пошуку?",
                buttons=[[Button.inline("Повний пошук", b"find_mode_full"),
                          Button.inline("Коротка зведення", b"find_mode_short")]])

        elif cmd in ("/add", "/monadd") and arg:
            ent = await resolve(arg)
            targets[ent.id] = {"name": display(ent), "notify": True}
            await sb.insert("tg_targets", [{"user_id": ent.id, "name": display(ent), "notify": True}],
                            merge=True, on_conflict="user_id")
            await e.reply(f"Додано в стеження: {display(ent)} ({ent.id})")

        elif cmd == "/addall":
            new = []
            async for d in user.iter_dialogs():
                ent = d.entity
                if (d.is_user and not ent.bot and not ent.is_self and not ent.deleted
                        and ent.id != 777000 and ent.id not in targets):
                    targets[ent.id] = {"name": display(ent), "notify": True}
                    new.append({"user_id": ent.id, "name": display(ent), "notify": True})
            for i in range(0, len(new), 500):
                await sb.insert("tg_targets", new[i:i + 500], ignore_dups=True, on_conflict="user_id")
            await e.reply(f"Додано нових: {len(new)}")

        elif cmd == "/remove" and arg:
            ent = await resolve(arg)
            targets.pop(ent.id, None)
            await sb.delete("tg_targets", {"user_id": f"eq.{ent.id}"})
            await e.reply(f"Видалено зі списку: {display(ent)}")

        elif cmd == "/monlist":
            rows = sorted(targets.items(), key=lambda x: x[1]["name"] or "")
            await reply_long(e, "\n".join(f"[{'on' if t['notify'] else 'off'}] {t['name']} ({i})"
                                          for i, t in rows) or "Список порожній")

        elif cmd == "/status":
            last_stop = {}
            for r in await sb.select("tg_sessions", {"select": "user_id,stop_at", "stop_at": "not.is.null",
                                                     "order": "id.desc", "limit": "1000"}):
                last_stop.setdefault(r["user_id"], pdt(r["stop_at"]))
            lines = []
            for uid, t in sorted(targets.items(), key=lambda x: x[1]["name"] or ""):
                op = open_sess.get(uid)
                if op:
                    lines.append(f"{t['name']} - онлайн {dur(now() - op[1])} (з {fmt_t(op[1])})")
                else:
                    cand = [x for x in (seen.get(uid), last_stop.get(uid)) if x]
                    lines.append(f"{t['name']} - був(ла) {fmt(max(cand))}" if cand
                                 else f"{t['name']} - даних немає")
            await reply_long(e, "\n".join(lines) or "Список порожній")

        elif cmd == "/time":
            sub = args[0].lower() if args else ""
            if sub in ("name", "bio", "off"):
                await clock_set(sub)
                await e.reply("Годинник вимкнено, профіль відновлено." if sub == "off"
                              else "Годинник увімкнено, оновлюється щохвилини.")
            elif sub == "tz" and len(args) > 1:
                ZoneInfo(args[1])
                await kv_set("clock_tz", args[1])
                await e.reply(f"Часовий пояс: {args[1]}")
            else:
                await e.reply(f"Режим: {kv.get('clock_mode', 'off')}, пояс: {clock_tz().key}\n"
                              "/time name|bio|off, /time tz <зона>")

        elif cmd == "/stats":
            if not args:
                await e.reply("/stats <людина> - онлайн\n/stats gifts [in|out] [днів] [людина]\n/stats msgs [днів]")
            elif args[0].lower() == "gifts":
                await stats_gifts(e, args[1:])
            elif args[0].lower() == "msgs":
                await stats_msgs(e, args[1:])
            else:
                await stats_online(e, arg)

        elif cmd == "/log" and arg:
            who, n = person_and_n(args)
            ent = await resolve(who)
            rows = await sb.select("tg_sessions", {"user_id": f"eq.{ent.id}", "order": "id.desc",
                                                   "limit": str(n)})
            lines = []
            for r in rows:
                st = pdt(r["start_at"])
                lines.append(f"{fmt(st)} -> {fmt(pdt(r['stop_at']))} ({dur(pdt(r['stop_at']) - st)})"
                             if r["stop_at"] else f"{fmt(st)}  зараз онлайн")
            await reply_long(e, "\n".join(lines) or "Логів немає")

        elif cmd == "/export" and arg:
            ent = await resolve(arg)
            path = f"export_{ent.id}.csv"
            rows = await sb.select_all("tg_sessions", {"user_id": f"eq.{ent.id}", "order": "id"})
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f)
                w.writerow(["start", "stop", "minutes"])
                for r in rows:
                    st = pdt(r["start_at"])
                    sp = pdt(r["stop_at"]) if r["stop_at"] else None
                    w.writerow([st.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S"),
                                sp.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if sp else "",
                                round((sp - st).total_seconds() / 60, 1) if sp else ""])
            await bot.send_file(OWNER_ID, path, caption=f"Сесії: {display(ent)}")
            os.remove(path)

        elif cmd == "/history":
            n = int(args[0]) if args and args[0].isdigit() else 10
            ev = []
            for r in await sb.select("tg_edits", {"order": "id.desc", "limit": str(n)}):
                d = pdt(r["created_at"])
                ev.append((d, f"{r['chat_name']} ({fmt(d)})\n{r['old_text']}\n-> {r['new_text']}"))
            for r in await sb.select("tg_deletions", {"order": "id.desc", "limit": str(n)}):
                d = pdt(r["created_at"])
                ev.append((d, f"{r['chat_name']} ({fmt(d)}) видалено\n{r['text']}"))
            ev.sort(key=lambda x: x[0], reverse=True)
            await reply_long(e, "\n\n".join(x for _, x in ev[:n]) or "Історія порожня")

        elif cmd in ("/mute", "/unmute") and arg:
            ent = await resolve(arg)
            if cmd == "/mute":
                muted.add(ent.id)
                await sb.insert("tg_muted", [{"chat_id": ent.id}], ignore_dups=True, on_conflict="chat_id")
            else:
                muted.discard(ent.id)
                await sb.delete("tg_muted", {"chat_id": f"eq.{ent.id}"})
            await e.reply(f"{'Вимкнено' if cmd == '/mute' else 'Увімкнено'} сповіщення про чат: {display(ent)}")

        elif cmd == "/notify":
            if arg:
                ent = await resolve(arg)
                t = targets.get(ent.id)
                if not t:
                    return await e.reply("Цієї людини немає в стеженні")
                t["notify"] = not t["notify"]
                await sb.update("tg_targets", {"user_id": f"eq.{ent.id}"}, {"notify": t["notify"]})
                await e.reply(f"{display(ent)}: сповіщення {'увімкнено' if t['notify'] else 'вимкнено'}")
            else:
                new = "0" if get_setting("notify_presence", "0") == "1" else "1"
                await kv_set("notify_presence", new)
                await e.reply(f"Сповіщення про вхід/вихід: {'увімкнено' if new == '1' else 'вимкнено'}")

        else:
            await e.reply("Невідома команда. /list - список усіх команд.")
    except Exception as ex:
        log.exception("cmd error")
        await e.reply(f"Помилка: {ex}")


# ---------- 5. годинник у профілі (/time) ----------
_clock_last = None


def clock_tz():
    try:
        return ZoneInfo(kv.get("clock_tz") or "")
    except Exception:
        return TZ


def with_clock(orig, limit):
    t = datetime.now(clock_tz()).strftime("%H:%M")
    keep = limit - len(t) - 3
    return f"{orig[:keep]} | {t}" if orig and keep > 0 else t


async def clock_apply(mode):
    global _clock_last
    key = (mode, datetime.now(clock_tz()).strftime("%H:%M"))
    if _clock_last == key:
        return
    if mode == "name":
        await user(UpdateProfileRequest(last_name=with_clock(kv.get("clock_orig_last", ""), 64)))
    else:
        await user(UpdateProfileRequest(about=with_clock(kv.get("clock_orig_about", ""), 70)))
    _clock_last = key


async def clock_restore():
    global _clock_last
    mode = kv.get("clock_mode", "off")
    try:
        if mode == "name":
            await user(UpdateProfileRequest(last_name=kv.get("clock_orig_last", "")))
        elif mode == "bio":
            await user(UpdateProfileRequest(about=kv.get("clock_orig_about", "")))
    finally:
        _clock_last = None
        await kv_set("clock_mode", "off")


async def clock_set(mode):
    if kv.get("clock_mode", "off") != "off":
        await clock_restore()
    if mode == "off":
        return
    me = await user.get_me()
    full = await user(GetFullUserRequest(me))
    await kv_set("clock_orig_last", me.last_name or "")
    await kv_set("clock_orig_about", full.full_user.about or "")
    await kv_set("clock_mode", mode)
    await clock_apply(mode)


async def clock_loop():
    while True:
        await asyncio.sleep(60.5 - datetime.now(timezone.utc).second)
        mode = kv.get("clock_mode", "off")
        if AUTHED and mode in ("name", "bio"):
            try:
                await clock_apply(mode)
            except FloodWaitError as ex:
                await asyncio.sleep(ex.seconds + 5)
            except Exception:
                log.exception("clock error")


# ---------- запуск ----------
async def main():
    global user, bot, AUTHED
    AUTHED = False
    await load_state()
    opts = dict(connection_retries=-1, retry_delay=3)
    user = TelegramClient(StringSession(kv.get("tg_session", "")), API_ID, API_HASH, **opts)
    bot = TelegramClient(StringSession(kv.get("bot_session", "")), API_ID, API_HASH, **opts)

    user.add_event_handler(on_new, events.NewMessage(func=lambda e: e.is_private))
    user.add_event_handler(on_edit, events.MessageEdited(func=lambda e: e.is_private))
    user.add_event_handler(on_delete, events.MessageDeleted())
    user.add_event_handler(on_presence, events.UserUpdate)
    bot.add_event_handler(on_callback, events.CallbackQuery(from_users=OWNER_ID))
    bot.add_event_handler(on_cmd, events.NewMessage(from_users=OWNER_ID))

    tasks, waiters = [], []
    try:
        await bot.start(bot_token=BOT_TOKEN)
        await kv_set("bot_session", bot.session.save())
        await user.connect()
        if await user.is_authorized():
            await finish_auth()
            await notify("Монітор запущено. /list")
        else:
            await notify("Акаунт не підключено. Надішли /reg")
        tasks = [asyncio.create_task(poller()), asyncio.create_task(digest_loop()),
                 asyncio.create_task(clock_loop())]
        waiters = [asyncio.ensure_future(user.run_until_disconnected()),
                   asyncio.ensure_future(bot.run_until_disconnected())]
        await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        raise RuntimeError("клієнт відключився")
    finally:
        for t in tasks + waiters:
            t.cancel()
        for c in (user, bot):
            try:
                await c.disconnect()
            except Exception:
                pass


async def runner():
    global sb
    sb = Supa(SUPABASE_URL, SUPABASE_KEY)
    while True:  # супервізор: будь-який збій -> перезапуск
        try:
            await main()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("збій, перезапуск через 10 с")
        await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(runner())
