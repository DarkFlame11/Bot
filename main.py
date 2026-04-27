import os
import re
import math
import asyncio
import logging
import html
import random
import datetime
import unicodedata
import asyncpg

from aiohttp import web

from aiogram import BaseMiddleware, Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, LabeledPrice, PreCheckoutQuery
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession

# --- НАСТРОЙКИ ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("CRITICAL: Переменная BOT_TOKEN не задана!")

ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))

def get_channel_id() -> int:
    try:
        return int(os.environ.get("CHANNEL_ID", "0").strip())
    except (ValueError, TypeError):
        return 0

DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    raise ValueError("CRITICAL: Переменная DATABASE_URL не задана!")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

USE_SSL = "railway" in DATABASE_URL or os.environ.get("DB_SSL", "").lower() == "true"

session = AiohttpSession(timeout=60)
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher(storage=MemoryStorage())

db_pool = None

# --- БАЗА ДАННЫХ ---
async def init_db_pool():
    global db_pool
    kwargs = dict(min_size=2, max_size=10, command_timeout=10, statement_cache_size=0, timeout=10)
    if USE_SSL:
        kwargs["ssl"] = "require"
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, **kwargs)
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        logging.info("✅ DB pool создан и проверен")
    except Exception as e:
        logging.error(f"❌ Ошибка БД: {e}")
        raise

async def get_db():
    return db_pool

async def register_user(user: types.User):
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO users (user_id, username, first_name, last_name)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    last_seen = CURRENT_TIMESTAMP,
                    is_active = TRUE
            """, user.id, user.username, user.first_name, user.last_name)
    except Exception as e:
        logging.error(f"Ошибка регистрации пользователя: {e}")

class RegisterUserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = None
        if isinstance(event, types.Message) and event.from_user:
            user = event.from_user
        elif isinstance(event, types.CallbackQuery) and event.from_user:
            user = event.from_user
        if user and not user.is_bot:
            await register_user(user)
        return await handler(event, data)

# --- ГЕЙТ ПОДПИСКИ НА КАНАЛ ---
SUB_CACHE: dict[int, tuple[bool, float]] = {}
SUB_CACHE_TTL_OK = 600.0
SUB_CACHE_TTL_FAIL = 30.0
CHANNEL_INFO_CACHE: dict = {"url": None, "title": None, "fetched_at": 0.0}
CHANNEL_INFO_TTL = 3600.0
SUB_GATE_BYPASS_CALLBACKS = {"check_sub"}

async def get_channel_info() -> tuple[str | None, str | None]:
    """Возвращает (url, title) канала. Кеширует на час."""
    now = asyncio.get_event_loop().time()
    if (CHANNEL_INFO_CACHE["url"] or CHANNEL_INFO_CACHE["title"]) and \
            now - CHANNEL_INFO_CACHE["fetched_at"] < CHANNEL_INFO_TTL:
        return CHANNEL_INFO_CACHE["url"], CHANNEL_INFO_CACHE["title"]
    cid = get_channel_id()
    if not cid:
        return None, None
    url = None
    title = None
    try:
        chat = await bot.get_chat(cid)
        title = chat.title or None
        if chat.username:
            url = f"https://t.me/{chat.username}"
        else:
            url = chat.invite_link
            if not url:
                try:
                    url = await bot.export_chat_invite_link(cid)
                except Exception as e:
                    logging.warning(f"Не удалось создать invite-ссылку канала: {e}")
    except Exception as e:
        logging.warning(f"Не удалось получить инфо канала: {e}")
    CHANNEL_INFO_CACHE["url"] = url
    CHANNEL_INFO_CACHE["title"] = title
    CHANNEL_INFO_CACHE["fetched_at"] = now
    return url, title

async def is_subscribed(user_id: int) -> bool:
    """Проверка подписки на CHANNEL_ID с кешем.
    Админ — всегда True. Если CHANNEL_ID не задан — гейт выключен (True).
    На ошибки Telegram отвечаем fail-open, чтобы не запереть всех при сбое API."""
    if user_id == ADMIN_ID:
        return True
    cid = get_channel_id()
    if not cid:
        return True
    now = asyncio.get_event_loop().time()
    cached = SUB_CACHE.get(user_id)
    if cached:
        ok, ts = cached
        ttl = SUB_CACHE_TTL_OK if ok else SUB_CACHE_TTL_FAIL
        if now - ts < ttl:
            return ok
    try:
        member = await bot.get_chat_member(cid, user_id)
        status = getattr(member, "status", None)
        if status in ("creator", "administrator", "member"):
            ok = True
        elif status == "restricted":
            ok = bool(getattr(member, "is_member", False))
        else:
            ok = False
    except Exception as e:
        logging.warning(f"Не удалось проверить подписку user={user_id}: {e}")
        return True
    SUB_CACHE[user_id] = (ok, now)
    return ok

def _build_sub_gate_kb(channel_url: str | None) -> InlineKeyboardMarkup:
    rows = []
    if channel_url:
        rows.append([InlineKeyboardButton(text="📢 Открыть канал", url=channel_url)])
    rows.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def send_sub_gate(event):
    url, title = await get_channel_info()
    title_html = html.escape(title) if title else "наш канал"
    text = (
        f"🔒 Доступ к боту только для подписчиков канала <b>{title_html}</b>.\n\n"
        f"Подпишись и нажми «Я подписался» — и сразу продолжим."
    )
    kb = _build_sub_gate_kb(url)
    if isinstance(event, types.CallbackQuery):
        try:
            await event.answer()
        except Exception:
            pass
        msg = event.message
        if msg:
            try:
                await msg.answer(text, parse_mode="HTML", reply_markup=kb)
                return
            except Exception:
                pass
    if isinstance(event, types.Message):
        try:
            await event.answer(text, parse_mode="HTML", reply_markup=kb)
        except Exception as e:
            logging.warning(f"Не удалось отправить sub-gate: {e}")

class SubscriptionMiddleware(BaseMiddleware):
    """Пускает в личку с ботом только подписчиков канала.
    Админа, ботов, а также апдейты из групп/каналов/обсуждалок пропускает без проверки."""
    async def __call__(self, handler, event, data):
        user = getattr(event, 'from_user', None)
        if not user or user.is_bot or user.id == ADMIN_ID:
            return await handler(event, data)
        # Определяем чат события
        chat = None
        if isinstance(event, types.Message):
            chat = event.chat
        elif isinstance(event, types.CallbackQuery) and event.message:
            chat = event.message.chat
        # Гейтим только личку с ботом — комментарии в обсуждалке и события каналов пропускаем
        if chat is not None and chat.type != "private":
            return await handler(event, data)
        # Кнопка "Я подписался" должна доходить до своего обработчика
        if isinstance(event, types.CallbackQuery) and event.data in SUB_GATE_BYPASS_CALLBACKS:
            return await handler(event, data)
        # PreCheckoutQuery и прочие апдейты без явного chat — не гейтим
        if chat is None and not isinstance(event, types.Message):
            return await handler(event, data)
        if await is_subscribed(user.id):
            return await handler(event, data)
        await send_sub_gate(event)
        return None

class ThrottleMiddleware(BaseMiddleware):
    """Антиспам: ограничивает частоту действий пользователя.
    По умолчанию: не больше RATE_LIMIT_HITS событий за RATE_LIMIT_WINDOW секунд."""
    RATE_LIMIT_HITS = 8
    RATE_LIMIT_WINDOW = 5.0
    WARN_COOLDOWN = 15.0

    def __init__(self):
        self._hits: dict[int, list[float]] = {}
        self._last_warn: dict[int, float] = {}

    async def __call__(self, handler, event, data):
        user = getattr(event, 'from_user', None)
        if not user or user.is_bot or user.id == ADMIN_ID:
            return await handler(event, data)
        now = asyncio.get_event_loop().time()
        hits = self._hits.setdefault(user.id, [])
        cutoff = now - self.RATE_LIMIT_WINDOW
        hits[:] = [t for t in hits if t > cutoff]
        hits.append(now)
        if len(hits) > self.RATE_LIMIT_HITS:
            last_warn = self._last_warn.get(user.id, 0)
            if now - last_warn > self.WARN_COOLDOWN:
                self._last_warn[user.id] = now
                try:
                    if isinstance(event, types.CallbackQuery):
                        await event.answer("⏳ Слишком часто! Подожди немного.", show_alert=False)
                    elif isinstance(event, types.Message):
                        await event.answer("⏳ Слишком часто! Подожди пару секунд.")
                except Exception:
                    pass
            return None
        return await handler(event, data)

async def init_db():
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tracks (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                artist TEXT,
                file_id TEXT UNIQUE NOT NULL,
                plays INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS favorites (
                user_id BIGINT NOT NULL,
                track_id INTEGER NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(user_id, track_id),
                FOREIGN KEY(track_id) REFERENCES tracks(id) ON DELETE CASCADE
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS playlists (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS playlist_tracks (
                playlist_id INTEGER NOT NULL,
                track_id INTEGER NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(playlist_id, track_id),
                FOREIGN KEY(playlist_id) REFERENCES playlists(id) ON DELETE CASCADE,
                FOREIGN KEY(track_id) REFERENCES tracks(id) ON DELETE CASCADE
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS vote_sessions (
                id SERIAL PRIMARY KEY,
                vote_type TEXT NOT NULL,
                period TEXT NOT NULL,
                channel_message_id BIGINT,
                channel_chat_id BIGINT,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(vote_type, period)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS vote_candidates (
                session_id INTEGER NOT NULL,
                track_id INTEGER NOT NULL,
                PRIMARY KEY(session_id, track_id),
                FOREIGN KEY(session_id) REFERENCES vote_sessions(id) ON DELETE CASCADE,
                FOREIGN KEY(track_id) REFERENCES tracks(id) ON DELETE CASCADE
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS votes (
                id SERIAL PRIMARY KEY,
                session_id INTEGER NOT NULL,
                track_id INTEGER NOT NULL,
                user_id BIGINT NOT NULL,
                voted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(session_id, user_id),
                FOREIGN KEY(session_id) REFERENCES vote_sessions(id) ON DELETE CASCADE,
                FOREIGN KEY(track_id) REFERENCES tracks(id) ON DELETE CASCADE
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS request_sessions (
                id SERIAL PRIMARY KEY,
                channel_chat_id BIGINT,
                channel_message_id BIGINT,
                discussion_chat_id BIGINT,
                discussion_thread_id BIGINT,
                title TEXT,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS track_requests (
                id SERIAL PRIMARY KEY,
                session_id INTEGER NOT NULL,
                user_id BIGINT,
                username TEXT,
                full_name TEXT,
                text TEXT NOT NULL,
                discussion_message_id BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(session_id) REFERENCES request_sessions(id) ON DELETE CASCADE
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_req_sessions_active "
            "ON request_sessions (status, created_at DESC)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_req_sessions_thread "
            "ON request_sessions (discussion_chat_id, discussion_thread_id)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_track_requests_session "
            "ON track_requests (session_id, created_at DESC)"
        )
        await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tracks_title_trgm ON tracks USING GIN (title gin_trgm_ops)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tracks_artist_trgm ON tracks USING GIN (artist gin_trgm_ops)"
        )
        await conn.execute(
            "ALTER TABLE vote_sessions ADD COLUMN IF NOT EXISTS closes_at TIMESTAMP"
        )
        await conn.execute(
            "ALTER TABLE tracks ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tracks_plays ON tracks (plays DESC)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tracks_created_at ON tracks (created_at DESC)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tracks_artist ON tracks (artist)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_favorites_user ON favorites (user_id, added_at DESC)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_playlist_tracks_pl ON playlist_tracks (playlist_id, added_at DESC)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_votes_session ON votes (session_id, user_id)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_vote_sessions_active ON vote_sessions (status, vote_type, created_at DESC)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_users_active ON users (is_active) WHERE is_active = TRUE"
        )
        logging.info("✅ База данных инициализирована")

# --- СОСТОЯНИЯ ---
class PlaylistForm(StatesGroup):
    waiting_name = State()

class BroadcastForm(StatesGroup):
    waiting_text = State()

class DonateForm(StatesGroup):
    waiting_amount = State()

class RequestForm(StatesGroup):
    waiting_text = State()

BOT_USERNAME = ""

# --- КОНСТАНТЫ ---
PAGE_SIZE = 10
FAV_PAGE_SIZE = 5
PL_TRACK_PAGE_SIZE = 5
MAX_PLAYLISTS = 5

# --- КЛАВИАТУРЫ ---
menu = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text="🔍 Поиск"), KeyboardButton(text="🎲 Случайный")],
    [KeyboardButton(text="🔥 Топ"), KeyboardButton(text="🆕 Новое")],
    [KeyboardButton(text="❤️ Избранное"), KeyboardButton(text="📋 Список Плейлистов")],
    [KeyboardButton(text="💝 Донат")],
], resize_keyboard=True)

def clean_artist(a):
    return "" if a and a.startswith("@") else (a or "")

def format_track(a, t):
    return f"{a} — {t}" if clean_artist(a) else t

DIGITS = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

def num_buttons(ids):
    b = [InlineKeyboardButton(text=DIGITS[i], callback_data=f"track_{tid}") for i, tid in enumerate(ids)]
    return [b[i:i+5] for i in range(0, len(b), 5)]

async def track_keyboard(tid, uid):
    pool = await get_db()
    async with pool.acquire() as conn:
        result = await conn.fetchval("SELECT 1 FROM favorites WHERE user_id=$1 AND track_id=$2", uid, tid)
        inf = result is not None
    btn = InlineKeyboardButton(
        text="💔 Убрать" if inf else "❤️ В избранное",
        callback_data=f"unfav_{tid}" if inf else f"fav_{tid}"
    )
    return InlineKeyboardMarkup(inline_keyboard=[
        [btn],
        [InlineKeyboardButton(text="➕ В плейлист", callback_data=f"topl_{tid}")],
        [InlineKeyboardButton(text="🎵 Похожие", callback_data=f"rec_{tid}")]
    ])

# --- ПОИСК И ТРАНСЛИТЕРАЦИЯ ---
CYR_LAT = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'yo','ж':'zh','з':'z','и':'i',
    'й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r','с':'s','т':'t',
    'у':'u','ф':'f','х':'kh','ц':'ts','ч':'ch','ш':'sh','щ':'shch','ъ':'','ы':'y','ь':'',
    'э':'e','ю':'yu','я':'ya'
}

CYR_LAT_SIMPLE = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'zh','з':'z','и':'i',
    'й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r','с':'s','т':'t',
    'у':'u','ф':'f','х':'h','ц':'ts','ч':'ch','ш':'sh','щ':'sh','ъ':'','ы':'y','ь':'',
    'э':'e','ю':'yu','я':'ya'
}

LAT_CYR = {
    "shch":"щ","sch":"щ","sh":"ш","ch":"ч","zh":"ж","kh":"х","ph":"ф","ts":"ц",
    "yu":"ю","ya":"я","yo":"ё",
    "a":"а","b":"б","v":"в","g":"г","d":"д","e":"е","z":"з","i":"и",
    "y":"и","k":"к","l":"л","m":"м","n":"н","o":"о","p":"п","r":"р","s":"с",
    "t":"т","u":"у","f":"ф","h":"х","c":"к","w":"в","x":"х","j":"й"
}

def translit_to_latin(text):
    result = text.lower()
    for cyr in sorted(CYR_LAT.keys(), key=len, reverse=True):
        result = result.replace(cyr, CYR_LAT[cyr])
    return result

def translit_to_latin_simple(text):
    result = text.lower()
    for cyr in sorted(CYR_LAT_SIMPLE.keys(), key=len, reverse=True):
        result = result.replace(cyr, CYR_LAT_SIMPLE[cyr])
    return result

def translit_to_cyrillic(text):
    result = text.lower()
    for lat in sorted(LAT_CYR.keys(), key=len, reverse=True):
        result = result.replace(lat, LAT_CYR[lat])
    return result

def get_var(q):
    q = q.lower().strip()
    if not q: return []
    q = q.replace('\u200b','').replace('\u200c','').replace('\u200d','')
    if not q: return []
    v = [q]
    has_cyrillic = any(0x0400 <= ord(c) <= 0x04FF for c in q)
    has_latin = any(c.isascii() and c.isalpha() for c in q)
    if has_cyrillic:
        lat = translit_to_latin(q)
        if lat and lat != q: v.append(lat)
        lat2 = translit_to_latin_simple(q)
        if lat2 and lat2 != q and lat2 not in v: v.append(lat2)
    if has_latin:
        cyr = translit_to_cyrillic(q)
        if cyr and cyr != q: v.append(cyr)
    return v

def _esc_like(s: str) -> str:
    """Экранирует спецсимволы LIKE/ILIKE."""
    return s.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')

def _search_query_parts(variants):
    """Собирает части SQL для поиска по треку/артисту с ранжированием.
    Возвращает (where_expr, score_expr, params, next_param_index).
    На каждый вариант запроса генерирует 3 параметра (exact, prefix, substring)
    и оценивает релевантность: точное совпадение > префикс > подстрока,
    название важнее исполнителя.
    """
    where = []
    score_terms = []
    params = []
    p = 1
    for v in variants:
        v_esc = _esc_like(v)
        params.extend([v_esc, v_esc + '%', '%' + v_esc + '%'])
        i_exact, i_prefix, i_substr = p, p + 1, p + 2
        p += 3
        where.append(f"title ILIKE ${i_substr}")
        where.append(f"artist ILIKE ${i_substr}")
        score_terms.append(
            f"(CASE WHEN title  ILIKE ${i_exact}  THEN 100 ELSE 0 END"
            f" + CASE WHEN artist ILIKE ${i_exact}  THEN 80  ELSE 0 END"
            f" + CASE WHEN title  ILIKE ${i_prefix} THEN 30  ELSE 0 END"
            f" + CASE WHEN artist ILIKE ${i_prefix} THEN 25  ELSE 0 END"
            f" + CASE WHEN title  ILIKE ${i_substr} THEN 5   ELSE 0 END"
            f" + CASE WHEN artist ILIKE ${i_substr} THEN 4   ELSE 0 END)"
        )
    where_expr = ' OR '.join(where) if where else 'FALSE'
    score_expr = ' + '.join(score_terms) if score_terms else '0'
    return where_expr, score_expr, params, p

async def run_search(query, offset=0):
    async with db_pool.acquire() as conn:
        var = get_var(query)
        if not var:
            return [], 0
        try:
            where_expr, score_expr, params, p = _search_query_parts(var)
            sql = (
                f"SELECT id, title, artist, COUNT(*) OVER() AS total "
                f"FROM (SELECT id, title, artist, plays, "
                f"             ({score_expr}) AS score "
                f"      FROM tracks WHERE {where_expr}) sub "
                f"ORDER BY score DESC, plays DESC, id DESC "
                f"LIMIT ${p} OFFSET ${p+1}"
            )
            params.extend([PAGE_SIZE, offset])
            rows = await conn.fetch(sql, *params)
            total = rows[0]['total'] if rows else 0
            return rows, int(total)
        except Exception as e:
            logging.error(f"❌ Ошибка поиска: {e}")
    return [], 0

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ПАГИНАЦИИ ---

async def show_fav_page(user_id: int, page: int, target, edit: bool = False):
    pool = await get_db()
    async with pool.acquire() as conn:
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM favorites WHERE user_id=$1", user_id
        )
        if total == 0:
            text = "❤️ Избранное пусто"
            if edit:
                try:
                    await target.message.edit_text(text)
                except Exception:
                    await target.message.answer(text)
            else:
                await target.answer(text)
            return
        offset = page * FAV_PAGE_SIZE
        res = await conn.fetch(
            "SELECT tracks.id, tracks.title, tracks.artist FROM tracks "
            "JOIN favorites ON tracks.id=favorites.track_id "
            "WHERE favorites.user_id=$1 ORDER BY favorites.added_at DESC "
            "LIMIT $2 OFFSET $3",
            user_id, FAV_PAGE_SIZE, offset
        )
    total_pages = math.ceil(total / FAV_PAGE_SIZE)
    ids = [r['id'] for r in res]
    lines = [
        f"{i + 1 + offset}. {html.escape(format_track(r['artist'], r['title']))}"
        for i, r in enumerate(res)
    ]
    h = (
        f"❤️ <b>Избранное</b> — {total} тр. • Стр. {page + 1}/{total_pages}\n\n"
        + "\n".join(lines)
    )
    play_rows = num_buttons(ids)
    unfav_btns = [
        InlineKeyboardButton(text=f"💔#{r['id']}", callback_data=f"unfav_f_{r['id']}_{page}")
        for r in res
    ]
    unfav_rows = [unfav_btns[i:i+5] for i in range(0, len(unfav_btns), 5)]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"favp_{page - 1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"favp_{page + 1}"))
    rows = play_rows + unfav_rows
    rows.append([InlineKeyboardButton(text="🎲 Случайный из избранного", callback_data="rnd_fav")])
    if nav:
        rows.append(nav)
    if total_pages > 1:
        rows.append([InlineKeyboardButton(text=f"Стр. {page + 1} из {total_pages}", callback_data="noop")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    if edit:
        try:
            await target.message.edit_text(h, reply_markup=kb, parse_mode="HTML")
        except Exception:
            await target.message.answer(h, reply_markup=kb, parse_mode="HTML")
    else:
        await target.answer(h, reply_markup=kb, parse_mode="HTML")

async def show_playlist_page(pid: int, user_id: int, page: int, target, edit: bool = False):
    pool = await get_db()
    async with pool.acquire() as conn:
        pl = await conn.fetchrow(
            "SELECT name FROM playlists WHERE id=$1 AND user_id=$2", pid, user_id
        )
        if not pl:
            await target.answer("❌", show_alert=True)
            return
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM playlist_tracks WHERE playlist_id=$1", pid
        )
        if total == 0:
            text = f"📋 «{html.escape(pl['name'])}» пуст."
            if edit:
                try:
                    await target.message.edit_text(text)
                except Exception:
                    await target.message.answer(text)
            else:
                await target.message.answer(text)
            return
        offset = page * PL_TRACK_PAGE_SIZE
        res = await conn.fetch(
            "SELECT tracks.id, tracks.title, tracks.artist FROM tracks "
            "JOIN playlist_tracks ON tracks.id=playlist_tracks.track_id "
            "WHERE playlist_tracks.playlist_id=$1 ORDER BY playlist_tracks.added_at "
            "LIMIT $2 OFFSET $3",
            pid, PL_TRACK_PAGE_SIZE, offset
        )
    total_pages = math.ceil(total / PL_TRACK_PAGE_SIZE)
    ids = [r['id'] for r in res]
    lines = [
        f"{i + 1 + offset}. {html.escape(format_track(r['artist'], r['title']))}"
        for i, r in enumerate(res)
    ]
    h = (
        f"📋 «{html.escape(pl['name'])}» — {total} тр. • Стр. {page + 1}/{total_pages}\n\n"
        + "\n".join(lines)
    )
    play_rows = num_buttons(ids)
    rm_btns = [
        InlineKeyboardButton(text=f"🗑#{r['id']}", callback_data=f"rmpl_{pid}_{r['id']}_{page}")
        for r in res
    ]
    rm_rows = [rm_btns[i:i+5] for i in range(0, len(rm_btns), 5)]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"oplp_{pid}_{page - 1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"oplp_{pid}_{page + 1}"))
    rows = play_rows + rm_rows
    rows.append([InlineKeyboardButton(text="🎲 Случайный из плейлиста", callback_data=f"rnd_pl_{pid}")])
    if nav:
        rows.append(nav)
    if total_pages > 1:
        rows.append([InlineKeyboardButton(text=f"Стр. {page + 1} из {total_pages}", callback_data="noop")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    if edit:
        try:
            await target.message.edit_text(h, reply_markup=kb, parse_mode="HTML")
        except Exception:
            await target.message.answer(h, reply_markup=kb, parse_mode="HTML")
    else:
        await target.message.answer(h, reply_markup=kb, parse_mode="HTML")

# --- ХЭНДЛЕРЫ ---

@dp.callback_query(F.data == "check_sub")
async def check_sub_cb(cb: types.CallbackQuery):
    """Кнопка «Я подписался» — повторная проверка подписки."""
    SUB_CACHE.pop(cb.from_user.id, None)
    if await is_subscribed(cb.from_user.id):
        try:
            await cb.answer("✅ Подписка подтверждена!", show_alert=False)
        except Exception:
            pass
        try:
            if cb.message:
                await cb.message.edit_text(
                    "✅ Подписка подтверждена. Жми /start, чтобы начать пользоваться ботом."
                )
        except Exception:
            try:
                if cb.message:
                    await cb.message.answer(
                        "✅ Подписка подтверждена. Жми /start, чтобы начать."
                    )
            except Exception:
                pass
    else:
        try:
            await cb.answer(
                "❌ Подписка не найдена. Подпишись на канал и нажми кнопку ещё раз.",
                show_alert=True
            )
        except Exception:
            pass

@dp.message(Command("start"))
async def start_cmd(m: types.Message, state: FSMContext):
    await state.clear()
    parts = m.text.split(maxsplit=1)
    payload = parts[1].strip() if len(parts) > 1 else ""
    if payload.startswith("req_"):
        try:
            sid = int(payload[4:])
        except ValueError:
            sid = None
        if sid:
            async with db_pool.acquire() as conn:
                session = await conn.fetchrow(
                    "SELECT id, status FROM request_sessions WHERE id=$1", sid
                )
            if not session or session['status'] != 'active':
                await m.answer(
                    "❌ Сбор заявок уже закрыт. Следи за каналом — будет новый пост.",
                    reply_markup=menu
                )
                return
            await state.set_state(RequestForm.waiting_text)
            await state.update_data(session_id=sid)
            await m.answer(
                "📝 Напиши <b>одним сообщением</b> исполнителя и название трека, "
                "который хочешь следующим.\n\nНапример: <code>Eminem — Mockingbird</code>\n\n"
                "Отменить — /cancel",
                parse_mode="HTML"
            )
            return
    await m.answer("🎧 Бот запущен", reply_markup=menu)

@dp.message(RequestForm.waiting_text, F.text & ~F.text.startswith("/"))
async def request_text_handler(m: types.Message, state: FSMContext):
    text = (m.text or "").strip()
    if not text:
        await m.answer("❌ Пусто. Напиши название трека или /cancel")
        return
    if len(text) > 500:
        text = text[:500]
    data = await state.get_data()
    sid = data.get("session_id")
    if not sid:
        await state.clear()
        await m.answer("❌ Сессия потеряна. Попробуй снова через пост в канале.")
        return
    async with db_pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT id, status FROM request_sessions WHERE id=$1", sid
        )
        if not session or session['status'] != 'active':
            await state.clear()
            await m.answer("❌ Сбор заявок уже закрыт.")
            return
        err_code, err_text = await _request_limit_status(conn, sid, m.from_user.id)
        if err_code:
            await state.clear()
            await m.answer(err_text, reply_markup=menu)
            return
        await conn.execute(
            "INSERT INTO track_requests "
            "(session_id, user_id, username, full_name, text) "
            "VALUES ($1, $2, $3, $4, $5)",
            sid,
            m.from_user.id,
            m.from_user.username,
            m.from_user.full_name,
            text
        )
    await state.clear()
    await m.answer("✅ Заявка принята, спасибо!", reply_markup=menu)

@dp.channel_post(F.audio)
async def save_track(m: types.Message):
    t = m.audio.title or "Unknown"
    a = m.audio.performer or "Unknown"
    f = m.audio.file_id
    try:
        async with db_pool.acquire() as conn:
            result = await conn.execute(
                "INSERT INTO tracks (title, artist, file_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                t, a, f
            )
        if result == "INSERT 0 1" and ADMIN_ID:
            track_name = html.escape(format_track(a, t))
            await bot.send_message(
                ADMIN_ID,
                f"🎵 Новый трек из канала:\n<b>{track_name}</b>",
                parse_mode="HTML"
            )
    except Exception as e:
        logging.error(f"Ошибка сохранения трека: {e}")

@dp.message(F.audio)
async def imp_track(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    t = m.audio.title or "Unknown"
    a = m.audio.performer or "Unknown"
    f = m.audio.file_id
    pool = await get_db()
    async with pool.acquire() as conn:
        try:
            if await conn.fetchval("SELECT 1 FROM tracks WHERE file_id=$1", f):
                await m.answer("⚠️ Уже есть")
                return
            await conn.execute("INSERT INTO tracks (title, artist, file_id) VALUES ($1, $2, $3)", t, a, f)
            await m.answer(f"✅ Сохранено: {a} — {t}")
        except Exception as e:
            logging.error(f"Ошибка импорта трека: {e}")

async def _send_random_track(target, user_id: int, query: str = ""):
    """Отправить случайный трек. query — SQL WHERE-условие (без WHERE)."""
    try:
        async with db_pool.acquire() as conn:
            bounds = await conn.fetchrow(
                f"SELECT MIN(id) AS mn, MAX(id) AS mx FROM tracks{' WHERE ' + query if query else ''}"
            )
            if not bounds or bounds['mx'] is None:
                text = "❌ База пуста" if not query else "❌ Нет треков"
                if isinstance(target, types.Message):
                    await target.answer(text)
                else:
                    await target.answer(text, show_alert=True)
                return
            rand_id = random.randint(bounds['mn'], bounds['mx'])
            sql = f"SELECT id, file_id FROM tracks WHERE id >= $1{' AND ' + query if query else ''} ORDER BY id LIMIT 1"
            r = await conn.fetchrow(sql, rand_id)
            if not r:
                sql_fallback = f"SELECT id, file_id FROM tracks{' WHERE ' + query if query else ''} ORDER BY id LIMIT 1"
                r = await conn.fetchrow(sql_fallback)
            if not r:
                if isinstance(target, types.Message):
                    await target.answer("❌ Нет треков")
                else:
                    await target.answer("❌ Нет треков", show_alert=True)
                return
            await conn.execute("UPDATE tracks SET plays=plays+1 WHERE id=$1", r['id'])
        kb = await track_keyboard(r['id'], user_id)
        if isinstance(target, types.Message):
            await target.answer_audio(r['file_id'], reply_markup=kb)
        else:
            await target.message.answer_audio(r['file_id'], reply_markup=kb)
            await target.answer()
    except Exception as e:
        logging.error(f"Ошибка в _send_random_track: {e}")

@dp.message(F.text == "🎲 Случайный")
async def rnd(m: types.Message):
    await _send_random_track(m, m.from_user.id)

@dp.message(F.text == "🔍 Поиск")
async def sb(m: types.Message, state: FSMContext):
    await state.clear()
    await m.answer("🔍 Пиши запрос:")

@dp.message(F.text == "❤️ Избранное")
async def sf(m: types.Message):
    try:
        await show_fav_page(m.from_user.id, 0, m)
    except Exception as e:
        logging.error(f"Ошибка в sf: {e}")

@dp.callback_query(F.data.startswith("favp_"))
async def fav_page_cb(c: types.CallbackQuery):
    try:
        page = int(c.data.split("_")[1])
        await show_fav_page(c.from_user.id, page, c, edit=True)
        await c.answer()
    except Exception as e:
        logging.error(f"Ошибка в fav_page_cb: {e}")
        await c.answer("❌ Ошибка", show_alert=True)

@dp.message(F.text == "🔥 Топ")
async def top(m: types.Message):
    pool = await get_db()
    try:
        async with pool.acquire() as conn:
            res = await conn.fetch("SELECT artist, title, plays FROM tracks ORDER BY plays DESC LIMIT 10")
        if not res:
            await m.answer("❌ Пусто")
            return
        medals = ["🥇","🥈","🥉"]
        lines = [
            f"{medals[i-1] if i<=3 else f'{i}.'} {html.escape(format_track(t['artist'], t['title']))} — {t['plays']} 🎧"
            for i, t in enumerate(res, 1)
        ]
        await m.answer("🔥 <b>Топ:</b>\n\n" + "\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logging.error(f"Ошибка в top: {e}")

@dp.message(F.text == "🆕 Новое")
async def new_tracks(m: types.Message):
    try:
        async with db_pool.acquire() as conn:
            res = await conn.fetch(
                "SELECT id, title, artist, created_at FROM tracks ORDER BY created_at DESC LIMIT 10"
            )
        if not res:
            await m.answer("❌ Пусто")
            return
        lines = [
            f"{i}. {html.escape(format_track(r['artist'], r['title']))} — <i>{r['created_at'].strftime('%d.%m.%Y')}</i>"
            for i, r in enumerate(res, 1)
        ]
        ids = [r['id'] for r in res]
        kb_rows = num_buttons(ids)
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
        await m.answer("🆕 <b>Новинки:</b>\n\n" + "\n".join(lines), reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Ошибка в new_tracks: {e}")

@dp.callback_query(F.data == "rnd_fav")
async def rnd_fav_cb(c: types.CallbackQuery):
    try:
        async with db_pool.acquire() as conn:
            track_ids = await conn.fetch(
                "SELECT track_id FROM favorites WHERE user_id=$1", c.from_user.id
            )
        if not track_ids:
            await c.answer("❌ Избранное пусто", show_alert=True)
            return
        ids = [r['track_id'] for r in track_ids]
        chosen = random.choice(ids)
        async with db_pool.acquire() as conn:
            r = await conn.fetchrow("SELECT id, file_id FROM tracks WHERE id=$1", chosen)
            if r:
                await conn.execute("UPDATE tracks SET plays=plays+1 WHERE id=$1", chosen)
        kb = await track_keyboard(r['id'], c.from_user.id)
        await c.message.answer_audio(r['file_id'], reply_markup=kb)
        await c.answer()
    except Exception as e:
        logging.error(f"Ошибка в rnd_fav_cb: {e}")
        await c.answer("❌ Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("rnd_pl_"))
async def rnd_pl_cb(c: types.CallbackQuery):
    try:
        pid = int(c.data.split("_")[2])
        async with db_pool.acquire() as conn:
            pl = await conn.fetchrow(
                "SELECT id FROM playlists WHERE id=$1 AND user_id=$2", pid, c.from_user.id
            )
            if not pl:
                await c.answer("❌", show_alert=True)
                return
            track_ids = await conn.fetch(
                "SELECT track_id FROM playlist_tracks WHERE playlist_id=$1", pid
            )
        if not track_ids:
            await c.answer("❌ Плейлист пуст", show_alert=True)
            return
        ids = [r['track_id'] for r in track_ids]
        chosen = random.choice(ids)
        async with db_pool.acquire() as conn:
            r = await conn.fetchrow("SELECT id, file_id FROM tracks WHERE id=$1", chosen)
            if r:
                await conn.execute("UPDATE tracks SET plays=plays+1 WHERE id=$1", chosen)
        kb = await track_keyboard(r['id'], c.from_user.id)
        await c.message.answer_audio(r['file_id'], reply_markup=kb)
        await c.answer()
    except Exception as e:
        logging.error(f"Ошибка в rnd_pl_cb: {e}")
        await c.answer("❌ Ошибка", show_alert=True)

@dp.message(F.text == "📋 Список Плейлистов")
async def spl(m: types.Message, state: FSMContext):
    await state.clear()
    pool = await get_db()
    try:
        async with pool.acquire() as conn:
            res = await conn.fetch("SELECT id, name FROM playlists WHERE user_id=$1", m.from_user.id)
        count = len(res)
        rows = [
            [
                InlineKeyboardButton(text=f"📋 {p['name']}", callback_data=f"opl_{p['id']}"),
                InlineKeyboardButton(text="🗑", callback_data=f"delpl_{p['id']}")
            ]
            for p in res
        ]
        if count < MAX_PLAYLISTS:
            rows.append([InlineKeyboardButton(
                text=f"➕ Создать ({count}/{MAX_PLAYLISTS})", callback_data="plnew"
            )])
        else:
            rows.append([InlineKeyboardButton(
                text=f"⛔ Лимит {MAX_PLAYLISTS} плейлиста достигнут", callback_data="noop"
            )])
        await m.answer("📋 Плейлисты:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    except Exception as e:
        logging.error(f"Ошибка в spl: {e}")

@dp.callback_query(F.data == "plnew")
async def cpn(c: types.CallbackQuery, state: FSMContext):
    await state.set_state(PlaylistForm.waiting_name)
    await c.message.answer("✏️ Название:")
    await c.answer()

@dp.message(PlaylistForm.waiting_name)
async def rpn(m: types.Message, state: FSMContext):
    n = m.text.strip()
    if not n or len(n) > 50:
        await m.answer("❌ От 1 до 50 симв.")
        return
    pool = await get_db()
    try:
        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM playlists WHERE user_id=$1", m.from_user.id
            )
            if count >= MAX_PLAYLISTS:
                await state.clear()
                await m.answer(f"❌ Максимум {MAX_PLAYLISTS} плейлистов. Удали один перед созданием нового.")
                return
            await conn.execute("INSERT INTO playlists (user_id, name) VALUES ($1, $2)", m.from_user.id, n)
        await state.clear()
        await m.answer(f"✅ «{html.escape(n)}» создан!")
    except Exception as e:
        logging.error(f"Ошибка в rpn: {e}")

@dp.callback_query(F.data.startswith("delpl_"))
async def dpl(c: types.CallbackQuery):
    try:
        pid = int(c.data.split("_")[1])
        pool = await get_db()
        async with pool.acquire() as conn:
            if not await conn.fetchval("SELECT 1 FROM playlists WHERE id=$1 AND user_id=$2", pid, c.from_user.id):
                await c.answer("❌", show_alert=True)
                return
            await conn.execute("DELETE FROM playlists WHERE id=$1", pid)
        await c.answer("🗑 Удален", show_alert=True)
        try: await c.message.delete()
        except: pass
    except Exception as e:
        logging.error(f"Ошибка в dpl: {e}")

@dp.callback_query(F.data.startswith("opl_"))
async def opl(c: types.CallbackQuery):
    try:
        pid = int(c.data.split("_")[1])
        await show_playlist_page(pid, c.from_user.id, 0, c, edit=False)
        await c.answer()
    except Exception as e:
        logging.error(f"Ошибка в opl: {e}")

@dp.callback_query(F.data.startswith("oplp_"))
async def oplp(c: types.CallbackQuery):
    try:
        parts = c.data.split("_")
        pid, page = int(parts[1]), int(parts[2])
        await show_playlist_page(pid, c.from_user.id, page, c, edit=True)
        await c.answer()
    except Exception as e:
        logging.error(f"Ошибка в oplp: {e}")
        await c.answer("❌ Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("topl_"))
async def cpl(c: types.CallbackQuery):
    try:
        tid = int(c.data.split("_")[1])
        pool = await get_db()
        async with pool.acquire() as conn:
            pls = await conn.fetch("SELECT id, name FROM playlists WHERE user_id=$1", c.from_user.id)
        if not pls:
            await c.answer("Сначала создай плейлист", show_alert=True)
            return
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"📋 {p['name']}", callback_data=f"apl_{p['id']}_{tid}")]
            for p in pls
        ])
        await c.message.answer("Выбери:", reply_markup=kb)
        await c.answer()
    except Exception as e:
        logging.error(f"Ошибка в cpl: {e}")

@dp.callback_query(F.data.startswith("apl_"))
async def apl(c: types.CallbackQuery):
    try:
        p = c.data.split("_")
        pid, tid = int(p[1]), int(p[2])
        pool = await get_db()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO playlist_tracks (playlist_id, track_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                pid, tid
            )
        await c.answer("✅ Добавлено", show_alert=True)
        try: await c.message.delete()
        except: pass
    except Exception as e:
        logging.error(f"Ошибка в apl: {e}")

@dp.callback_query(F.data.startswith("rmpl_"))
async def rmpl(c: types.CallbackQuery):
    try:
        p = c.data.split("_")
        pid, tid = int(p[1]), int(p[2])
        page = int(p[3]) if len(p) > 3 else 0
        pool = await get_db()
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM playlist_tracks WHERE playlist_id=$1 AND track_id=$2",
                pid, tid
            )
        await c.answer("🗑 Убрано", show_alert=True)
        await show_playlist_page(pid, c.from_user.id, page, c, edit=True)
    except Exception as e:
        logging.error(f"Ошибка в rmpl: {e}")

@dp.callback_query(F.data.startswith("track_"))
async def st(c: types.CallbackQuery):
    try:
        tid = int(c.data.split("_")[1])
        pool = await get_db()
        async with pool.acquire() as conn:
            r = await conn.fetchrow("SELECT id, file_id FROM tracks WHERE id=$1", tid)
            if not r:
                await c.answer("❌ Трек не найден", show_alert=True)
                return
            await conn.execute("UPDATE tracks SET plays=plays+1 WHERE id=$1", tid)
        await c.message.answer_audio(r['file_id'], reply_markup=await track_keyboard(tid, c.from_user.id))
        await c.answer()
    except Exception as e:
        logging.error(f"Ошибка в st: {e}")

@dp.callback_query(F.data.startswith("del_"))
async def dt(c: types.CallbackQuery):
    if c.from_user.id != ADMIN_ID:
        await c.answer("❌", show_alert=True)
        return
    try:
        tid = int(c.data.split("_")[1])
        pool = await get_db()
        async with pool.acquire() as conn:
            if not await conn.fetchval("SELECT 1 FROM tracks WHERE id=$1", tid):
                await c.answer("❌ Не найден", show_alert=True)
                return
            await conn.execute("DELETE FROM tracks WHERE id=$1", tid)
        await c.answer("✅ Удалено", show_alert=True)
        try: await c.message.delete()
        except: pass
    except Exception as e:
        logging.error(f"Ошибка в dt: {e}")

@dp.callback_query(F.data.startswith("fav_"))
async def fav(c: types.CallbackQuery):
    try:
        tid = int(c.data.split("_")[1])
        pool = await get_db()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO favorites (user_id, track_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                c.from_user.id, tid
            )
        await c.answer("❤️ Добавлено в избранное", show_alert=True)
        try:
            await c.message.edit_reply_markup(reply_markup=await track_keyboard(tid, c.from_user.id))
        except: pass
    except Exception as e:
        logging.error(f"Ошибка в fav: {e}")

@dp.callback_query(F.data.startswith("unfav_"))
async def unfav(c: types.CallbackQuery):
    try:
        parts = c.data.split("_")
        if parts[1] == "f":
            tid = int(parts[2])
            page = int(parts[3]) if len(parts) > 3 else 0
            pool = await get_db()
            async with pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM favorites WHERE user_id=$1 AND track_id=$2",
                    c.from_user.id, tid
                )
            await c.answer("💔 Убрано из избранного", show_alert=True)
            await show_fav_page(c.from_user.id, page, c, edit=True)
        else:
            tid = int(parts[1])
            pool = await get_db()
            async with pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM favorites WHERE user_id=$1 AND track_id=$2",
                    c.from_user.id, tid
                )
            await c.answer("💔 Убрано из избранного", show_alert=True)
            try:
                await c.message.edit_reply_markup(reply_markup=await track_keyboard(tid, c.from_user.id))
            except: pass
    except Exception as e:
        logging.error(f"Ошибка в unfav: {e}")

# --- РЕКОМЕНДАЦИИ ---

@dp.callback_query(F.data.startswith("rec_"))
async def rec_cmd(c: types.CallbackQuery):
    try:
        tid = int(c.data.split("_")[1])
        pool = await get_db()
        async with pool.acquire() as conn:
            track = await conn.fetchrow("SELECT id, title, artist FROM tracks WHERE id=$1", tid)
            if not track:
                await c.answer("❌ Трек не найден", show_alert=True)
                return
            recs = []
            artist = track['artist']
            if artist and not artist.startswith('@') and artist not in ('Unknown', ''):
                recs = list(await conn.fetch(
                    "SELECT id, title, artist FROM tracks "
                    "WHERE artist ILIKE $1 AND id != $2 ORDER BY plays DESC LIMIT 5",
                    artist, tid
                ))
            if len(recs) < 5:
                need = 5 - len(recs)
                exclude = [tid] + [r['id'] for r in recs]
                placeholders = ', '.join(f'${i + 1}' for i in range(len(exclude)))
                top = list(await conn.fetch(
                    f"SELECT id, title, artist FROM tracks "
                    f"WHERE id NOT IN ({placeholders}) "
                    f"ORDER BY plays DESC LIMIT ${len(exclude) + 1}",
                    *exclude, need
                ))
                recs += top
        if not recs:
            await c.answer("Похожих треков нет", show_alert=True)
            return
        ids = [r['id'] for r in recs]
        lines = [
            f"{i + 1}. {html.escape(format_track(r['artist'], r['title']))}"
            for i, r in enumerate(recs)
        ]
        title_str = html.escape(format_track(track['artist'], track['title']))
        h = f"🎵 <b>Похожие на «{title_str}»:</b>\n\n" + "\n".join(lines)
        kb = InlineKeyboardMarkup(inline_keyboard=num_buttons(ids))
        await c.message.answer(h, reply_markup=kb, parse_mode="HTML")
        await c.answer()
    except Exception as e:
        logging.error(f"Ошибка в rec_cmd: {e}")
        await c.answer("❌ Ошибка", show_alert=True)

@dp.message(Command("cancel"))
async def cancel_cmd(m: types.Message, state: FSMContext):
    cur = await state.get_state()
    if cur:
        await state.clear()
        await m.answer("❌ Отменено", reply_markup=menu)
    else:
        await m.answer("Нечего отменять", reply_markup=menu)

# --- ДОНАТ (Telegram Stars) ---
DONATE_MIN = 1
DONATE_MAX = 100000

@dp.message(F.text == "💝 Донат")
async def donate_start(m: types.Message, state: FSMContext):
    await state.set_state(DonateForm.waiting_amount)
    await m.answer(
        "💝 <b>Поддержать бота</b>\n\n"
        "Спасибо, что хочешь помочь! 🙏\n"
        f"Введи сумму в Telegram Stars ⭐️ (от {DONATE_MIN} до {DONATE_MAX}).\n\n"
        "Для отмены: /cancel",
        parse_mode="HTML"
    )

@dp.message(DonateForm.waiting_amount)
async def donate_amount(m: types.Message, state: FSMContext):
    txt = (m.text or "").strip()
    if not txt.isdigit():
        await m.answer("❌ Введи целое число звёзд, например: <code>50</code>", parse_mode="HTML")
        return
    amount = int(txt)
    if amount < DONATE_MIN or amount > DONATE_MAX:
        await m.answer(f"❌ Сумма должна быть от {DONATE_MIN} до {DONATE_MAX} ⭐️")
        return
    await state.clear()
    try:
        await bot.send_invoice(
            chat_id=m.chat.id,
            title="Поддержка бота",
            description=f"Донат {amount} ⭐️ на развитие бота",
            payload=f"donate:{m.from_user.id}:{amount}",
            currency="XTR",
            prices=[LabeledPrice(label=f"Донат {amount} ⭐️", amount=amount)],
        )
    except Exception as e:
        logging.error(f"Ошибка отправки счёта: {e}")
        await m.answer("❌ Не удалось создать счёт. Попробуй позже.")

@dp.pre_checkout_query()
async def pre_checkout(q: PreCheckoutQuery):
    try:
        await bot.answer_pre_checkout_query(q.id, ok=True)
    except Exception as e:
        logging.error(f"Ошибка pre_checkout: {e}")

@dp.message(F.successful_payment)
async def on_payment(m: types.Message):
    sp = m.successful_payment
    amount = sp.total_amount
    await m.answer(
        f"💖 <b>Спасибо за поддержку!</b>\n\nТы задонатил <b>{amount} ⭐️</b>. Это очень помогает!",
        parse_mode="HTML",
        reply_markup=menu
    )
    if ADMIN_ID and m.from_user.id != ADMIN_ID:
        try:
            uname = f"@{m.from_user.username}" if m.from_user.username else m.from_user.full_name
            await bot.send_message(
                ADMIN_ID,
                f"💝 Новый донат: <b>{amount} ⭐️</b>\nОт: {html.escape(uname)} (id <code>{m.from_user.id}</code>)",
                parse_mode="HTML"
            )
        except Exception as e:
            logging.error(f"Не удалось уведомить админа о донате: {e}")

@dp.message(Command("stats"))
async def stats_cmd(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    pool = await get_db()
    try:
        async with pool.acquire() as conn:
            tc = await conn.fetchval("SELECT COUNT(*) FROM tracks")
            tp = await conn.fetchval("SELECT COALESCE(SUM(plays),0) FROM tracks")
            fc = await conn.fetchval("SELECT COUNT(*) FROM favorites")
            pc = await conn.fetchval("SELECT COUNT(*) FROM playlists")
            uc = await conn.fetchval("SELECT COUNT(*) FROM users WHERE is_active = TRUE")
        await m.answer(
            f"📊 <b>Статистика</b>\n\n"
            f"Треков: {tc}\nПрослушиваний: {tp}\nИзбранных: {fc}\nПлейлистов: {pc}\n"
            f"Пользователей: {uc}",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Ошибка в stats: {e}")

@dp.message(Command("topusers"))
async def topusers_cmd(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    try:
        async with db_pool.acquire() as conn:
            res = await conn.fetch("""
                SELECT u.user_id, u.first_name, u.username,
                       COUNT(DISTINCT f.track_id) AS fav_count,
                       COUNT(DISTINCT p.id) AS pl_count,
                       u.last_seen
                FROM users u
                LEFT JOIN favorites f ON f.user_id = u.user_id
                LEFT JOIN playlists p ON p.user_id = u.user_id
                WHERE u.is_active = TRUE
                GROUP BY u.user_id, u.first_name, u.username, u.last_seen
                ORDER BY fav_count DESC
                LIMIT 15
            """)
        if not res:
            await m.answer("❌ Нет пользователей")
            return
        lines = []
        for i, r in enumerate(res, 1):
            name = html.escape(r['first_name'] or "")
            uname = f" (@{r['username']})" if r['username'] else ""
            seen = r['last_seen'].strftime('%d.%m.%y') if r['last_seen'] else "—"
            lines.append(
                f"{i}. <b>{name}</b>{html.escape(uname)}\n"
                f"   ❤️ {r['fav_count']} • 📋 {r['pl_count']} • 🕐 {seen}"
            )
        await m.answer("👥 <b>Топ пользователей по избранному:</b>\n\n" + "\n\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logging.error(f"Ошибка в topusers_cmd: {e}")

@dp.message(Command("broadcast"))
async def broadcast_cmd(m: types.Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID: return
    try:
        async with db_pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM users WHERE is_active = TRUE")
        await state.set_state(BroadcastForm.waiting_text)
        await m.answer(
            f"📢 <b>Рассылка</b>\n\n"
            f"Активных пользователей: <b>{count}</b>\n\n"
            f"Отправь сообщение для рассылки (текст, фото, аудио, видео, стикер).\n"
            f"Для отмены: /cancel",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Ошибка в broadcast_cmd: {e}")

@dp.message(BroadcastForm.waiting_text)
async def broadcast_send(m: types.Message, state: FSMContext):
    await state.clear()
    try:
        async with db_pool.acquire() as conn:
            users = await conn.fetch("SELECT user_id FROM users WHERE is_active = TRUE")
        total = len(users)
        if total == 0:
            await m.answer("❌ Нет активных пользователей")
            return
        sent = 0
        failed = 0
        blocked = 0
        status_msg = await m.answer(f"📤 Отправляю... 0/{total}")
        for i, row in enumerate(users):
            uid = row['user_id']
            try:
                await m.copy_to(uid)
                sent += 1
            except Exception as e:
                err_text = str(e).lower()
                if any(w in err_text for w in ("blocked", "deactivated", "not found", "forbidden", "chat not found")):
                    blocked += 1
                    async with db_pool.acquire() as conn:
                        await conn.execute("UPDATE users SET is_active=FALSE WHERE user_id=$1", uid)
                else:
                    failed += 1
            if (i + 1) % 25 == 0:
                try:
                    await status_msg.edit_text(f"📤 Отправляю... {i+1}/{total}")
                except Exception:
                    pass
            await asyncio.sleep(0.05)
        await status_msg.edit_text(
            f"✅ <b>Рассылка завершена!</b>\n\n"
            f"📤 Отправлено: {sent}\n"
            f"🚫 Заблокировали бота: {blocked}\n"
            f"❌ Другие ошибки: {failed}\n"
            f"👥 Всего: {total}",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Ошибка в broadcast_send: {e}")
        await m.answer(f"❌ Ошибка рассылки: {e}")

def build_search_keyboard(query: str, offset: int, total: int) -> InlineKeyboardMarkup:
    total_pages = math.ceil(total / PAGE_SIZE)
    current_page = offset // PAGE_SIZE + 1
    nav = []
    q = query[:50]
    if offset > 0:
        nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"pg_{offset - PAGE_SIZE}:{q}"))
    if offset + PAGE_SIZE < total:
        nav.append(InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"pg_{offset + PAGE_SIZE}:{q}"))
    rows = []
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text=f"Страница {current_page} из {total_pages}", callback_data="noop")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.message(F.chat.type == "private", F.text & ~F.text.startswith("/") & ~F.text.in_({"🔍 Поиск","🎲 Случайный","🔥 Топ","🆕 Новое","❤️ Избранное","📋 Список Плейлистов","💝 Донат"}))
async def search(m: types.Message, state: FSMContext):
    cur = await state.get_state()
    if cur in (PlaylistForm.waiting_name, BroadcastForm.waiting_text, DonateForm.waiting_amount, RequestForm.waiting_text):
        return
    q = m.text.strip()
    if not q: return
    res, total = await run_search(q, offset=0)
    if total == 0:
        await m.answer("❌ Ничего не найдено")
        return
    ids = [r['id'] for r in res]
    lines = [f"{i+1}. {html.escape(format_track(r['artist'], r['title']))}" for i, r in enumerate(res)]
    total_pages = math.ceil(total / PAGE_SIZE)
    h = f"🔍 Найдено {total} • Страница 1 из {total_pages}\n\n" + "\n".join(lines)
    kb_rows = num_buttons(ids)
    nav_kb = build_search_keyboard(q, 0, total)
    kb_rows += nav_kb.inline_keyboard
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await m.answer(h, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "noop")
async def noop(c: types.CallbackQuery):
    await c.answer()

@dp.callback_query(F.data.startswith("pg_"))
async def page_nav(c: types.CallbackQuery):
    try:
        raw = c.data[3:]
        sep = raw.index(":")
        offset = int(raw[:sep])
        q = raw[sep+1:]
        res, total = await run_search(q, offset=offset)
        if total == 0:
            await c.answer("❌ Ничего не найдено", show_alert=True)
            return
        if not res:
            await c.answer("❌ Страница не найдена", show_alert=True)
            return
        ids = [r['id'] for r in res]
        lines = [f"{i+1}. {html.escape(format_track(r['artist'], r['title']))}" for i, r in enumerate(res)]
        total_pages = math.ceil(total / PAGE_SIZE)
        current_page = offset // PAGE_SIZE + 1
        h = f"🔍 Найдено {total} • Страница {current_page} из {total_pages}\n\n" + "\n".join(lines)
        kb_rows = num_buttons(ids)
        nav_kb = build_search_keyboard(q, offset, total)
        kb_rows += nav_kb.inline_keyboard
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
        await c.message.edit_text(h, reply_markup=kb, parse_mode="HTML")
        await c.answer()
    except Exception as e:
        logging.error(f"Ошибка в page_nav: {e}")
        await c.answer("❌ Ошибка", show_alert=True)

@dp.message(Command("mg"))
async def mg_cmd(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    try:
        args = m.text.split(maxsplit=1)
        query = args[1].strip() if len(args) > 1 else None
        pool = await get_db()
        async with pool.acquire() as conn:
            if query:
                var = get_var(query)
                if not var:
                    await m.answer("❌ Пустой запрос")
                    return
                where_expr, score_expr, params, _ = _search_query_parts(var)
                sql = (
                    f"SELECT id, title, artist FROM ("
                    f"  SELECT id, title, artist, plays, ({score_expr}) AS score "
                    f"  FROM tracks WHERE {where_expr}"
                    f") sub "
                    f"ORDER BY score DESC, plays DESC, id DESC LIMIT 30"
                )
                res = await conn.fetch(sql, *params)
                header = f"🔍 По запросу «{html.escape(query)}»:"
            else:
                res = await conn.fetch("SELECT id, title, artist FROM tracks ORDER BY id DESC LIMIT 30")
                header = "🎵 <b>Последние 30 треков:</b>"
        if not res:
            await m.answer("❌ Ничего не найдено")
            return
        lines = [f"{i+1}. [#{r['id']}] {html.escape(format_track(r['artist'], r['title']))}" for i, r in enumerate(res)]
        h = header + "\n\n" + "\n".join(lines)
        del_buttons = [InlineKeyboardButton(text=f"🗑#{r['id']}", callback_data=f"del_{r['id']}") for r in res]
        del_rows = [del_buttons[i:i+5] for i in range(0, len(del_buttons), 5)]
        kb = InlineKeyboardMarkup(inline_keyboard=del_rows)
        await m.answer(h, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Ошибка в mg: {e}")
        await m.answer(f"❌ Ошибка: {e}")

# --- ГОЛОСОВАНИЕ ---

_MONTHS_RU = ["янв", "фев", "мар", "апр", "май", "июн",
              "июл", "авг", "сен", "окт", "ноя", "дек"]

def current_period(vote_type: str) -> str:
    today = datetime.date.today()
    if vote_type == 'day':
        return today.strftime("%d.%m.%Y")
    weekday = today.weekday()
    monday = today - datetime.timedelta(days=weekday)
    sunday = monday + datetime.timedelta(days=6)
    if monday.month == sunday.month:
        return f"{monday.day}–{sunday.day} {_MONTHS_RU[monday.month - 1]} {sunday.year}"
    return (f"{monday.day} {_MONTHS_RU[monday.month - 1]} – "
            f"{sunday.day} {_MONTHS_RU[sunday.month - 1]} {sunday.year}")

async def get_vote_counts(session_id: int):
    pool = await get_db()
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT vc.track_id, t.title, t.artist,
                   COUNT(v.id) AS vote_count
            FROM vote_candidates vc
            JOIN tracks t ON t.id = vc.track_id
            LEFT JOIN votes v ON v.track_id = vc.track_id AND v.session_id = vc.session_id
            WHERE vc.session_id = $1
            GROUP BY vc.track_id, t.title, t.artist
            ORDER BY vote_count DESC, vc.track_id
        """, session_id)

def _split_period(period: str):
    """Возвращает (date_part, artist_or_None). Период с артистом хранится как 'date|artist'."""
    if period and '|' in period:
        d, a = period.split('|', 1)
        return d, a
    return period, None

def _vote_short_label(vote_type: str, period: str) -> str:
    date_part, artist = _split_period(period)
    if vote_type == 'day':
        base = "Трек дня"
    elif vote_type == 'week':
        base = "Трек недели"
    elif vote_type == 'artist':
        return f"Лучшие треки {period}"
    else:
        return vote_type
    return f"{base} — {artist}" if artist else base

def build_vote_text(vote_type: str, period: str, rows, closed=False, closes_at=None) -> str:
    date_part, artist = _split_period(period)
    if vote_type == 'artist':
        title = f"Лучшие треки — {html.escape(period)}"
        period_suffix = ""
    else:
        label = "дня" if vote_type == 'day' else "недели"
        if artist:
            title = f"Трек {label} — {html.escape(artist)}"
        else:
            title = f"Трек {label}"
        period_suffix = f" • {date_part}"
    if closed:
        header = f"🏆 <b>Результаты голосования — {title}</b>"
    else:
        header = f"🗳 <b>Голосование — {title}</b>"
    total = sum(r['vote_count'] for r in rows)
    medals = ["🥇", "🥈", "🥉"]
    lines = []
    for i, r in enumerate(rows):
        pct = round(r['vote_count'] / total * 100) if total > 0 else 0
        filled = round(pct / 10)
        bar = "█" * filled + "░" * (10 - filled)
        if closed and i == 0:
            prefix = "🥇"
        elif closed and i < 3:
            prefix = medals[i]
        else:
            prefix = f"{i + 1}."
        track_name = html.escape(format_track(r['artist'], r['title']))
        count_str = f"{r['vote_count']} гол." if r['vote_count'] != 1 else "1 гол."
        lines.append(
            f"{prefix} <b>{track_name}</b>\n"
            f"<code>{bar}</code> {count_str} ({pct}%)"
        )
    if closed:
        footer_parts = [f"📊 Всего проголосовало: <b>{total}</b>"]
        if rows:
            winner = html.escape(format_track(rows[0]['artist'], rows[0]['title']))
            footer_parts.append(f"🏅 Победитель: <b>{winner}</b>")
        footer = "\n".join(footer_parts)
    else:
        footer_parts = [f"👥 Проголосовало: <b>{total}</b>"]
        if closes_at:
            now = datetime.datetime.utcnow()
            delta = closes_at - now
            if delta.total_seconds() > 0:
                h = int(delta.total_seconds() // 3600)
                mn = int((delta.total_seconds() % 3600) // 60)
                footer_parts.append(f"⏰ Закроется через: {h}ч {mn}мин")
        footer_parts.append("👇🏼 Нажми на кнопку ниже чтобы проголосовать • 1 голос")
        footer = "\n".join(footer_parts)
    return f"{header}{period_suffix}\n\n" + "\n\n".join(lines) + f"\n\n{footer}"

def build_vote_keyboard(session_id: int, rows, closed=False) -> InlineKeyboardMarkup:
    if closed:
        return InlineKeyboardMarkup(inline_keyboard=[])
    medals = ["🥇", "🥈", "🥉"]
    total = sum(r['vote_count'] for r in rows)
    buttons = []
    for i, r in enumerate(rows):
        pct = round(r['vote_count'] / total * 100) if total > 0 else 0
        prefix = medals[i] if i < 3 else f"{i + 1}."
        name = format_track(r['artist'], r['title'])[:30]
        label = f"{prefix} {name} — {r['vote_count']} ({pct}%)"
        buttons.append([InlineKeyboardButton(
            text=label,
            callback_data=f"vote_{session_id}_{r['track_id']}"
        )])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def _start_vote(m: types.Message, vote_type: str, limit: int, hours: int = None, artist: str = None):
    if m.from_user.id != ADMIN_ID: return
    channel_id = get_channel_id()
    if not channel_id:
        await m.answer("❌ CHANNEL_ID не задан в переменных окружения")
        return
    date_period = current_period(vote_type)
    canonical_artist = None
    async with db_pool.acquire() as conn:
        if artist:
            cands = await _find_artist_candidates(conn, artist, limit=8)
            if not cands:
                await m.answer(f"❌ Исполнитель «{html.escape(artist)}» не найден в базе")
                return
            if len(cands) > 1 and not cands[0]['exact']:
                await m.answer(_format_artist_candidates(cands, artist), parse_mode="HTML")
                return
            canonical_artist = cands[0]['artist']
        period = f"{date_period}|{canonical_artist}" if canonical_artist else date_period
        existing = await conn.fetchval(
            "SELECT id FROM vote_sessions WHERE vote_type=$1 AND period=$2 AND status='active'",
            vote_type, period
        )
        if existing:
            if canonical_artist:
                await m.answer(f"⚠️ Голосование за {html.escape(canonical_artist)} ({date_period}) уже идёт")
            else:
                label = "сегодня" if vote_type == 'day' else "эту неделю"
                await m.answer(f"⚠️ Голосование за {label} уже запущено")
            return
        if canonical_artist:
            tracks = await _fetch_vote_tracks(conn, cands[0], limit)
        else:
            tracks = await conn.fetch(
                "SELECT id, title, artist FROM tracks ORDER BY plays DESC LIMIT $1", limit
            )
        if len(tracks) < 2:
            who = f"у {html.escape(canonical_artist)}" if canonical_artist else "в базе"
            await m.answer(f"❌ Мало треков {who} (нужно минимум 2)")
            return
        await conn.execute(
            "DELETE FROM vote_sessions WHERE vote_type=$1 AND period=$2 AND status<>'active'",
            vote_type, period
        )
        closes_at = datetime.datetime.utcnow() + datetime.timedelta(hours=hours) if hours else None
        session_id = await conn.fetchval(
            "INSERT INTO vote_sessions (vote_type, period, closes_at) VALUES ($1, $2, $3) RETURNING id",
            vote_type, period, closes_at
        )
        await conn.executemany(
            "INSERT INTO vote_candidates (session_id, track_id) VALUES ($1, $2)",
            [(session_id, t['id']) for t in tracks]
        )
    rows = await get_vote_counts(session_id)
    text = build_vote_text(vote_type, period, rows, closes_at=closes_at)
    kb = build_vote_keyboard(session_id, rows)
    try:
        msg = await bot.send_message(channel_id, text, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        await m.answer(f"❌ Не удалось отправить в канал: {e}")
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE vote_sessions SET channel_message_id=$1, channel_chat_id=$2 WHERE id=$3",
            msg.message_id, channel_id, session_id
        )
    label_name = _vote_short_label(vote_type, period)
    timer_info = f" Закроется через {hours}ч." if hours else ""
    await m.answer(f"✅ Голосование «{html.escape(label_name)}» запущено! {len(tracks)} треков.{timer_info}", parse_mode="HTML")

async def _do_close_vote(session: dict):
    """Закрыть голосование по объекту сессии (используется вручную и авто-таймером)."""
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE vote_sessions SET status='closed' WHERE id=$1", session['id'])
    rows = await get_vote_counts(session['id'])
    text = build_vote_text(session['vote_type'], session['period'], rows, closed=True)
    kb = build_vote_keyboard(session['id'], rows, closed=True)
    if session['channel_message_id'] and session['channel_chat_id']:
        try:
            await bot.edit_message_text(
                text,
                chat_id=session['channel_chat_id'],
                message_id=session['channel_message_id'],
                parse_mode="HTML",
                reply_markup=kb
            )
        except Exception as e:
            logging.warning(f"Не удалось обновить сообщение в канале: {e}")
    return rows

async def _close_vote(m: types.Message, vote_type: str):
    if m.from_user.id != ADMIN_ID: return
    pool = await get_db()
    async with pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT * FROM vote_sessions WHERE vote_type=$1 AND status='active' "
            "ORDER BY created_at DESC LIMIT 1",
            vote_type
        )
        if not session:
            await m.answer("❌ Нет активного голосования")
            return
    rows = await _do_close_vote(dict(session))
    if rows:
        winner = rows[0]
        total = sum(r['vote_count'] for r in rows)
        await m.answer(
            f"✅ Голосование закрыто!\n"
            f"🏆 Победитель: <b>{html.escape(format_track(winner['artist'], winner['title']))}</b>\n"
            f"Голосов: {winner['vote_count']} из {total}",
            parse_mode="HTML"
        )
    else:
        await m.answer("✅ Голосование закрыто. Голосов не было.")

async def vote_auto_close_loop():
    """Фоновая задача: автоматически закрывает голосования по истечении таймера."""
    while True:
        try:
            await asyncio.sleep(60)
            async with db_pool.acquire() as conn:
                expired = await conn.fetch(
                    "SELECT * FROM vote_sessions "
                    "WHERE status='active' AND closes_at IS NOT NULL AND closes_at <= NOW()"
                )
            for session in expired:
                logging.info(f"Авто-закрытие голосования #{session['id']} ({session['vote_type']})")
                try:
                    await _do_close_vote(dict(session))
                    if ADMIN_ID:
                        label = _vote_short_label(session['vote_type'], session['period'])
                        await bot.send_message(
                            ADMIN_ID,
                            f"⏰ Голосование «{label}» автоматически закрыто.",
                        )
                except Exception as e:
                    logging.error(f"Ошибка авто-закрытия голосования #{session['id']}: {e}")
        except Exception as e:
            logging.error(f"Ошибка в vote_auto_close_loop: {e}")

@dp.message(Command("debugenv"))
async def debug_env(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    raw = os.environ.get("CHANNEL_ID")
    parsed = get_channel_id()
    await m.answer(
        f"<b>Диагностика CHANNEL_ID</b>\n\n"
        f"Сырое значение: <code>{repr(raw)}</code>\n"
        f"После парсинга: <code>{parsed}</code>",
        parse_mode="HTML"
    )

_WS_RE = re.compile(r'\s+')
_ALNUM_RE = re.compile(r'[^a-z0-9а-яё]+')

def _normalize_str(s: str) -> str:
    """Нормализует строку для сравнения: убирает невидимые символы,
    NFKC-нормализация, нижний регистр, обрезка и схлопывание пробелов."""
    if not s:
        return ""
    for ch in ('\u200b', '\u200c', '\u200d', '\ufeff', '\u00a0'):
        s = s.replace(ch, ' ' if ch == '\u00a0' else '')
    s = unicodedata.normalize('NFKC', s)
    s = s.lower().strip()
    s = _WS_RE.sub(' ', s)
    return s

def _alnum_only(s: str) -> str:
    """Только буквы и цифры в нижнем регистре (всё остальное удаляется)."""
    return _ALNUM_RE.sub('', _normalize_str(s))

def _artist_keys(name: str) -> set:
    """Множество ключей для сравнения имени артиста: нормализованные
    варианты + версии без пробелов + только буквы/цифры + translit."""
    keys = set()
    n = _normalize_str(name)
    if not n:
        return keys
    keys.add(n)
    keys.add(n.replace(' ', ''))
    keys.add(_alnum_only(n))
    for v in get_var(n):
        nv = _normalize_str(v)
        if nv:
            keys.add(nv)
            keys.add(nv.replace(' ', ''))
            keys.add(_alnum_only(nv))
    keys.discard('')
    return keys

async def _find_artist_candidates(conn, query: str, limit: int = 8):
    """Ищет артистов в Python — это надёжнее, чем чистый SQL ILIKE,
    потому что мы можем нормализовать невидимые символы, схлопнуть
    повторные пробелы, применить NFKC и translit-варианты с обеих сторон.
    Возвращает [{"artist", "track_count", "exact"}] по убыванию релевантности.
    """
    q_keys = _artist_keys(query)
    if not q_keys:
        return []

    q_norm = _normalize_str(query)
    q_subs = set(k for k in q_keys if k)
    q_tokens = [t for t in q_norm.split(' ') if t]

    rows = await conn.fetch(
        "SELECT artist, COUNT(*) AS track_count FROM tracks "
        "WHERE artist IS NOT NULL AND artist <> '' "
        "GROUP BY artist"
    )

    exact_hits, sub_hits, token_hits = [], [], []
    for r in rows:
        artist = r['artist']
        a_keys = _artist_keys(artist)
        if not a_keys:
            continue
        if a_keys & q_keys:
            exact_hits.append((artist, r['track_count']))
            continue
        matched = False
        for ak in a_keys:
            for qs in q_subs:
                if qs and qs in ak:
                    sub_hits.append((artist, r['track_count']))
                    matched = True
                    break
            if matched:
                break
        if matched:
            continue
        if len(q_tokens) >= 2:
            if all(any(tok in ak for ak in a_keys) for tok in q_tokens):
                token_hits.append((artist, r['track_count']))

    def _sort(hits):
        hits.sort(key=lambda x: (-x[1], x[0].lower()))
        return hits

    if exact_hits:
        return [{"artist": a, "track_count": c, "exact": True, "match_field": "artist"}
                for a, c in _sort(exact_hits)[:limit]]
    if sub_hits:
        return [{"artist": a, "track_count": c, "exact": False, "match_field": "artist"}
                for a, c in _sort(sub_hits)[:limit]]
    if token_hits:
        return [{"artist": a, "track_count": c, "exact": False, "match_field": "artist"}
                for a, c in _sort(token_hits)[:limit]]

    # Fallback: ничего не нашли в колонке artist — ищем в title (как обычный поиск).
    # Возвращаем синтетический кандидат с меткой запроса.
    var = get_var(query)
    if var:
        where_parts = []
        params = []
        for v in var:
            v_esc = _esc_like(v)
            params.append('%' + v_esc + '%')
            where_parts.append(f"title ILIKE ${len(params)}")
        if where_parts:
            where = ' OR '.join(where_parts)
            cnt = await conn.fetchval(
                f"SELECT COUNT(*) FROM tracks WHERE {where}", *params
            )
            if cnt and cnt > 0:
                return [{
                    "artist": query.strip(),
                    "track_count": int(cnt),
                    "exact": True,
                    "match_field": "title",
                }]
    return []

async def _fetch_vote_tracks(conn, cand, limit: int):
    """Достаёт треки для голосования по найденному кандидату-артисту.
    Если совпадение было по колонке title — ищет по title (через ILIKE
    со всеми вариантами транслита), иначе строго по artist=$1."""
    if cand.get('match_field') == 'title':
        var = get_var(cand['artist'])
        where_parts = []
        params = []
        for v in var:
            v_esc = _esc_like(v)
            params.append('%' + v_esc + '%')
            where_parts.append(f"title ILIKE ${len(params)}")
        if not where_parts:
            return []
        where = ' OR '.join(where_parts)
        params.append(limit)
        return await conn.fetch(
            f"SELECT id, title, artist FROM tracks WHERE {where} "
            f"ORDER BY plays DESC, id LIMIT ${len(params)}",
            *params
        )
    return await conn.fetch(
        "SELECT id, title, artist FROM tracks WHERE artist=$1 "
        "ORDER BY plays DESC, id LIMIT $2",
        cand['artist'], limit
    )

async def _resolve_artist(conn, query: str):
    """Возвращает каноничное имя артиста или None (лучший кандидат)."""
    cands = await _find_artist_candidates(conn, query, limit=1)
    return cands[0]["artist"] if cands else None

@dp.message(Command("findartist"))
async def find_artist_cmd(m: types.Message):
    """Диагностика поиска артиста. Показывает, кого находит бот по запросу."""
    if m.from_user.id != ADMIN_ID:
        return
    args = m.text.split(maxsplit=1)
    query = args[1].strip() if len(args) > 1 else ""
    if not query:
        await m.answer(
            "Использование: <code>/findartist &lt;запрос&gt;</code>",
            parse_mode="HTML"
        )
        return
    async with db_pool.acquire() as conn:
        total_artists = await conn.fetchval(
            "SELECT COUNT(DISTINCT artist) FROM tracks "
            "WHERE artist IS NOT NULL AND artist <> ''"
        )
        cands = await _find_artist_candidates(conn, query, limit=10)
    keys = sorted(_artist_keys(query))
    lines = [
        f"🔍 <b>Запрос:</b> <code>{html.escape(query)}</code>",
        f"📚 <b>Артистов в базе:</b> {total_artists}",
        f"🔑 <b>Ключи поиска ({len(keys)}):</b> <code>{html.escape(', '.join(keys))}</code>",
        "",
    ]
    if not cands:
        lines.append("❌ Совпадений не найдено")
        async with db_pool.acquire() as conn:
            sample = await conn.fetch(
                "SELECT artist, COUNT(*) AS c FROM tracks "
                "WHERE artist IS NOT NULL AND artist <> '' "
                "GROUP BY artist ORDER BY c DESC, artist ASC LIMIT 20"
            )
        if sample:
            lines.append("")
            lines.append(f"📂 <b>Что есть в базе (до 20):</b>")
            for r in sample:
                lines.append(
                    f"• <b>{html.escape(r['artist'])}</b> — {r['c']} тр."
                )
    else:
        lines.append(f"✅ <b>Найдено ({len(cands)}):</b>")
        for i, c in enumerate(cands, 1):
            mark = "★" if c['exact'] else "·"
            lines.append(
                f"{i}. {mark} <b>{html.escape(c['artist'])}</b> — {c['track_count']} тр."
            )
    await m.answer("\n".join(lines), parse_mode="HTML")

def _format_artist_candidates(cands, query: str) -> str:
    """Форматирует список кандидатов для показа админу."""
    lines = [f"❓ По запросу «{html.escape(query)}» нашлось несколько артистов:\n"]
    for i, c in enumerate(cands, 1):
        lines.append(f"{i}. <b>{html.escape(c['artist'])}</b> — {c['track_count']} тр.")
    lines.append("\nУточни имя в команде.")
    return "\n".join(lines)

def _parse_hours(arg: str):
    """Парсит аргумент часов. Возвращает (hours, error_msg)."""
    stripped = arg.strip()
    if not stripped:
        return None, None
    if stripped.isdigit():
        h = int(stripped)
        if h < 1:
            return None, "❌ Минимум 1 час."
        if h > 720:
            return None, "❌ Максимум 720 часов (30 дней)."
        return h, None
    return None, f"❌ Неверный формат времени: <code>{html.escape(stripped)}</code>\nУкажи целое число часов, например: /startday 24"

def _parse_artist_and_hours(text: str):
    """Разбирает '<команда> [артист...] [часов]'. Возвращает (artist_or_None, hours_or_None, err_or_None)."""
    parts = text.split()[1:]
    hours = None
    if parts and parts[-1].isdigit():
        h, err = _parse_hours(parts[-1])
        if err:
            return None, None, err
        hours = h
        parts = parts[:-1]
    artist = " ".join(parts).strip() or None
    return artist, hours, None

@dp.message(Command("startday"))
async def start_day(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    artist, hours, err = _parse_artist_and_hours(m.text)
    if err:
        await m.answer(err, parse_mode="HTML")
        return
    await _start_vote(m, 'day', limit=5, hours=hours, artist=artist)

@dp.message(Command("startweek"))
async def start_week(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    artist, hours, err = _parse_artist_and_hours(m.text)
    if err:
        await m.answer(err, parse_mode="HTML")
        return
    await _start_vote(m, 'week', limit=10, hours=hours, artist=artist)

@dp.message(Command("closeday"))
async def close_day(m: types.Message):
    await _close_vote(m, 'day')

@dp.message(Command("closeweek"))
async def close_week(m: types.Message):
    await _close_vote(m, 'week')

@dp.message(Command("startartist"))
async def start_artist(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    args = m.text.split(maxsplit=2)
    if len(args) < 2:
        await m.answer(
            "Использование: <code>/startartist &lt;исполнитель&gt; [часов]</code>\n"
            "Например: <code>/startartist Eminem 24</code>",
            parse_mode="HTML"
        )
        return
    artist_query = args[1].strip()
    hours, err = _parse_hours(args[2] if len(args) > 2 else "")
    if err:
        await m.answer(err, parse_mode="HTML")
        return
    channel_id = get_channel_id()
    if not channel_id:
        await m.answer("❌ CHANNEL_ID не задан в переменных окружения")
        return
    async with db_pool.acquire() as conn:
        cands = await _find_artist_candidates(conn, artist_query, limit=8)
        if not cands:
            await m.answer(f"❌ Исполнитель «{html.escape(artist_query)}» не найден в базе")
            return
        if len(cands) > 1 and not cands[0]['exact']:
            await m.answer(_format_artist_candidates(cands, artist_query), parse_mode="HTML")
            return
        canonical = cands[0]['artist']
        existing = await conn.fetchval(
            "SELECT id FROM vote_sessions WHERE vote_type='artist' AND period=$1 AND status='active'",
            canonical
        )
        if existing:
            await m.answer(f"⚠️ Голосование за {html.escape(canonical)} уже идёт")
            return
        tracks = await _fetch_vote_tracks(conn, cands[0], 10)
        if len(tracks) < 2:
            await m.answer(f"❌ У {html.escape(canonical)} меньше 2 треков в базе")
            return
        await conn.execute(
            "DELETE FROM vote_sessions WHERE vote_type='artist' AND period=$1 AND status<>'active'",
            canonical
        )
        closes_at = datetime.datetime.utcnow() + datetime.timedelta(hours=hours) if hours else None
        session_id = await conn.fetchval(
            "INSERT INTO vote_sessions (vote_type, period, closes_at) "
            "VALUES ('artist', $1, $2) RETURNING id",
            canonical, closes_at
        )
        await conn.executemany(
            "INSERT INTO vote_candidates (session_id, track_id) VALUES ($1, $2)",
            [(session_id, t['id']) for t in tracks]
        )
    rows = await get_vote_counts(session_id)
    text = build_vote_text('artist', canonical, rows, closes_at=closes_at)
    kb = build_vote_keyboard(session_id, rows)
    try:
        msg = await bot.send_message(channel_id, text, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        await m.answer(f"❌ Не удалось отправить в канал: {e}")
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE vote_sessions SET channel_message_id=$1, channel_chat_id=$2 WHERE id=$3",
            msg.message_id, channel_id, session_id
        )
    timer_info = f" Закроется через {hours}ч." if hours else ""
    await m.answer(
        f"✅ Голосование «Лучшие треки {html.escape(canonical)}» запущено! "
        f"{len(tracks)} треков (по числу прослушиваний).{timer_info}",
        parse_mode="HTML"
    )

@dp.message(Command("closeartist"))
async def close_artist(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    args = m.text.split(maxsplit=1)
    artist_query = args[1].strip() if len(args) > 1 else None
    async with db_pool.acquire() as conn:
        if artist_query:
            session = await conn.fetchrow(
                "SELECT * FROM vote_sessions WHERE vote_type='artist' AND status='active' "
                "AND period ILIKE $1 ORDER BY created_at DESC LIMIT 1",
                artist_query
            )
        else:
            session = await conn.fetchrow(
                "SELECT * FROM vote_sessions WHERE vote_type='artist' AND status='active' "
                "ORDER BY created_at DESC LIMIT 1"
            )
    if not session:
        await m.answer("❌ Нет активного голосования по исполнителю")
        return
    rows = await _do_close_vote(dict(session))
    if rows:
        winner = rows[0]
        total = sum(r['vote_count'] for r in rows)
        winner_name = html.escape(format_track(winner['artist'], winner['title']))
        await m.answer(
            f"✅ Голосование закрыто.\n🏆 Победитель: <b>{winner_name}</b>\n"
            f"👥 Всего голосов: {total}",
            parse_mode="HTML"
        )
    else:
        await m.answer("✅ Голосование закрыто.")

@dp.message(Command("votestatus"))
async def vote_status(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    pool = await get_db()
    async with pool.acquire() as conn:
        sessions = await conn.fetch(
            "SELECT * FROM vote_sessions WHERE status='active' ORDER BY created_at DESC"
        )
    if not sessions:
        await m.answer("Нет активных голосований")
        return
    for s in sessions:
        rows = await get_vote_counts(s['id'])
        total = sum(r['vote_count'] for r in rows)
        label = _vote_short_label(s['vote_type'], s['period'])
        lines = [f"{i+1}. {html.escape(format_track(r['artist'], r['title']))} — {r['vote_count']} гол."
                 for i, r in enumerate(rows)]
        await m.answer(
            f"📊 <b>{html.escape(label)}</b>\nВсего голосов: {total}\n\n" + "\n".join(lines),
            parse_mode="HTML"
        )

@dp.callback_query(F.data.startswith("vote_"))
async def handle_vote(c: types.CallbackQuery):
    try:
        parts = c.data.split("_")
        session_id, track_id = int(parts[1]), int(parts[2])
        pool = await get_db()
        async with pool.acquire() as conn:
            session = await conn.fetchrow("SELECT * FROM vote_sessions WHERE id=$1", session_id)
            if not session or session['status'] != 'active':
                await c.answer("❌ Голосование уже закрыто", show_alert=True)
                return
            existing = await conn.fetchval(
                "SELECT track_id FROM votes WHERE session_id=$1 AND user_id=$2",
                session_id, c.from_user.id
            )
            if existing is not None:
                if existing == track_id:
                    await c.answer("Ты уже голосовал за этот трек", show_alert=True)
                else:
                    await c.answer("Ты уже проголосовал в этом голосовании", show_alert=True)
                return
            is_candidate = await conn.fetchval(
                "SELECT 1 FROM vote_candidates WHERE session_id=$1 AND track_id=$2",
                session_id, track_id
            )
            if not is_candidate:
                await c.answer("❌ Трек не найден", show_alert=True)
                return
            await conn.execute(
                "INSERT INTO votes (session_id, track_id, user_id) VALUES ($1, $2, $3)",
                session_id, track_id, c.from_user.id
            )
        rows = await get_vote_counts(session_id)
        text = build_vote_text(session['vote_type'], session['period'], rows, closes_at=session['closes_at'])
        kb = build_vote_keyboard(session_id, rows)
        try:
            await c.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
        except Exception:
            pass
        voted_row = next((r for r in rows if r['track_id'] == track_id), None)
        name = html.escape(format_track(voted_row['artist'], voted_row['title'])) if voted_row else "трек"
        await c.answer(f"✅ Голос за «{name}» засчитан!", show_alert=True)
    except Exception as e:
        logging.error(f"Ошибка в handle_vote: {e}")
        await c.answer("❌ Ошибка", show_alert=True)

# --- ЗАЯВКИ НА ТРЕКИ (ПОСТ + КОММЕНТАРИИ В ОБСУЖДАЛКЕ) ---

REQUEST_DEFAULT_TEXT = (
    "🎵 <b>Какую песню хотите следующей?</b>\n\n"
    "Нажмите кнопку ниже и напишите боту в личку: исполнитель — название."
)
MAX_REQUESTS_PER_USER = 3
MAX_REQUESTS_PER_SESSION = 500

async def _request_limit_status(conn, session_id: int, user_id: int):
    """Проверка лимитов заявок. Возвращает (None, None) если можно,
    иначе (код_ошибки, текст_для_пользователя)."""
    total = await conn.fetchval(
        "SELECT COUNT(*) FROM track_requests WHERE session_id=$1", session_id
    )
    if total and total >= MAX_REQUESTS_PER_SESSION:
        return ("session_full",
                "❌ Лимит заявок в этой сессии исчерпан. Жди следующего поста.")
    if user_id:
        from_user = await conn.fetchval(
            "SELECT COUNT(*) FROM track_requests "
            "WHERE session_id=$1 AND user_id=$2",
            session_id, user_id
        )
        if from_user and from_user >= MAX_REQUESTS_PER_USER:
            return ("user_limit",
                    f"❌ Ты уже отправил {MAX_REQUESTS_PER_USER} "
                    f"{'заявку' if MAX_REQUESTS_PER_USER == 1 else 'заявки'} "
                    f"в этой сессии — это максимум.")
    return (None, None)

@dp.message(Command("askreq"))
async def ask_requests_cmd(m: types.Message):
    """Опубликовать в канале пост-приглашение для заявок.
    Использование: /askreq [текст поста]. Без аргумента — текст по умолчанию."""
    if m.from_user.id != ADMIN_ID:
        return
    channel_id = get_channel_id()
    if not channel_id:
        await m.answer("❌ CHANNEL_ID не задан в переменных окружения")
        return
    args = m.text.split(maxsplit=1)
    custom_text = args[1].strip() if len(args) > 1 else ""
    post_text = custom_text if custom_text else REQUEST_DEFAULT_TEXT
    if not BOT_USERNAME:
        await m.answer("❌ Имя бота ещё не определено, попробуй через пару секунд")
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE request_sessions SET status='closed' WHERE status='active'"
        )
        session_id = await conn.fetchval(
            "INSERT INTO request_sessions "
            "(channel_chat_id, channel_message_id, title) "
            "VALUES (NULL, NULL, $1) RETURNING id",
            (custom_text[:100] if custom_text else "Заявки на трек")
        )
    deep_link = f"https://t.me/{BOT_USERNAME}?start=req_{session_id}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Подать заявку", url=deep_link)]
    ])
    try:
        sent = await bot.send_message(
            channel_id, post_text, parse_mode="HTML", reply_markup=kb
        )
    except Exception as e:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM request_sessions WHERE id=$1", session_id
            )
        await m.answer(f"❌ Не удалось отправить пост в канал: {html.escape(str(e))}")
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE request_sessions SET channel_chat_id=$1, channel_message_id=$2 "
            "WHERE id=$3",
            sent.chat.id, sent.message_id, session_id
        )
    await m.answer(
        f"✅ Пост опубликован (сессия #{session_id}).\n"
        f"Заявки идут двумя путями:\n"
        f"• кнопка «Подать заявку» под постом → личка с ботом;\n"
        f"• комментарии под постом (если у канала есть обсуждалка).\n\n"
        f"Собрать список — /requests",
        parse_mode="HTML"
    )

@dp.message(F.is_automatic_forward, F.forward_from_chat)
async def request_thread_anchor(m: types.Message):
    """Авто-форвард поста из канала в обсуждалку. Привязываем
    активную сессию к этому треду, чтобы потом ловить комментарии."""
    if m.forward_from_chat.type != "channel":
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE request_sessions SET discussion_chat_id=$1, "
            "discussion_thread_id=$2 "
            "WHERE channel_chat_id=$3 AND channel_message_id=$4 "
            "AND status='active'",
            m.chat.id, m.message_id,
            m.forward_from_chat.id, m.forward_from_message_id
        )

@dp.message(F.chat.type.in_({"group", "supergroup"}), F.message_thread_id)
async def request_comment_handler(m: types.Message):
    """Комментарии под постом-приглашением (в связанной обсуждалке)."""
    if m.from_user and m.from_user.is_bot:
        return
    text = (m.text or m.caption or "").strip()
    if not text:
        return
    async with db_pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT id FROM request_sessions "
            "WHERE discussion_chat_id=$1 AND discussion_thread_id=$2 "
            "AND status='active'",
            m.chat.id, m.message_thread_id
        )
        if not session:
            return
        uid = m.from_user.id if m.from_user else None
        err_code, _ = await _request_limit_status(conn, session['id'], uid)
        if err_code:
            return
        await conn.execute(
            "INSERT INTO track_requests "
            "(session_id, user_id, username, full_name, text, discussion_message_id) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            session['id'],
            uid,
            m.from_user.username if m.from_user else None,
            m.from_user.full_name if m.from_user else None,
            text[:500],
            m.message_id
        )

def _format_user_label(username: str, full_name: str, user_id: int) -> str:
    if username:
        return f"@{username}"
    if full_name:
        return full_name
    return f"id{user_id}" if user_id else "anon"

@dp.message(Command("requests"))
async def list_requests_cmd(m: types.Message):
    """Показать список заявок последней активной сессии."""
    if m.from_user.id != ADMIN_ID:
        return
    async with db_pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT id, status, discussion_thread_id, created_at "
            "FROM request_sessions ORDER BY created_at DESC LIMIT 1"
        )
        if not session:
            await m.answer(
                "❌ Сессий заявок нет. Запусти: <code>/askreq</code>",
                parse_mode="HTML"
            )
            return
        rows = await conn.fetch(
            "SELECT id, user_id, username, full_name, text, created_at "
            "FROM track_requests WHERE session_id=$1 "
            "ORDER BY created_at ASC",
            session['id']
        )
    status_label = "активна" if session['status'] == 'active' else "закрыта"
    thread_warn = ""
    if session['status'] == 'active' and not session['discussion_thread_id']:
        thread_warn = (
            "\n⚠️ Бот пока не получил авто-форвард поста в обсуждалку. "
            "Убедись, что бот добавлен в обсуждалку канала."
        )
    if not rows:
        await m.answer(
            f"📭 Сессия #{session['id']} ({status_label}) — заявок пока нет.{thread_warn}",
            parse_mode="HTML"
        )
        return
    lines = [
        f"📥 <b>Заявки сессии #{session['id']}</b> ({status_label}, всего {len(rows)}):"
    ]
    for r in rows:
        who = _format_user_label(r['username'], r['full_name'], r['user_id'])
        lines.append(
            f"{r['id']}. <b>{html.escape(who)}</b>: {html.escape(r['text'])}"
        )
    if thread_warn:
        lines.append(thread_warn)
    text = "\n".join(lines)
    for chunk_start in range(0, len(text), 3500):
        await m.answer(text[chunk_start:chunk_start + 3500], parse_mode="HTML")

@dp.message(Command("closereq"))
async def close_requests_cmd(m: types.Message):
    """Закрыть текущую активную сессию заявок."""
    if m.from_user.id != ADMIN_ID:
        return
    async with db_pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT id FROM request_sessions WHERE status='active' "
            "ORDER BY created_at DESC LIMIT 1"
        )
        if not session:
            await m.answer("❌ Активных сессий заявок нет")
            return
        await conn.execute(
            "UPDATE request_sessions SET status='closed' WHERE id=$1",
            session['id']
        )
    await m.answer(f"✅ Сессия #{session['id']} закрыта. Новые комментарии не учитываются.")

@dp.message(Command("clearreq"))
async def clear_requests_cmd(m: types.Message):
    """Удалить все заявки последней сессии."""
    if m.from_user.id != ADMIN_ID:
        return
    async with db_pool.acquire() as conn:
        session = await conn.fetchrow(
            "SELECT id FROM request_sessions ORDER BY created_at DESC LIMIT 1"
        )
        if not session:
            await m.answer("❌ Сессий заявок нет")
            return
        deleted = await conn.fetchval(
            "WITH d AS (DELETE FROM track_requests WHERE session_id=$1 RETURNING 1) "
            "SELECT COUNT(*) FROM d",
            session['id']
        )
    await m.answer(f"🧹 Удалено заявок: {deleted} (сессия #{session['id']})")

# --- HEALTH CHECK ---
WEBHOOK_PATH = "/webhook"

async def health_check(request):
    return web.Response(text="ok")

async def main():
    from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

    dp.message.middleware(ThrottleMiddleware())
    dp.callback_query.middleware(ThrottleMiddleware())
    dp.message.middleware(RegisterUserMiddleware())
    dp.callback_query.middleware(RegisterUserMiddleware())
    dp.message.middleware(SubscriptionMiddleware())
    dp.callback_query.middleware(SubscriptionMiddleware())

    webhook_url = os.environ.get("WEBHOOK_URL", "")
    if not webhook_url:
        replit_domain = os.environ.get("REPLIT_DEV_DOMAIN", "")
        if replit_domain:
            webhook_url = f"https://{replit_domain}"
            logging.info(f"WEBHOOK_URL не задан, используется REPLIT_DEV_DOMAIN: {webhook_url}")
        else:
            raise ValueError("CRITICAL: WEBHOOK_URL не задан!")

    port = int(os.environ.get("PORT", 5000))

    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)

    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"🚀 Сервер запущен на порту {port}")

    await init_db_pool()
    await init_db()

    global BOT_USERNAME
    try:
        me = await bot.get_me()
        BOT_USERNAME = me.username or ""
        logging.info(f"✅ Bot username: @{BOT_USERNAME}")
    except Exception as e:
        logging.error(f"❌ Не удалось получить username бота: {e}")

    full_url = webhook_url.rstrip("/") + WEBHOOK_PATH
    try:
        info = await bot.get_webhook_info()
        current_url = (info.url or "").rstrip("/")
    except Exception as e:
        logging.warning(f"Не удалось получить webhook info: {e}")
        current_url = ""
    if current_url != full_url:
        await bot.set_webhook(url=full_url, drop_pending_updates=False)
        logging.info(f"✅ Webhook обновлён: {full_url}")
    else:
        logging.info(f"✅ Webhook уже актуален: {full_url} — апдейты не сбрасываем")

    asyncio.create_task(vote_auto_close_loop())
    logging.info("✅ Авто-закрытие голосований запущено")

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logging.info("Остановка бота... webhook оставляем зарегистрированным, "
                     "чтобы Telegram копил апдейты до следующего запуска")
        await runner.cleanup()
        if db_pool:
            await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())