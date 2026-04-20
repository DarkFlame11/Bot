import os
import math
import asyncio
import logging
import html
import random
import datetime
import asyncpg

from aiohttp import web

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
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
    kwargs = dict(min_size=1, max_size=5, command_timeout=10, statement_cache_size=0, timeout=10)
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
        logging.info("✅ База данных инициализирована")

# --- СОСТОЯНИЯ ---
class PlaylistForm(StatesGroup):
    waiting_name = State()

# --- КОНСТАНТЫ ---
PAGE_SIZE = 10
FAV_PAGE_SIZE = 5
PL_TRACK_PAGE_SIZE = 5
MAX_PLAYLISTS = 5

# --- КЛАВИАТУРЫ ---
menu = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text="🔍 Поиск"), KeyboardButton(text="🎲 Случайный")],
    [KeyboardButton(text="🔥 Топ"), KeyboardButton(text="❤️ Избранное")],
    [KeyboardButton(text="📋 Список Плейлистов")],
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

def _search_conditions(var):
    conditions, params, p = [], [], 1
    for v in var:
        conditions.append(f"title ILIKE ${p}")
        params.append(f"%{v}%")
        p += 1
        conditions.append(f"artist ILIKE ${p}")
        params.append(f"%{v}%")
        p += 1
    return conditions, params, p

async def count_search(query) -> int:
    pool = await get_db()
    async with pool.acquire() as conn:
        var = get_var(query)
        if not var: return 0
        try:
            conditions, params, _ = _search_conditions(var)
            sql = f"SELECT COUNT(*) FROM (SELECT DISTINCT id FROM tracks WHERE {' OR '.join(conditions)}) t"
            return await conn.fetchval(sql, *params)
        except Exception as e:
            logging.error(f"❌ Ошибка count_search: {e}")
    return 0

async def run_search(query, offset=0):
    pool = await get_db()
    async with pool.acquire() as conn:
        var = get_var(query)
        if not var: return []
        try:
            conditions, params, p = _search_conditions(var)
            sql = (
                f"SELECT DISTINCT id, title, artist FROM tracks "
                f"WHERE {' OR '.join(conditions)} "
                f"ORDER BY id "
                f"LIMIT ${p} OFFSET ${p+1}"
            )
            params.extend([PAGE_SIZE, offset])
            return await conn.fetch(sql, *params)
        except Exception as e:
            logging.error(f"❌ Ошибка поиска: {e}")
    return []

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

@dp.message(Command("start"))
async def start_cmd(m: types.Message, state: FSMContext):
    await state.clear()
    await m.answer("🎧 Бот запущен", reply_markup=menu)

@dp.channel_post(F.audio)
async def save_track(m: types.Message):
    t = m.audio.title or "Unknown"
    a = m.audio.performer or "Unknown"
    f = m.audio.file_id
    pool = await get_db()
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                "INSERT INTO tracks (title, artist, file_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                t, a, f
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

@dp.message(F.text == "🎲 Случайный")
async def rnd(m: types.Message):
    pool = await get_db()
    try:
        async with pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM tracks")
            if count == 0:
                await m.answer("❌ База пуста")
                return
            offset = random.randint(0, count - 1)
            r = await conn.fetchrow("SELECT id, file_id FROM tracks LIMIT 1 OFFSET $1", offset)
            await conn.execute("UPDATE tracks SET plays=plays+1 WHERE id=$1", r['id'])
        await m.answer_audio(r['file_id'], reply_markup=await track_keyboard(r['id'], m.from_user.id))
    except Exception as e:
        logging.error(f"Ошибка в rnd: {e}")

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
                InlineKeyboardButton(text=f"📋 {html.escape(p['name'])}", callback_data=f"opl_{p['id']}"),
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
            [InlineKeyboardButton(text=f"📋 {html.escape(p['name'])}", callback_data=f"apl_{p['id']}_{tid}")]
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
        await m.answer(
            f"📊 <b>Статистика</b>\n\n"
            f"Треков: {tc}\nПрослушиваний: {tp}\nИзбранных: {fc}\nПлейлистов: {pc}",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"Ошибка в stats: {e}")

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

@dp.message(F.text & ~F.text.startswith("/") & ~F.text.in_({"🔍 Поиск","🎲 Случайный","🔥 Топ","❤️ Избранное","📋 Список Плейлистов"}))
async def search(m: types.Message, state: FSMContext):
    cur = await state.get_state()
    if cur == PlaylistForm.waiting_name:
        return
    q = m.text.strip()
    if not q: return
    total = await count_search(q)
    if total == 0:
        await m.answer("❌ Ничего не найдено")
        return
    res = await run_search(q, offset=0)
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
        total = await count_search(q)
        if total == 0:
            await c.answer("❌ Ничего не найдено", show_alert=True)
            return
        res = await run_search(q, offset=offset)
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
                conditions, params, p = _search_conditions(var)
                sql = f"SELECT id, title, artist FROM tracks WHERE {' OR '.join(conditions)} ORDER BY id DESC LIMIT 30"
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

def current_period(vote_type: str) -> str:
    today = datetime.date.today()
    if vote_type == 'day':
        return today.strftime("%d.%m.%Y")
    iso = today.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"

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

def build_vote_text(vote_type: str, period: str, rows, closed=False) -> str:
    if vote_type == 'day':
        title = "🏆 Трек дня" if closed else "🗳 Голосование: Трек дня"
    else:
        title = "🏆 Трек недели" if closed else "🗳 Голосование: Трек недели"
    total = sum(r['vote_count'] for r in rows)
    medals = ["🥇", "🥈", "🥉"]
    lines = []
    for i, r in enumerate(rows):
        pct = round(r['vote_count'] / total * 100) if total > 0 else 0
        filled = pct // 10
        bar = "█" * filled + "░" * (10 - filled)
        prefix = medals[i] if (closed and i < 3) else f"{i + 1}."
        lines.append(
            f"{prefix} {html.escape(format_track(r['artist'], r['title']))}\n"
            f"    {bar} {r['vote_count']} гол. ({pct}%)"
        )
    footer = (
        f"\n\nВсего голосов: <b>{total}</b>"
        if closed else
        "\n\n👆 Нажми на трек чтобы проголосовать • 1 голос на человека"
    )
    return f"<b>{title}</b> — {period}\n\n" + "\n\n".join(lines) + footer

def build_vote_keyboard(session_id: int, rows) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(
            text=f"{i + 1}. {html.escape(format_track(r['artist'], r['title']))[:35]}",
            callback_data=f"vote_{session_id}_{r['track_id']}"
        )]
        for i, r in enumerate(rows)
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def _start_vote(m: types.Message, vote_type: str, limit: int):
    if m.from_user.id != ADMIN_ID: return
    channel_id = get_channel_id()
    if not channel_id:
        await m.answer("❌ CHANNEL_ID не задан в переменных окружения")
        return
    pool = await get_db()
    period = current_period(vote_type)
    async with pool.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT id FROM vote_sessions WHERE vote_type=$1 AND period=$2", vote_type, period
        )
        if existing:
            label = "сегодня" if vote_type == 'day' else "эту неделю"
            await m.answer(f"⚠️ Голосование за {label} уже запущено")
            return
        tracks = await conn.fetch(
            "SELECT id, title, artist FROM tracks ORDER BY plays DESC LIMIT $1", limit
        )
        if len(tracks) < 2:
            await m.answer("❌ Мало треков в базе (нужно минимум 2)")
            return
        session_id = await conn.fetchval(
            "INSERT INTO vote_sessions (vote_type, period) VALUES ($1, $2) RETURNING id",
            vote_type, period
        )
        for t in tracks:
            await conn.execute(
                "INSERT INTO vote_candidates (session_id, track_id) VALUES ($1, $2)",
                session_id, t['id']
            )
    rows = await get_vote_counts(session_id)
    text = build_vote_text(vote_type, period, rows)
    kb = build_vote_keyboard(session_id, rows)
    try:
        msg = await bot.send_message(channel_id, text, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        await m.answer(f"❌ Не удалось отправить в канал: {e}")
        return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE vote_sessions SET channel_message_id=$1, channel_chat_id=$2 WHERE id=$3",
            msg.message_id, channel_id, session_id
        )
    label = "Трек дня" if vote_type == 'day' else "Трек недели"
    await m.answer(f"✅ Голосование «{label}» запущено! {len(tracks)} треков в списке.")

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
        await conn.execute("UPDATE vote_sessions SET status='closed' WHERE id=$1", session['id'])
    rows = await get_vote_counts(session['id'])
    text = build_vote_text(vote_type, session['period'], rows, closed=True)
    if session['channel_message_id'] and session['channel_chat_id']:
        try:
            await bot.edit_message_text(
                text,
                chat_id=session['channel_chat_id'],
                message_id=session['channel_message_id'],
                parse_mode="HTML"
            )
        except Exception as e:
            logging.warning(f"Не удалось обновить сообщение в канале: {e}")
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

@dp.message(Command("startday"))
async def start_day(m: types.Message):
    await _start_vote(m, 'day', limit=5)

@dp.message(Command("startweek"))
async def start_week(m: types.Message):
    await _start_vote(m, 'week', limit=10)

@dp.message(Command("closeday"))
async def close_day(m: types.Message):
    await _close_vote(m, 'day')

@dp.message(Command("closeweek"))
async def close_week(m: types.Message):
    await _close_vote(m, 'week')

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
        label = "Трек дня" if s['vote_type'] == 'day' else "Трек недели"
        lines = [f"{i+1}. {html.escape(format_track(r['artist'], r['title']))} — {r['vote_count']} гол."
                 for i, r in enumerate(rows)]
        await m.answer(
            f"📊 <b>{label}</b> ({s['period']})\nВсего голосов: {total}\n\n" + "\n".join(lines),
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
        text = build_vote_text(session['vote_type'], session['period'], rows)
        kb = build_vote_keyboard(session_id, rows)
        try:
            await c.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
        except: pass
        voted_row = next((r for r in rows if r['track_id'] == track_id), None)
        name = html.escape(format_track(voted_row['artist'], voted_row['title'])) if voted_row else "трек"
        await c.answer(f"✅ Голос за «{name}» засчитан!", show_alert=True)
    except Exception as e:
        logging.error(f"Ошибка в handle_vote: {e}")
        await c.answer("❌ Ошибка", show_alert=True)

# --- HEALTH CHECK ---
WEBHOOK_PATH = "/webhook"

async def health_check(request):
    return web.Response(text="ok")

async def main():
    from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

    webhook_url = os.environ.get("WEBHOOK_URL", "")
    if not webhook_url:
        raise ValueError("CRITICAL: WEBHOOK_URL не задан!")

    port = int(os.environ.get("PORT", 8080))

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

    await bot.delete_webhook(drop_pending_updates=True)
    full_url = webhook_url.rstrip("/") + WEBHOOK_PATH
    await bot.set_webhook(url=full_url)
    logging.info(f"✅ Webhook установлен: {full_url}")

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logging.info("Остановка бота...")
        await bot.delete_webhook()
        await runner.cleanup()
        if db_pool:
            await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())