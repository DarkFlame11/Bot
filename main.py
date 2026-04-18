import os
import sys
import asyncio
import logging
import html
import random
import asyncpg
import socket
import urllib.parse
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
    raise ValueError("CRITICAL: Переменная BOT_TOKEN не задана в Railway!")

ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
DATABASE_URL = os.environ.get("DATABASE_URL", "")

if not DATABASE_URL:
    raise ValueError("CRITICAL: Переменная DATABASE_URL не задана!")

# Railway даёт postgres://, asyncpg требует postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Инициализация бота
session = AiohttpSession(timeout=60)
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher(storage=MemoryStorage())

# Глобальный пул подключений
db_pool = None

# --- БАЗА ДАННЫХ ---
async def init_db_pool():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(
            DATABASE_URL,
            ssl="require",          # Railway PostgreSQL требует SSL
            min_size=1,
            max_size=5,
            command_timeout=10,
            statement_cache_size=0,
            timeout=10
        )
        logging.info("✅ DB pool создан")

        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")

        logging.info("✅ DB проверена и работает")

    except Exception as e:
        logging.error(f"❌ Ошибка БД: {e}")
        raise  # При Railway важно упасть громко, чтобы видеть в логах

async def get_db():
    return db_pool

async def init_db():
    """Создание таблиц"""
    pool = await get_db()
    async with pool.acquire() as conn:
        try:
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

            logging.info("✅ База данных инициализирована")
        except Exception as e:
            logging.error(f"❌ Ошибка инициализации БД: {e}")
            raise

# --- СОСТОЯНИЯ ---
class PlaylistForm(StatesGroup):
    waiting_name = State()

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

def num_buttons(ids):
    d = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    b = [InlineKeyboardButton(text=d[i], callback_data=f"track_{tid}") for i, tid in enumerate(ids)]
    return InlineKeyboardMarkup(inline_keyboard=[b[i:i+5] for i in range(0, len(b), 5)])

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
        [InlineKeyboardButton(text="➕ В плейлист", callback_data=f"topl_{tid}")]
    ])

# --- ПОИСК И ТРАНСЛИТЕРАЦИЯ ---
CYR_LAT = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'yo','ж':'zh','з':'z','и':'i',
    'й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r','с':'s','т':'t',
    'у':'u','ф':'f','х':'kh','ц':'ts','ч':'ch','ш':'sh','щ':'shch','ъ':'','ы':'y','ь':'',
    'э':'e','ю':'yu','я':'ya'
}

LAT_CYR = {
    "shch":"щ","sch":"щ","sh":"ш","ch":"ч","zh":"ж","ts":"ц","yu":"ю","ya":"я","kh":"х",
    "yo":"ё","a":"а","b":"б","v":"в","g":"г","d":"д","e":"е","z":"з","i":"и",
    "y":"и","k":"к","l":"л","m":"м","n":"н","o":"о","p":"п","r":"р","s":"с",
    "t":"т","u":"у","f":"ф","h":"х","c":"к","w":"в","x":"х","j":"й"
}

def translit_to_latin(text):
    result = text.lower()
    for cyr in sorted(CYR_LAT.keys(), key=len, reverse=True):
        result = result.replace(cyr, CYR_LAT[cyr])
    return result

def translit_to_cyrillic(text):
    result = text.lower()
    for lat in sorted(LAT_CYR.keys(), key=len, reverse=True):
        result = result.replace(lat, LAT_CYR[lat])
    return result

def get_var(q):
    q = q.lower().strip()
    if not q: return []
    q = q.replace('\u200b','').replace('\u200c','').replace('\u200d','').replace(' ','')
    if not q: return []

    v = [q]
    has_cyrillic = any(0x0400 <= ord(c) <= 0x04FF for c in q)
    has_latin = any(c.isascii() and c.isalpha() for c in q)

    if has_cyrillic:
        lat = translit_to_latin(q)
        if lat and lat != q: v.append(lat)

    if has_latin:
        cyr = translit_to_cyrillic(q)
        if cyr and cyr != q: v.append(cyr)
    return v

async def run_search(query, limit=10):
    pool = await get_db()
    async with pool.acquire() as conn:
        var = get_var(query)
        if not var: return []

        try:
            all_conditions = []
            all_params = []
            param_count = 1

            for v in var:
                all_conditions.append(f"title ILIKE ${param_count}")
                all_params.append(f"%{v}%")
                param_count += 1

                all_conditions.append(f"artist ILIKE ${param_count}")
                all_params.append(f"%{v}%")
                param_count += 1

            sql = f"SELECT DISTINCT id, title, artist FROM tracks WHERE {' OR '.join(all_conditions)} LIMIT ${param_count}"
            all_params.append(limit)

            res = await conn.fetch(sql, *all_params)
            if res: return res
        except Exception as e:
            logging.error(f"❌ Ошибка поиска: {e}")
    return []

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
            result = await conn.fetchval("SELECT 1 FROM tracks WHERE file_id=$1", f)
            if result: await m.answer("⚠️ Уже есть"); return
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
            if count == 0: await m.answer("❌ База пуста"); return
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
    pool = await get_db()
    async with pool.acquire() as conn:
        try:
            res = await conn.fetch(
                "SELECT tracks.id, tracks.file_id FROM tracks JOIN favorites ON tracks.id=favorites.track_id WHERE favorites.user_id=$1",
                m.from_user.id
            )
            if not res: await m.answer("Пусто ❤️"); return
            for t in res:
                await conn.execute("UPDATE tracks SET plays=plays+1 WHERE id=$1", t['id'])
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="💔 Убрать", callback_data=f"unfav_{t['id']}")],
                    [InlineKeyboardButton(text="➕ В плейлист", callback_data=f"topl_{t['id']}")]
                ])
                try: await m.answer_audio(t['file_id'], reply_markup=kb)
                except: await m.answer("⚠️ Один из треков недоступен")
        except Exception as e: logging.error(f"Ошибка в sf: {e}")

@dp.message(F.text == "🔥 Топ")
async def top(m: types.Message):
    pool = await get_db()
    try:
        async with pool.acquire() as conn:
            res = await conn.fetch("SELECT artist, title, plays FROM tracks ORDER BY plays DESC LIMIT 10")
        if not res: await m.answer("❌ Пусто"); return
        md = ["🥇","🥈","🥉"]
        lines = [f"{md[i-1] if i<=3 else f'{i}.'} {html.escape(format_track(t['artist'], t['title']))} — {t['plays']} 🎧" for i, t in enumerate(res, 1)]
        await m.answer("🔥 <b>Топ:</b>\n\n" + "\n".join(lines), parse_mode="HTML")
    except Exception as e: logging.error(f"Ошибка в top: {e}")

@dp.message(F.text == "📋 Список Плейлистов")
async def spl(m: types.Message, state: FSMContext):
    await state.clear()
    pool = await get_db()
    try:
        async with pool.acquire() as conn:
            res = await conn.fetch("SELECT id, name FROM playlists WHERE user_id=$1", m.from_user.id)
        rows = [[InlineKeyboardButton(text=f"📋 {html.escape(p['name'])}", callback_data=f"opl_{p['id']}"), InlineKeyboardButton(text="🗑", callback_data=f"delpl_{p['id']}")] for p in res]
        rows.append([InlineKeyboardButton(text="➕ Создать", callback_data="plnew")])
        await m.answer("📋 Плейлисты:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    except Exception as e: logging.error(f"Ошибка в spl: {e}")

@dp.callback_query(F.data == "plnew")
async def cpn(c: types.CallbackQuery, state: FSMContext):
    await state.set_state(PlaylistForm.waiting_name)
    await c.message.answer("✏️ Название:")
    await c.answer()

@dp.message(PlaylistForm.waiting_name)
async def rpn(m: types.Message, state: FSMContext):
    n = m.text.strip()
    if not n or len(n) > 50: await m.answer("❌ От 1 до 50 симв."); return
    pool = await get_db()
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO playlists (user_id, name) VALUES ($1, $2)", m.from_user.id, n)
        await state.clear()
        await m.answer(f"✅ «{html.escape(n)}» создан!")
    except Exception as e: logging.error(f"Ошибка в rpn: {e}")

@dp.callback_query(F.data.startswith("delpl_"))
async def dpl(c: types.CallbackQuery):
    try:
        pid = int(c.data.split("_")[1])
        pool = await get_db()
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT 1 FROM playlists WHERE id=$1 AND user_id=$2", pid, c.from_user.id)
            if not result: await c.answer("❌", show_alert=True); return
            await conn.execute("DELETE FROM playlists WHERE id=$1", pid)
        await c.answer("🗑 Удален", show_alert=True)
        try: await c.message.delete()
        except: pass
    except Exception as e: logging.error(f"Ошибка в dpl: {e}")

@dp.callback_query(F.data.startswith("opl_"))
async def opl(c: types.CallbackQuery):
    try:
        pid = int(c.data.split("_")[1])
        pool = await get_db()
        async with pool.acquire() as conn:
            pl = await conn.fetchrow("SELECT name FROM playlists WHERE id=$1 AND user_id=$2", pid, c.from_user.id)
            if not pl: await c.answer("❌", show_alert=True); return
            tr = await conn.fetch("SELECT tracks.id, tracks.file_id FROM tracks JOIN playlist_tracks ON tracks.id=playlist_tracks.track_id WHERE playlist_tracks.playlist_id=$1", pid)
            if not tr: await c.message.answer(f"📋 «{html.escape(pl['name'])}» пуст."); await c.answer(); return
            await c.message.answer(f"📋 «{html.escape(pl['name'])}»:")
            for t in tr:
                await conn.execute("UPDATE tracks SET plays=plays+1 WHERE id=$1", t['id'])
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑 Убрать", callback_data=f"rmpl_{pid}_{t['id']}")]])
                try: await c.message.answer_audio(t['file_id'], reply_markup=kb)
                except: await c.message.answer("⚠️ Трек недоступен")
        await c.answer()
    except Exception as e: logging.error(f"Ошибка в opl: {e}")

@dp.callback_query(F.data.startswith("topl_"))
async def cpl(c: types.CallbackQuery):
    try:
        tid = int(c.data.split("_")[1])
        pool = await get_db()
        async with pool.acquire() as conn:
            pls = await conn.fetch("SELECT id, name FROM playlists WHERE user_id=$1", c.from_user.id)
        if not pls: await c.answer("Сначала создай плейлист", show_alert=True); return
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"📋 {html.escape(p['name'])}", callback_data=f"apl_{p['id']}_{tid}")] for p in pls])
        await c.message.answer("Выбери:", reply_markup=kb)
        await c.answer()
    except Exception as e: logging.error(f"Ошибка в cpl: {e}")

@dp.callback_query(F.data.startswith("apl_"))
async def apl(c: types.CallbackQuery):
    try:
        p = c.data.split("_")
        pid, tid = int(p[1]), int(p[2])
        pool = await get_db()
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO playlist_tracks (playlist_id, track_id) VALUES ($1, $2) ON CONFLICT DO NOTHING", pid, tid)
        await c.answer("✅ Добавлено", show_alert=True)
        try: await c.message.delete()
        except: pass
    except Exception as e: logging.error(f"Ошибка в apl: {e}")

@dp.callback_query(F.data.startswith("rmpl_"))
async def rmpl(c: types.CallbackQuery):
    try:
        p = c.data.split("_")
        pid, tid = int(p[1]), int(p[2])
        pool = await get_db()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM playlist_tracks WHERE playlist_id=$1 AND track_id=$2", pid, tid)
        await c.answer("🗑 Убрано", show_alert=True)
        try: await c.message.delete()
        except: pass
    except Exception as e: logging.error(f"Ошибка в rmpl: {e}")

@dp.message(Command("stats"))
async def stats(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    pool = await get_db()
    try:
        async with pool.acquire() as conn:
            tt = await conn.fetchval("SELECT COUNT(*) FROM tracks")
            tp = await conn.fetchval("SELECT COALESCE(SUM(plays),0) FROM tracks")
            tu = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM favorites")
            tpl = await conn.fetchval("SELECT COUNT(*) FROM playlists")
        await m.answer(f"📊 <b>Статистика:</b>\n🎵 Треков: {tt}\n🎧 Прослушиваний: {tp}\n👥 С избранных юзеров: {tu}\n📋 Плейлистов: {tpl}", parse_mode="HTML")
    except Exception as e: logging.error(f"Ошибка в stats: {e}")

@dp.message(Command("manage"))
async def mg(m: types.Message):
    if m.from_user.id != ADMIN_ID: return
    a = m.text.split(maxsplit=1)
    pool = await get_db()
    try:
        async with pool.acquire() as conn:
            if len(a) >= 2:
                q = a[1].strip().lower()
                res = await conn.fetch("SELECT id, title, artist FROM tracks WHERE title ILIKE $1 OR artist ILIKE $1 ORDER BY id DESC", f"%{q}%")
                h = f"🔍 По «{html.escape(a[1].strip())}»:"
            else:
                res = await conn.fetch("SELECT id, title, artist FROM tracks ORDER BY id DESC LIMIT 30")
                h = "🗑 Последние 30:"
        if not res: await m.answer("📂 Пусто."); return
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"🗑 {html.escape(format_track(t['artist'], t['title']))}", callback_data=f"del_{t['id']}")] for t in res])
        await m.answer(h, reply_markup=kb)
    except Exception as e: logging.error(f"Ошибка в mg: {e}")

# ИСПРАВЛЕНО: был баг — переменная называлась r, а проверялась как t; и логика была от хэндлера удаления
@dp.callback_query(F.data.startswith("track_"))
async def st(c: types.CallbackQuery):
    try:
        tid = int(c.data.split("_")[1])
        pool = await get_db()
        async with pool.acquire() as conn:
            r = await conn.fetchrow("SELECT id, file_id FROM tracks WHERE id=$1", tid)
            if not r: await c.answer("❌ Трек не найден", show_alert=True); return
            await conn.execute("UPDATE tracks SET plays=plays+1 WHERE id=$1", tid)
        await c.message.answer_audio(r['file_id'], reply_markup=await track_keyboard(tid, c.from_user.id))
        await c.answer()
    except Exception as e: logging.error(f"Ошибка в st: {e}")

# Хэндлер удаления трека (для /manage)
@dp.callback_query(F.data.startswith("del_"))
async def dt(c: types.CallbackQuery):
    if c.from_user.id != ADMIN_ID: await c.answer("❌", show_alert=True); return
    try:
        tid = int(c.data.split("_")[1])
        pool = await get_db()
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT 1 FROM tracks WHERE id=$1", tid)
            if not result: await c.answer("❌ Не найден", show_alert=True); return
            await conn.execute("DELETE FROM tracks WHERE id=$1", tid)
        await c.answer("✅ Удалено", show_alert=True)
        try: await c.message.delete()
        except: pass
    except Exception as e: logging.error(f"Ошибка в dt: {e}")

# Избранное — добавить
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
        # Обновляем клавиатуру
        try:
            await c.message.edit_reply_markup(reply_markup=await track_keyboard(tid, c.from_user.id))
        except: pass
    except Exception as e: logging.error(f"Ошибка в fav: {e}")

# Избранное — убрать
@dp.callback_query(F.data.startswith("unfav_"))
async def unfav(c: types.CallbackQuery):
    try:
        tid = int(c.data.split("_")[1])
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
    except Exception as e: logging.error(f"Ошибка в unfav: {e}")

@dp.message(F.text & ~F.text.startswith("/"))
async def sm(m: types.Message, state: FSMContext):
    if await state.get_state() == PlaylistForm.waiting_name.state: return
    q = m.text.strip()
    if not q: return
    res = await run_search(q)
    if not res:
        pool = await get_db()
        try:
            async with pool.acquire() as conn:
                tot = await conn.fetchval("SELECT COUNT(*) FROM tracks")
            await m.answer("❌ База пуста." if tot == 0 else f"❌ Ничего по «{html.escape(q)}»")
        except Exception as e: logging.error(f"Ошибка в sm: {e}")
        return
    lines = [f"{i}. {html.escape(format_track(t['artist'], t['title']))}" for i, t in enumerate(res, 1)]
    await m.answer(f"🎧 <b>Найдено {len(res)}:</b>\n\n" + "\n".join(lines) + "\n\n👇 Нажми номер:", parse_mode="HTML", reply_markup=num_buttons([t['id'] for t in res]))

# --- HEALTH CHECK ---
async def health_check(request):
    return web.Response(text="Bot is alive")

# --- ЗАПУСК ДЛЯ RAILWAY ---
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

async def main():
    WEBHOOK_PATH = "/webhook"
    webhook_url = os.environ.get("WEBHOOK_URL")
    if not webhook_url:
        raise ValueError("WEBHOOK_URL не задана!")

    port = int(os.environ.get("PORT", 8080))  # Railway пробрасывает PORT автоматически

    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)

    # Регистрируем aiogram webhook handler
    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    logging.info(f"🚀 Сервер запущен на порту {port}")

    # БД подключаем ПОСЛЕ того как сервер уже слушает порт
    await init_db_pool()
    await init_db()

    # Устанавливаем webhook
    await bot.delete_webhook(drop_pending_updates=True)
    full_webhook_url = webhook_url.rstrip("/") + WEBHOOK_PATH
    await bot.set_webhook(url=full_webhook_url)
    logging.info(f"✅ Webhook установлен: {full_webhook_url}")

    # Держим процесс живым
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