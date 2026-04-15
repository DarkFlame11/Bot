import os
import asyncio
import logging
import html
import random
import asyncpg
from aiohttp import web

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.webhook.aiohttp_server import SimpleRequestHandler


# =========================
# CONFIG
# =========================
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PORT = int(os.getenv("PORT", 8000))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN missing")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing")
if not WEBHOOK_URL:
    raise RuntimeError("WEBHOOK_URL missing")


# =========================
# BOT INIT
# =========================
session = AiohttpSession(timeout=60)
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher(storage=MemoryStorage())


# =========================
# DB LAYER (PRO POOL MANAGER)
# =========================
class Database:
    def __init__(self):
        self.pool: asyncpg.Pool | None = None

    async def connect(self):
        retries = 5
        for i in range(retries):
            try:
                self.pool = await asyncpg.create_pool(
                    DATABASE_URL,
                    ssl="require",
                    min_size=1,
                    max_size=5,
                    command_timeout=10
                )
                logging.info("DB pool ready")
                return
            except Exception as e:
                logging.error(f"DB init fail {i+1}/{retries}: {e}")
                await asyncio.sleep(2)

        raise RuntimeError("DB connection failed")

    async def fetch(self, *args, **kwargs):
        async with self.pool.acquire() as conn:
            return await conn.fetch(*args, **kwargs)

    async def fetchrow(self, *args, **kwargs):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(*args, **kwargs)

    async def fetchval(self, *args, **kwargs):
        async with self.pool.acquire() as conn:
            return await conn.fetchval(*args, **kwargs)

    async def execute(self, *args, **kwargs):
        async with self.pool.acquire() as conn:
            return await conn.execute(*args, **kwargs)


db = Database()


# =========================
# INIT DB SCHEMA
# =========================
async def init_db():
    await db.execute("""
        CREATE TABLE IF NOT EXISTS tracks (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            artist TEXT,
            file_id TEXT UNIQUE NOT NULL,
            plays INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    await db.execute("""
        CREATE TABLE IF NOT EXISTS favorites (
            user_id BIGINT,
            track_id INT,
            PRIMARY KEY(user_id, track_id)
        )
    """)

    await db.execute("""
        CREATE TABLE IF NOT EXISTS playlists (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            name TEXT
        )
    """)

    await db.execute("""
        CREATE TABLE IF NOT EXISTS playlist_tracks (
            playlist_id INT,
            track_id INT,
            PRIMARY KEY(playlist_id, track_id)
        )
    """)

    logging.info("DB schema ready")


# =========================
# FSM
# =========================
class PlaylistForm(StatesGroup):
    waiting_name = State()


# =========================
# UI
# =========================
menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🔍 Поиск"), KeyboardButton(text="🎲 Случайный")],
        [KeyboardButton(text="🔥 Топ"), KeyboardButton(text="❤️ Избранное")],
        [KeyboardButton(text="📋 Плейлисты")]
    ],
    resize_keyboard=True
)


def track_kb(tid, liked=False):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="💔 Убрать" if liked else "❤️ В избранное",
            callback_data=f"{'unfav' if liked else 'fav'}_{tid}"
        )],
        [InlineKeyboardButton(text="➕ Плейлист", callback_data=f"topl_{tid}")]
    ])


def num_buttons(ids):
    nums = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=nums[i], callback_data=f"track_{tid}") for i, tid in enumerate(ids)]
    ])


# =========================
# SEARCH (FIXED SAFE VERSION)
# =========================
def variants(q: str):
    q = q.lower().strip()
    if not q:
        return []
    return [q]


async def search_tracks(q):
    v = variants(q)
    if not v:
        return []

    query = "SELECT id, title, artist FROM tracks WHERE " + \
            " OR ".join(["title ILIKE $1 OR artist ILIKE $1"])

    return await db.fetch(query, f"%{q}%")


# =========================
# HANDLERS
# =========================
@dp.message(Command("start"))
async def start(m: types.Message, state: FSMContext):
    await state.clear()
    await m.answer("🎧 Online", reply_markup=menu)


@dp.message(F.text == "🎲 Случайный")
async def rnd(m: types.Message):
    count = await db.fetchval("SELECT COUNT(*) FROM tracks")
    if not count:
        return await m.answer("Empty DB")

    offset = random.randint(0, count - 1)
    row = await db.fetchrow("SELECT id, file_id FROM tracks LIMIT 1 OFFSET $1", offset)

    await db.execute("UPDATE tracks SET plays = plays + 1 WHERE id=$1", row["id"])

    await m.answer_audio(row["file_id"], reply_markup=track_kb(row["id"]))


@dp.message(F.text == "🔍 Поиск")
async def search_ui(m: types.Message):
    await m.answer("Send query:")


@dp.message(F.text & ~F.text.startswith("/"))
async def search(m: types.Message, state: FSMContext):
    res = await search_tracks(m.text)

    if not res:
        return await m.answer("Nothing found")

    text = "\n".join(
        f"{i+1}. {html.escape(r['title'])}"
        for i, r in enumerate(res)
    )

    await m.answer(text, reply_markup=num_buttons([r["id"] for r in res]))


@dp.callback_query(F.data.startswith("track_"))
async def open_track(c: types.CallbackQuery):
    tid = int(c.data.split("_")[1])

    row = await db.fetchrow("SELECT file_id FROM tracks WHERE id=$1", tid)
    if not row:
        return await c.answer("Not found", show_alert=True)

    await db.execute("UPDATE tracks SET plays=plays+1 WHERE id=$1", tid)

    await c.message.answer_audio(row["file_id"], reply_markup=track_kb(tid))
    await c.answer()


@dp.callback_query(F.data.startswith("fav_"))
async def fav(c: types.CallbackQuery):
    tid = int(c.data.split("_")[1])
    await db.execute(
        "INSERT INTO favorites(user_id, track_id) VALUES($1,$2) ON CONFLICT DO NOTHING",
        c.from_user.id, tid
    )
    await c.answer("❤️")


@dp.callback_query(F.data.startswith("unfav_"))
async def unfav(c: types.CallbackQuery):
    tid = int(c.data.split("_")[1])
    await db.execute(
        "DELETE FROM favorites WHERE user_id=$1 AND track_id=$2",
        c.from_user.id, tid
    )
    await c.answer("💔")


# =========================
# WEBHOOK / APP
# =========================
async def health(request):
    return web.Response(text="OK")


async def on_startup():
    await db.connect()
    await init_db()

    await bot.delete_webhook(drop_pending_updates=True)

    await bot.set_webhook(WEBHOOK_URL + "/webhook")


async def main():
    await on_startup()

    app = web.Application()
    app.router.add_get("/", health)

    SimpleRequestHandler(dp, bot).register(app, path="/webhook")

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    logging.info("BOT RUNNING")

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())