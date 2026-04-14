import os
import sys
import asyncio
import logging
import aiosqlite
from contextlib import asynccontextmanager
from aiohttp import web
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

logging.basicConfig(level=logging.INFO)

# Koyeb автоматически подставит эти значения
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
DB_PATH = "database.db"

@asynccontextmanager
async def get_db():
    db = await aiosqlite.connect(DB_PATH)
    try:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        await db.execute("PRAGMA case_sensitive_like = OFF")
        yield db
    finally:
        await db.close()

async def init_db():
    async with get_db() as db:
        await db.execute("CREATE TABLE IF NOT EXISTS tracks (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, artist TEXT, file_id TEXT UNIQUE, plays INTEGER DEFAULT 0)")
        await db.execute("CREATE TABLE IF NOT EXISTS favorites (user_id INTEGER, track_id INTEGER, PRIMARY KEY(user_id, track_id))")
        await db.execute("CREATE TABLE IF NOT EXISTS playlists (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, name TEXT)")
        await db.execute("CREATE TABLE IF NOT EXISTS playlist_tracks (playlist_id INTEGER, track_id INTEGER, PRIMARY KEY(playlist_id, track_id))")
        await db.commit()

class PlaylistForm(StatesGroup):
    waiting_name = State()

menu = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text="🔍 Поиск"), KeyboardButton(text="🎲 Случайный")],
    [KeyboardButton(text="🔥 Топ"), KeyboardButton(text="❤️ Избранное")],
    [KeyboardButton(text="📋 Плейлисты")], ], resize_keyboard=True)

def clean_artist(a): return "" if a and a.startswith("@") else (a or "")
def format_track(a, t): a = clean_artist(a); return f"{a} — {t}" if a else t

def num_buttons(ids):
    d = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    b = [InlineKeyboardButton(text=d[i], callback_data=f"track_{tid}") for i, tid in enumerate(ids)]
    return InlineKeyboardMarkup(inline_keyboard=[b[i:i+5] for i in range(0, len(b), 5)])

async def track_keyboard(tid, uid):
    async with get_db() as db:
        cur = await db.execute("SELECT 1 FROM favorites WHERE user_id=? AND track_id=?", (uid, tid))
        inf = (await cur.fetchone()) is not None
    btn = InlineKeyboardButton(text="💔 Убрать" if inf else "❤️ В избранное", callback_data=f"unfav_{tid}" if inf else f"fav_{tid}")
    return InlineKeyboardMarkup(inline_keyboard=[[btn], [InlineKeyboardButton(text="➕ В плейлист", callback_data=f"topl_{tid}")]])

CYR_LAT = {'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'yo','ж':'zh','з':'z','и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','х':'kh','ц':'ts','ч':'ch','ш':'sh','щ':'shch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya','дж':'j'}
LAT_CYR = {"shch":"щ","sh":"ш","ch":"ч","zh":"ж","ts":"ц","yu":"ю","ya":"я","kh":"х","yo":"ё","a":"а","b":"б","v":"в","g":"г","d":"д","e":"е","z":"з","i":"и","y":"и","k":"к","l":"л","m":"м","n":"н","o":"о","p":"п","r":"р","s":"с","t":"т","u":"у","f":"ф","w":"в","x":"кс","j":"дж","h":"х"}

def translit(t, m):
    r = t.lower()
    for k, v in sorted(m.items(), key=lambda x: -len(x[0])): r = r.replace(k, v)
    return r

def get_var(q):
    q = "".join(str(q).lower().split())
    if not q: return []
    v = {q}
    lat = translit(q, CYR_LAT)
    if lat: v.add(lat)
    cyr = translit(q, LAT_CYR)
    if cyr: v.add(cyr)
    return list(v)

async def run_search(query, limit=10):
    async with get_db() as db:
        var = get_var(query)
        if not var: return []
        p = []
        for v in var: p.extend([f"%{v}%", f"%{v}%"])
        sql = "SELECT id, title, artist FROM tracks WHERE " + " OR ".join(["title LIKE ? OR artist LIKE ?"]*len(var)) + " LIMIT ?"
        p.append(limit)
        try:
            cur = await db.execute(sql, p); res = await cur.fetchall()
            if res: return res
        except Exception as e: print(f"Err1: {e}")
        words = set()
        for v in var:
            for w in v.split():
                if len(w)>=2: words.add(w)
        if not words: return []
        wc, wp = [], []
        for w in words:
            wc.append("title LIKE ? OR artist LIKE ?")
            wp.extend([f"%{w}%", f"%{w}%"])
        ws = "SELECT id, title, artist FROM tracks WHERE " + " OR ".join(wc) + " LIMIT ?"
        wp.append(limit)
        try:
            cur = await db.execute(ws, wp); rows = await cur.fetchall()
            seen, comb = set(), []
            for r in rows:
                if r[0] not in seen: seen.add(r[0]); comb.append(r)
                if len(comb) >= limit: break
            return comb[:limit]
        except Exception as e: print(f"Err2: {e}")
    return []

@dp.message(Command("start"))
async def start(m, state: FSMContext):
    await state.clear(); await m.answer("🎧 Бот запущен", reply_markup=menu)

@dp.channel_post(F.audio)
async def save_track(m):
    t, a, f = m.audio.title or "Unknown", m.audio.performer or "Unknown", m.audio.file_id
    async with get_db() as db:
        await db.execute("INSERT OR IGNORE INTO tracks (title, artist, file_id) VALUES (?,?,?)", (t,a,f)); await db.commit()

@dp.message(F.audio)
async def imp_track(m):
    if m.from_user.id != ADMIN_ID: return
    t, a, f = m.audio.title or "Unknown", m.audio.performer or "Unknown", m.audio.file_id
    async with get_db() as db:
        cur = await db.execute("SELECT 1 FROM tracks WHERE file_id=?", (f,))
        if await cur.fetchone(): await m.answer("⚠️ Уже есть"); return
        await db.execute("INSERT INTO tracks (title, artist, file_id) VALUES (?,?,?)", (t,a,f)); await db.commit()
    await m.answer(f"✅ Сохранено: {a} — {t}")

@dp.message(F.text == "🎲 Случайный")
async def rnd(m):
    async with get_db() as db:
        cur = await db.execute("SELECT id, file_id FROM tracks ORDER BY RANDOM() LIMIT 1"); r = await cur.fetchone()
        if not r: await m.answer("❌ Пусто"); return
        await db.execute("UPDATE tracks SET plays=plays+1 WHERE id=?", (r[0],)); await db.commit()
    await m.answer_audio(r[1], reply_markup=await track_keyboard(r[0], m.from_user.id))

@dp.message(F.text == "🔍 Поиск")
async def sb(m, state: FSMContext):
    await state.clear(); await m.answer("🔍 Пиши запрос:")

@dp.message(F.text == "❤️ Избранное")
async def sf(m):
    async with get_db() as db:
        cur = await db.execute("SELECT tracks.id, tracks.file_id FROM tracks JOIN favorites ON tracks.id=favorites.track_id WHERE favorites.user_id=?", (m.from_user.id,)); res = await cur.fetchall()
        if not res: await m.answer("Пусто ❤️"); return
        for t in res:
            await db.execute("UPDATE tracks SET plays=plays+1 WHERE id=?", (t[0],))
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💔 Убрать", callback_data=f"unfav_{t[0]}")],[InlineKeyboardButton(text="➕ В плейлист", callback_data=f"topl_{t[0]}")]])
            await m.answer_audio(t[1], reply_markup=kb)
        await db.commit()

@dp.message(F.text == "🔥 Топ")
async def top(m):
    async with get_db() as db:
        cur = await db.execute("SELECT artist, title, plays FROM tracks ORDER BY plays DESC LIMIT 10"); res = await cur.fetchall()
    if not res: await m.answer("❌ Пусто"); return
    md = ["🥇","🥈","🥉"]
    lines = [f"{md[i-1] if i<=3 else f'{i}.'} {format_track(t[0], t[1])} — {t[2]} 🎧" for i, t in enumerate(res, 1)]
    await m.answer("🔥 <b>Топ:</b>\n\n" + "\n".join(lines), parse_mode="HTML")

@dp.message(F.text == "📋 Плейлисты")
async def spl(m, state: FSMContext):
    await state.clear()
    async with get_db() as db:
        cur = await db.execute("SELECT id, name FROM playlists WHERE user_id=?", (m.from_user.id,)); res = await cur.fetchall()
    rows = [[InlineKeyboardButton(text=f"📋 {p[1]}", callback_data=f"opl_{p[0]}"), InlineKeyboardButton(text="🗑", callback_data=f"delpl_{p[0]}")] for p in res]
    rows.append([InlineKeyboardButton(text="➕ Создать", callback_data="plnew")])
    await m.answer("📋 Плейлисты:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@dp.callback_query(F.data == "plnew")
async def cpn(c, state: FSMContext):
    await state.set_state(PlaylistForm.waiting_name); await c.message.answer("✏️ Название:"); await c.answer()

@dp.message(PlaylistForm.waiting_name)
async def rpn(m, state: FSMContext):
    n = m.text.strip()
    if not n or len(n)>50: await m.answer("❌ От 1 до 50 симв."); return
    async with get_db() as db:
        await db.execute("INSERT INTO playlists (user_id, name) VALUES (?,?)", (m.from_user.id, n)); await db.commit()
    await state.clear(); await m.answer(f"✅ «{n}» создан!")

@dp.callback_query(F.data.startswith("delpl_"))
async def dpl(c):
    pid = int(c.data.split("_")[1])
    async with get_db() as db:
        cur = await db.execute("SELECT 1 FROM playlists WHERE id=? AND user_id=?", (pid, c.from_user.id))
        if not await cur.fetchone(): await c.answer("❌", show_alert=True); return
        await db.execute("DELETE FROM playlists WHERE id=?", (pid,)); await db.execute("DELETE FROM playlist_tracks WHERE playlist_id=?", (pid,)); await db.commit()
    await c.answer("🗑 Удален", show_alert=True)
    try: await c.message.delete()
    except: pass

@dp.callback_query(F.data.startswith("opl_"))
async def opl(c):
    pid = int(c.data.split("_")[1])
    async with get_db() as db:
        cur = await db.execute("SELECT name FROM playlists WHERE id=? AND user_id=?", (pid, c.from_user.id)); pl = await cur.fetchone()
        if not pl: await c.answer("❌", show_alert=True); return
        cur = await db.execute("SELECT tracks.id, tracks.file_id FROM tracks JOIN playlist_tracks ON tracks.id=playlist_tracks.track_id WHERE playlist_tracks.playlist_id=?", (pid,)); tr = await cur.fetchall()
        if not tr: await c.message.answer(f"📋 «{pl[0]}» пуст."); await c.answer(); return
        await c.message.answer(f"📋 «{pl[0]}»:")
        for t in tr:
            await db.execute("UPDATE tracks SET plays=plays+1 WHERE id=?", (t[0],))
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🗑 Убрать", callback_data=f"rmpl_{pid}_{t[0]}")]])
            await c.message.answer_audio(t[1], reply_markup=kb)
        await db.commit()
    await c.answer()

@dp.callback_query(F.data.startswith("topl_"))
async def cpl(c):
    tid = int(c.data.split("_")[1])
    async with get_db() as db:
        cur = await db.execute("SELECT id, name FROM playlists WHERE user_id=?", (c.from_user.id,)); pls = await cur.fetchall()
    if not pls: await c.answer("Создай плейлист", show_alert=True); return
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"📋 {p[1]}", callback_data=f"apl_{p[0]}_{tid}")] for p in pls])
    await c.message.answer("Выбери:", reply_markup=kb); await c.answer()

@dp.callback_query(F.data.startswith("apl_"))
async def apl(c):
    p = c.data.split("_"); pid, tid = int(p[1]), int(p[2])
    async with get_db() as db:
        await db.execute("INSERT OR IGNORE INTO playlist_tracks (playlist_id, track_id) VALUES (?,?)", (pid, tid)); await db.commit()
    await c.answer("✅", show_alert=True)
    try: await c.message.delete()
    except: pass

@dp.callback_query(F.data.startswith("rmpl_"))
async def rmpl(c):
    p = c.data.split("_"); pid, tid = int(p[1]), int(p[2])
    async with get_db() as db:
        await db.execute("DELETE FROM playlist_tracks WHERE playlist_id=? AND track_id=?", (pid, tid)); await db.commit()
    await c.answer("🗑", show_alert=True)
    try: await c.message.delete()
    except: pass

@dp.message(Command("stats"))
async def stats(m):
    if m.from_user.id != ADMIN_ID: return
    async with get_db() as db:
        cur = await db.execute("SELECT COUNT(*) FROM tracks"); tt = (await cur.fetchone())[0]
        cur = await db.execute("SELECT COALESCE(SUM(plays),0) FROM tracks"); tp = (await cur.fetchone())[0]
        cur = await db.execute("SELECT COUNT(DISTINCT user_id) FROM favorites"); tu = (await cur.fetchone())[0]
        cur = await db.execute("SELECT COUNT(*) FROM playlists"); tpl = await cur.fetchone())[0]
    await m.answer(f"📊 <b>Стат:</b>\n🎵 Треков: {tt}\n🎧 Прослушиваний: {tp}\n👥 Избранных юзеров: {tu}\n📋 Плейлистов: {tpl}", parse_mode="HTML")

@dp.message(Command("manage"))
async def mg(m):
    if m.from_user.id != ADMIN_ID: return
    a = m.text.split(maxsplit=1)
    async with get_db() as db:
        if len(a)>=2:
            q = a[1].strip().lower(); cur = await db.execute("SELECT id, title, artist FROM tracks WHERE title LIKE ? OR artist LIKE ? ORDER BY id DESC", (f"%{q}%", f"%{q}%")); h = f"🔍 По «{a[1].strip()}»:"
        else:
            cur = await db.execute("SELECT id, title, artist FROM tracks ORDER BY id DESC LIMIT 30"); h = "🗑 Последние 30:"
        res = await cur.fetchall()
    if not res: await m.answer("📂 Пусто."); return
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"🗑 {format_track(t[2], t[1])}", callback_data=f"del_{t[0]}")] for t in res])
    await m.answer(h, reply_markup=kb)

@dp.message(F.text & ~F.text.startswith("/"))
async def sm(m, state: FSMContext):
    if await state.get_state() == PlaylistForm.waiting_name.state: return
    q = m.text.strip()
    if not q: return
    res = await run_search(q)
    if not res:
        async with get_db() as db:
            cur = await db.execute("SELECT COUNT(*) FROM tracks"); tot = (await cur.fetchone())[0]
        await m.answer("❌ База пуста." if tot==0 else f"❌ Ничего по «<b>{q}</b>»", parse_mode="HTML"); return
    lines = [f"{i}. {format_track(t[2], t[1])}" for i, t in enumerate(res, 1)]
    await m.answer(f"🎧 <b>Найдено {len(res)}:</b>\n\n" + "\n".join(lines) + "\n\n👇 Нажми номер:", parse_mode="HTML", reply_markup=num_buttons([t[0] for t in res]))

@dp.callback_query(F.data.startswith("track_"))
async def st(c):
    tid = int(c.data.split("_")[1])
    async with get_db() as db:
        cur = await db.execute("SELECT file_id FROM tracks WHERE id=?", (tid,)); r = await cur.fetchone()
        if not r: await c.answer("❌", show_alert=True); return
        await db.execute("UPDATE tracks SET plays=plays+1 WHERE id=?", (tid,)); await db.commit()
    await c.message.answer_audio(r[0], reply_markup=await track_keyboard(tid, c.from_user.id)); await c.answer()

@dp.callback_query(F.data.startswith("fav_"))
async def af(c):
    tid = int(c.data.split("_")[1])
    async with get_db() as db:
        await db.execute("INSERT OR IGNORE INTO favorites (user_id, track_id) VALUES (?,?)", (c.from_user.id, tid)); await db.commit()
    await c.answer("❤️")

@dp.callback_query(F.data.startswith("unfav_"))
async def uf(c):
    tid = int(c.data.split("_")[1])
    async with get_db() as db:
        await db.execute("DELETE FROM favorites WHERE user_id=? AND track_id=?", (c.from_user.id, tid)); await db.commit()
    await c.answer("💔")
    try: await c.message.delete()
    except: pass

@dp.callback_query(F.data.startswith("del_"))
async def dt(c):
    if c.from_user.id != ADMIN_ID: await c.answer("⛔", show_alert=True); return
    tid = int(c.data.split("_")[1])
    async with get_db() as db:
        cur = await db.execute("SELECT title, artist FROM tracks WHERE id=?", (tid,)); t = await cur.fetchone()
        if not t: await c.answer("❌", show_alert=True); return
        await db.execute("DELETE FROM tracks WHERE id=?", (tid,))
        await db.execute("DELETE FROM favorites WHERE track_id=?", (tid,))
        await db.execute("DELETE FROM playlist_tracks WHERE track_id=?", (tid,))
        await db.commit()
    await c.answer("✅ Удалено", show_alert=True)
    try: await c.message.delete()
    except: pass


# --- WEB-СЕРВЕР ДЛЯ KOYEB ---
# --- WEB-СЕРВЕР ДЛЯ KOYEB ---
WEBHOOK_PATH = "/webhook"
app = web.Application()
app.router.add_get("/", lambda r: web.Response(text="OK"))

async def on_startup(app):
    await init_db()
    port = int(os.environ.get("PORT", 8080))
    app_url = os.environ.get("KOYEB_APP_URL")
    
    if app_url:
        webhook_url = f"{app_url}{WEBHOOK_PATH}"
        print(f"Устанавливаю вебхук: {webhook_url}")
        
        try:
            await bot.set_webhook(webhook_url, drop_pending_updates=True)
            print("✅ Вебхук успешно установлен!")
        except Exception as e:
            print(f"❌ ОШИБКА ВЕБХУКА: {e}")
            print("⚠️ Переключаюсь на Polling...")
            asyncio.create_task(dp.start_polling(bot))
    else:
        print("KOYEB_APP_URL не найден, запускаю Polling...")
        asyncio.create_task(dp.start_polling(bot))

SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
setup_application(app, dp, bot=bot)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
