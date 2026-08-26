# webapp_api.py
#
# Добавляет к твоему боту:
#   1) валидацию Telegram Mini App initData (проверка подписи)
#   2) JSON API, который повторяет ту же логику, что уже есть в main.py:
#      /api/search  -> run_search()          (как кнопка "🔍 Поиск")
#      /api/top     -> ORDER BY plays DESC   (как кнопка "🔥 Топ")
#      /api/new     -> ORDER BY created_at   (как кнопка "🆕 Новое")
#      /api/random  -> тот же алгоритм, что и _send_random_track()
#      /api/favorites -> избранное текущего пользователя
#   3) обработчик web_app_data — когда Mini App просит "воспроизвести"
#      или "добавить в избранное", бот реально присылает аудио/реакцию
#      в чат, теми же функциями, что уже используются в main.py.
#
# Ничего в main.py не переписывается — этот файл только добавляет
# новые route'ы и один новый message-хендлер. Импортируется в конце
# main.py и подключается в функции main().

import hmac
import hashlib
import json
import os
import random
import urllib.parse
import time

from aiohttp import web
from aiogram import F, types


def validate_init_data(init_data: str, bot_token: str, max_age: int = 86400):
    """Проверяет подпись Telegram WebApp initData.
    Возвращает dict с полем 'user' (Telegram User как dict) или None, если подпись неверна/просрочена."""
    if not init_data:
        return None
    try:
        parsed = dict(urllib.parse.parse_qsl(init_data, strict_parsing=True))
    except ValueError:
        return None
    received_hash = parsed.pop("hash", None)
    if not received_hash:
        return None
    auth_date = parsed.get("auth_date")
    if auth_date:
        try:
            if time.time() - int(auth_date) > max_age:
                return None
        except ValueError:
            return None
    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
    secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    computed_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(computed_hash, received_hash):
        return None
    user = None
    if parsed.get("user"):
        try:
            user = json.loads(parsed["user"])
        except (ValueError, TypeError):
            user = None
    return {"user": user, "auth_date": auth_date}


def register_webapp(app: web.Application, dp, bot, db_pool_getter, bot_token: str,
                     is_subscribed, run_search, format_track, track_keyboard,
                     num_buttons, static_dir: str, max_playlists: int = 5):
    """Регистрирует API-роуты и обработчик web_app_data.
    Вызови это один раз из main() в main.py — см. инструкцию ниже."""

    async def _auth_user(request):
        init_data = request.headers.get("X-Telegram-Init-Data", "")
        data = validate_init_data(init_data, bot_token)
        if not data or not data.get("user"):
            return None
        return data["user"]

    async def _require_user(request):
        user = await _auth_user(request)
        if not user:
            return None, web.json_response({"error": "unauthorized"}, status=401)
        if not await is_subscribed(user["id"]):
            return None, web.json_response({"error": "subscription_required"}, status=403)
        return user, None

    async def serve_app(request):
        return web.FileResponse(os.path.join(static_dir, "index.html"))

    async def api_search(request):
        user, err = await _require_user(request)
        if err:
            return err
        q = request.query.get("q", "").strip()
        offset = int(request.query.get("offset", 0) or 0)
        rows, total = await run_search(q, offset)
        return web.json_response({
            "total": total,
            "items": [{"id": r["id"], "title": r["title"], "artist": r["artist"]} for r in rows],
        })

    async def api_top(request):
        user, err = await _require_user(request)
        if err:
            return err
        pool = db_pool_getter()
        async with pool.acquire() as conn:
            res = await conn.fetch(
                "SELECT id, artist, title, plays FROM tracks ORDER BY plays DESC LIMIT 10"
            )
        return web.json_response({
            "items": [{"id": r["id"], "title": r["title"], "artist": r["artist"], "plays": r["plays"]} for r in res]
        })

    async def api_new(request):
        user, err = await _require_user(request)
        if err:
            return err
        pool = db_pool_getter()
        async with pool.acquire() as conn:
            res = await conn.fetch(
                "SELECT id, title, artist, created_at FROM tracks ORDER BY created_at DESC LIMIT 10"
            )
        return web.json_response({
            "items": [{"id": r["id"], "title": r["title"], "artist": r["artist"]} for r in res]
        })

    async def api_random(request):
        user, err = await _require_user(request)
        if err:
            return err
        pool = db_pool_getter()
        async with pool.acquire() as conn:
            bounds = await conn.fetchrow("SELECT MIN(id) AS mn, MAX(id) AS mx FROM tracks")
            if not bounds or bounds["mx"] is None:
                return web.json_response({"item": None})
            rand_id = random.randint(bounds["mn"], bounds["mx"])
            row = await conn.fetchrow(
                "SELECT id, title, artist FROM tracks WHERE id >= $1 ORDER BY id LIMIT 1", rand_id
            )
            if not row:
                row = await conn.fetchrow("SELECT id, title, artist FROM tracks ORDER BY id LIMIT 1")
        item = {"id": row["id"], "title": row["title"], "artist": row["artist"]} if row else None
        return web.json_response({"item": item})

    async def api_favorites(request):
        user, err = await _require_user(request)
        if err:
            return err
        pool = db_pool_getter()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT tracks.id, tracks.title, tracks.artist FROM tracks "
                "JOIN favorites ON tracks.id = favorites.track_id "
                "WHERE favorites.user_id=$1 ORDER BY favorites.added_at DESC LIMIT 50",
                user["id"],
            )
        return web.json_response({
            "items": [{"id": r["id"], "title": r["title"], "artist": r["artist"]} for r in rows]
        })

    async def api_playlists(request):
        user, err = await _require_user(request)
        if err:
            return err
        pool = db_pool_getter()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT p.id, p.name, COUNT(pt.track_id) AS cnt "
                "FROM playlists p LEFT JOIN playlist_tracks pt ON pt.playlist_id = p.id "
                "WHERE p.user_id=$1 GROUP BY p.id, p.name ORDER BY p.id",
                user["id"],
            )
        return web.json_response({
            "max": max_playlists,
            "items": [{"id": r["id"], "name": r["name"], "count": r["cnt"]} for r in rows],
        })

    async def api_playlist_tracks(request):
        user, err = await _require_user(request)
        if err:
            return err
        pid = int(request.match_info["pid"])
        pool = db_pool_getter()
        async with pool.acquire() as conn:
            pl = await conn.fetchrow(
                "SELECT id, name FROM playlists WHERE id=$1 AND user_id=$2", pid, user["id"]
            )
            if not pl:
                return web.json_response({"error": "not_found"}, status=404)
            rows = await conn.fetch(
                "SELECT tracks.id, tracks.title, tracks.artist FROM tracks "
                "JOIN playlist_tracks ON tracks.id = playlist_tracks.track_id "
                "WHERE playlist_tracks.playlist_id=$1 ORDER BY playlist_tracks.added_at",
                pid,
            )
        return web.json_response({
            "name": pl["name"],
            "items": [{"id": r["id"], "title": r["title"], "artist": r["artist"]} for r in rows],
        })

    app.router.add_get("/app", serve_app)
    app.router.add_get("/api/search", api_search)
    app.router.add_get("/api/top", api_top)
    app.router.add_get("/api/new", api_new)
    app.router.add_get("/api/random", api_random)
    app.router.add_get("/api/favorites", api_favorites)
    app.router.add_get("/api/playlists", api_playlists)
    app.router.add_get("/api/playlists/{pid}", api_playlist_tracks)

    @dp.message(F.web_app_data)
    async def webapp_data_handler(m: types.Message):
        try:
            payload = json.loads(m.web_app_data.data)
        except (ValueError, TypeError):
            return
        action = payload.get("action")
        pool = db_pool_getter()

        if action == "play":
            tid = int(payload.get("track_id", 0) or 0)
            async with pool.acquire() as conn:
                r = await conn.fetchrow("SELECT id, file_id FROM tracks WHERE id=$1", tid)
                if not r:
                    await m.answer("❌ Трек не найден")
                    return
                await conn.execute("UPDATE tracks SET plays=plays+1 WHERE id=$1", tid)
            await m.answer_audio(r["file_id"], reply_markup=await track_keyboard(tid, m.from_user.id))

        elif action == "fav":
            tid = int(payload.get("track_id", 0) or 0)
            async with pool.acquire() as conn:
                exists = await conn.fetchval(
                    "SELECT 1 FROM favorites WHERE user_id=$1 AND track_id=$2", m.from_user.id, tid
                )
                if exists:
                    await conn.execute(
                        "DELETE FROM favorites WHERE user_id=$1 AND track_id=$2", m.from_user.id, tid
                    )
                    await m.answer("💔 Убрано из избранного")
                else:
                    await conn.execute(
                        "INSERT INTO favorites (user_id, track_id) VALUES ($1,$2) ON CONFLICT DO NOTHING",
                        m.from_user.id, tid,
                    )
                    await m.answer("❤️ Добавлено в избранное")

        elif action == "playlist_create":
            name = (payload.get("name") or "").strip()
            if not name or len(name) > 50:
                await m.answer("❌ Название плейлиста — от 1 до 50 символов.")
                return
            async with pool.acquire() as conn:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM playlists WHERE user_id=$1", m.from_user.id
                )
                if count >= max_playlists:
                    await m.answer(f"❌ Максимум {max_playlists} плейлистов. Удали один перед созданием нового.")
                    return
                await conn.execute(
                    "INSERT INTO playlists (user_id, name) VALUES ($1, $2)", m.from_user.id, name
                )
            import html as _html
            await m.answer(f"✅ «{_html.escape(name)}» создан!")

        elif action == "playlist_delete":
            pid = int(payload.get("playlist_id", 0) or 0)
            async with pool.acquire() as conn:
                owned = await conn.fetchval(
                    "SELECT 1 FROM playlists WHERE id=$1 AND user_id=$2", pid, m.from_user.id
                )
                if not owned:
                    await m.answer("❌ Плейлист не найден.")
                    return
                await conn.execute("DELETE FROM playlists WHERE id=$1", pid)
            await m.answer("🗑 Плейлист удалён")

        elif action == "playlist_add":
            pid = int(payload.get("playlist_id", 0) or 0)
            tid = int(payload.get("track_id", 0) or 0)
            async with pool.acquire() as conn:
                pl = await conn.fetchrow(
                    "SELECT name FROM playlists WHERE id=$1 AND user_id=$2", pid, m.from_user.id
                )
                if not pl:
                    await m.answer("❌ Плейлист не найден.")
                    return
                await conn.execute(
                    "INSERT INTO playlist_tracks (playlist_id, track_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    pid, tid,
                )
            import html as _html
            await m.answer(f"✅ Добавлено в «{_html.escape(pl['name'])}»")

        elif action == "playlist_remove":
            pid = int(payload.get("playlist_id", 0) or 0)
            tid = int(payload.get("track_id", 0) or 0)
            async with pool.acquire() as conn:
                pl = await conn.fetchrow(
                    "SELECT name FROM playlists WHERE id=$1 AND user_id=$2", pid, m.from_user.id
                )
                if not pl:
                    await m.answer("❌ Плейлист не найден.")
                    return
                await conn.execute(
                    "DELETE FROM playlist_tracks WHERE playlist_id=$1 AND track_id=$2", pid, tid
                )
            import html as _html
            await m.answer(f"🗑 Убрано из «{_html.escape(pl['name'])}»")

        elif action == "search":
            query = (payload.get("query") or "").strip()
            if not query:
                return
            rows, total = await run_search(query)
            if not rows:
                await m.answer(f"❌ Ничего не найдено по запросу «{query}»")
                return
            import html as _html
            lines = [f"{i}. {_html.escape(format_track(r['artist'], r['title']))}" for i, r in enumerate(rows, 1)]
            ids = [r["id"] for r in rows]
            kb = types.InlineKeyboardMarkup(inline_keyboard=num_buttons(ids))
            await m.answer(
                f"🔍 Результаты по «{_html.escape(query)}»:\n\n" + "\n".join(lines),
                reply_markup=kb, parse_mode="HTML",
            )
