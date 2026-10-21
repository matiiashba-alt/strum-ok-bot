import asyncio
import os
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo
from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is not set")

TZ = ZoneInfo("Europe/Kyiv")
QUEUES = [f"{n}.{s}" for n in range(1, 7) for s in (1, 2)]

router = Router()
db = sqlite3.connect("strum_ok.db")
db.execute("""
CREATE TABLE IF NOT EXISTS users (
    telegram_id INTEGER PRIMARY KEY,
    queue TEXT,
    notifications INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
)
""")
db.commit()

def queue_keyboard():
    rows = []
    for i in range(0, len(QUEUES), 2):
        rows.append([
            InlineKeyboardButton(text=QUEUES[i], callback_data=f"queue:{QUEUES[i]}"),
            InlineKeyboardButton(text=QUEUES[i+1], callback_data=f"queue:{QUEUES[i+1]}")
        ])
    rows.append([InlineKeyboardButton(text="🔎 Не знаю свою підчергу", url="https://svitlo.oe.if.ua/")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⚡ Стан зараз", callback_data="status"),
            InlineKeyboardButton(text="📅 Сьогодні", callback_data="today")
        ],
        [
            InlineKeyboardButton(text="📆 Завтра", callback_data="tomorrow"),
            InlineKeyboardButton(text="🔄 Змінити підчергу", callback_data="change_queue")
        ],
        [InlineKeyboardButton(text="🔔 Сповіщення", callback_data="notifications")]
    ])

def get_user(user_id):
    return db.execute(
        "SELECT queue, notifications FROM users WHERE telegram_id=?",
        (user_id,)
    ).fetchone()

@router.message(CommandStart())
async def start(message: Message):
    user = get_user(message.from_user.id)
    if user and user[0]:
        await message.answer(
            f"⚡ <b>Струм ОК</b>\n\nВаша підчерга: <b>{user[0]}</b>\nОберіть потрібний розділ:",
            reply_markup=main_keyboard()
        )
    else:
        await message.answer(
            "⚡ <b>Струм ОК</b>\n\n"
            "Бот сповіщатиме про графіки відключення електроенергії "
            "в Івано-Франківську.\n\nОберіть свою підчергу:",
            reply_markup=queue_keyboard()
        )

@router.callback_query(F.data.startswith("queue:"))
async def choose_queue(callback: CallbackQuery):
    queue = callback.data.split(":", 1)[1]
    db.execute("""
        INSERT INTO users(telegram_id, queue, notifications, created_at)
        VALUES (?, ?, 1, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET queue=excluded.queue
    """, (callback.from_user.id, queue, datetime.now(TZ).isoformat()))
    db.commit()
    await callback.message.edit_text(
        f"✅ Підчергу збережено: <b>{queue}</b>\n\n"
        "Тепер бот зможе показувати персональний графік.",
        reply_markup=main_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data == "change_queue")
async def change_queue(callback: CallbackQuery):
    await callback.message.edit_text("Оберіть свою підчергу:", reply_markup=queue_keyboard())
    await callback.answer()

@router.callback_query(F.data == "notifications")
async def notifications(callback: CallbackQuery):
    user = get_user(callback.from_user.id)
    if not user:
        await callback.answer("Спочатку оберіть підчергу.", show_alert=True)
        return
    new_value = 0 if user[1] else 1
    db.execute("UPDATE users SET notifications=? WHERE telegram_id=?", (new_value, callback.from_user.id))
    db.commit()
    state = "увімкнено 🔔" if new_value else "вимкнено 🔕"
    await callback.answer(f"Сповіщення {state}", show_alert=True)

@router.callback_query(F.data.in_({"status", "today", "tomorrow"}))
async def schedule_placeholder(callback: CallbackQuery):
    user = get_user(callback.from_user.id)
    if not user:
        await callback.answer("Спочатку оберіть підчергу.", show_alert=True)
        return
    labels = {
        "status": "⚡ Стан зараз",
        "today": "📅 Графік на сьогодні",
        "tomorrow": "📆 Графік на завтра",
    }
    await callback.message.edit_text(
        f"<b>{labels[callback.data]}</b>\n"
        f"Підчерга: <b>{user[0]}</b>\n\n"
        "🔧 Модуль автоматичного отримання офіційного графіка "
        "Прикарпаттяобленерго підключається на наступному етапі.\n\n"
        "Поки що перевірити офіційні дані можна на svitlo.oe.if.ua.",
        reply_markup=main_keyboard()
    )
    await callback.answer()

async def health(request):
    return web.Response(text="Strum OK bot is running")

async def main():
    app = web.Application()
    app.router.add_get("/", health)

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)
if __name__ == "__main__":
    asyncio.run(main())
