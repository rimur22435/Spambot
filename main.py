import os import json import asyncio import logging
from aiohttp import web from aiogram import Bot, Dispatcher, types from aiogram.filters import Command from aiogram.webhook.aiohttp_server import ( SimpleRequestHandler, setup_application, )
logging.basicConfig(level=logging.INFO)
BOT_TOKEN = os.environ["BOT_TOKEN"] OWNER_ID = int(os.environ["OWNER_ID"]) WEBHOOK_URL = os.environ["WEBHOOK_URL"].rstrip("/") WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]
WEBHOOK_PATH = "/webhook" INTERVAL = 1800 DATA_FILE = "/tmp/bot_data.json"
bot = Bot(token=BOT_TOKEN) dp = Dispatcher()
groups = set() post_text = "" autopost_enabled = False scheduler_task = None
def load_data(): global groups, post_text, autopost_enabled
try:
    with open(DATA_FILE, "r", encoding="utf-8") as file:
        data = json.load(file)

    groups = set(data.get("groups", []))
    post_text = data.get("post_text", "")
    autopost_enabled = data.get("autopost_enabled", False)

except (FileNotFoundError, json.JSONDecodeError):
    logging.info("Файл настроек пока не создан")
def save_data(): data = { "groups": list(groups), "post_text": post_text, "autopost_enabled": autopost_enabled, }
with open(DATA_FILE, "w", encoding="utf-8") as file:
    json.dump(data, file, ensure_ascii=False)
def is_owner(message: types.Message): return message.from_user is not None and message.from_user.id == OWNER_ID
async def publish_to_groups(): if not post_text: return
if not groups:
    return

for group_id in list(groups):
    try:
        await bot.send_message(group_id, post_text)
        await asyncio.sleep(2)

    except Exception:
        logging.exception("Ошибка отправки в группу %s", group_id)
async def scheduler(): while True: await asyncio.sleep(INTERVAL)
    if autopost_enabled:
        logging.info("Запуск запланированной публикации")
        await publish_to_groups()
@dp.message(Command("start")) async def start(message: types.Message): if not is_owner(message): return
await message.answer(
    "Spambot запущен.\n\n"
    "/help — команды\n"
    "/setpost Текст — задать публикацию\n"
    "/addhere — добавить группу\n"
    "/groups — количество групп\n"
    "/publish — опубликовать сейчас\n"
    "/autopost_on — включить расписание\n"
    "/autopost_off — выключить расписание\n"
    "/status — состояние бота"
)
@dp.message(Command("help")) async def help_command(message: types.Message): if not is_owner(message): return
await message.answer(
    "/setpost Текст — задать текст\n"
    "/addhere — добавить текущую группу\n"
    "/groups — количество групп\n"
    "/publish — отправить публикацию сейчас\n"
    "/autopost_on — включить расписание\n"
    "/autopost_off — выключить расписание\n"
    "/status — состояние"
)
@dp.message(Command("setpost")) async def set_post(message: types.Message): global post_text
if not is_owner(message):
    return

parts = (message.text or "").split(maxsplit=1)

if len(parts) < 2:
    await message.answer("Использование: /setpost Текст публикации")
    return

post_text = parts[1]
save_data()

await message.answer("Текст публикации сохранён.")
@dp.message(Command("addhere")) async def add_group(message: types.Message): if not is_owner(message): return
if message.chat.type not in ("group", "supergroup"):
    await message.answer("Отправь /addhere непосредственно в группе.")
    return

groups.add(message.chat.id)
save_data()

await message.answer("Группа добавлена в список.")
@dp.message(Command("groups")) async def list_groups(message: types.Message): if not is_owner(message): return
await message.answer(f"Подключено групп: {len(groups)}")
@dp.message(Command("publish")) async def publish_now(message: types.Message): if not is_owner(message): return
if not post_text:
    await message.answer("Сначала задай текст через /setpost.")
    return

if not groups:
    await message.answer("Сначала добавь группу через /addhere.")
    return

await message.answer("Начинаю публикацию.")
await publish_to_groups()
await message.answer("Публикация завершена.")
@dp.message(Command("autopost_on")) async def autopost_on(message: types.
