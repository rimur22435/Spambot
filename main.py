import os import json import asyncio import logging
from aiohttp import web from aiogram import Bot, Dispatcher, types from aiogram.filters import Command from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
logging.basicConfig(level=logging.INFO)
BOT_TOKEN = os.environ["BOT_TOKEN"] OWNER_ID = int(os.environ["OWNER_ID"]) WEBHOOK_URL = os.environ["WEBHOOK_URL"] WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]
INTERVAL = 1800 DATA_FILE = "bot_data.json"
bot = Bot(token=BOT_TOKEN) dp = Dispatcher()
groups = set() post_text = "" autopost_enabled = False scheduler_task = None
def load_data(): global groups, post_text, autopost_enabled
try:
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    groups = set(data.get("groups", []))
    post_text = data.get("post_text", "")
    autopost_enabled = data.get("autopost_enabled", False)
except (FileNotFoundError, json.JSONDecodeError):
    pass
def save_data(): with open(DATA_FILE, "w", encoding="utf-8") as f: json.dump({ "groups": list(groups), "post_text": post_text, "autopost_enabled": autopost_enabled }, f, ensure_ascii=False)
def is_owner(message: types.Message): return message.from_user and message.from_user.id == OWNER_ID
async def publish_to_groups(): if not post_text or not groups: return
failed = []

for group_id in list(groups):
    try:
        await bot.send_message(group_id, post_text)
        await asyncio.sleep(2)
    except Exception as error:
        logging.warning("Не удалось отправить сообщение в %s: %s", group_id, error)
        failed.append(group_id)

if failed:
    logging.warning("Не удалось отправить в группы: %s", failed)
async def scheduler(): while True: await asyncio.sleep(INTERVAL)
    if autopost_enabled:
        await publish_to_groups()
@dp.message(Command("start")) async def start(message: types.Message): if is_owner(message): await message.answer( "Бот запущен.\n\n" "/help — команды\n" "/setpost Текст — задать публикацию\n" "/addhere — добавить группу\n" "/groups — список групп\n" "/publish — отправить сейчас\n" "/autopost_on — включить автопубликацию\n" "/autopost_off — выключить автопубликацию\n" "/status — состояние" ) else: await message.answer("Бот работает.")
@dp.message(Command("help")) async def help_command(message: types.Message): if not is_owner(message): return
await message.answer(
    "/setpost Текст — задать текст публикации\n"
    "/addhere — добавить текущую группу\n"
    "/groups — список групп\n"
    "/publish — отправить публикацию сейчас\n"
    "/autopost_on — включить публикации каждые 30 минут\n"
    "/autopost_off — выключить публикации\n"
    "/status — состояние бота"
)
@dp.message(Command("setpost")) async def set_post(message: types.Message): global post_text
if not is_owner(message):
    return

parts = (message.text or "").split(maxsplit=1)

if len(parts) < 2:
    await message.answer("Напиши текст после команды /setpost.")
    return

post_text = parts[1]
save_data()
await message.answer("Текст публикации сохранён.")
@dp.message(Command("addhere")) async def add_group(message: types.Message): if not is_owner(message): return
if message.chat.type not in ("group", "supergroup"):
    await message.answer("Отправь эту команду в нужной группе.")
    return

groups.add(message.chat.id)
save_data()
await message.answer("Группа добавлена.")
@dp.message(Command("groups")) async def list_groups(message: types.Message): if not is_owner(message): return
await message.answer(f"Зарегистрировано групп: {len(groups)}")
@dp.message(Command("publish")) async def publish_now(message: types.Message): if not is_owner(message): return
if not post_text:
    await message.answer("Сначала задай текст командой /setpost.")
    return

if not groups:
    await message.answer("Сначала добавь группы командой /addhere.")
    return

await message.answer("Начинаю публикацию.")
await publish_to_groups()
await message.answer("Отправка завершена.")
@dp.message(Command("autopost_on")) async def autopost_on(message: types.Message): global autopost_enabled
if not is_owner(message):
    return
