import os
import json
import asyncio
import logging

from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])
WEBHOOK_URL = os.environ["WEBHOOK_URL"].rstrip("/")
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]

WEBHOOK_PATH = "/webhook"
INTERVAL = 1800
DATA_FILE = "/tmp/bot_data.json"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

groups = set()
post_text = ""
autopost_enabled = False
scheduler_task = None


def load_data():
    global groups, post_text, autopost_enabled
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
        groups = set(data.get("groups", []))
        post_text = data.get("post_text", "")
        autopost_enabled = data.get("autopost_enabled", False)
    except (FileNotFoundError, json.JSONDecodeError):
        logging.info("No saved settings yet")


def save_data():
    data = {
        "groups": list(groups),
        "post_text": post_text,
        "autopost_enabled": autopost_enabled,
    }
    with open(DATA_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False)


def is_owner(message: types.Message):
    return message.from_user is not None and message.from_user.id == OWNER_ID


async def publish_to_groups():
    if not post_text or not groups:
        return
    for group_id in list(groups):
        try:
            await bot.send_message(group_id, post_text)
            await asyncio.sleep(2)
        except Exception:
            logging.exception("Failed to send to group %s", group_id)


async def scheduler():
    while True:
        await asyncio.sleep(INTERVAL)
        if autopost_enabled:
            logging.info("Scheduled publication started")
            await publish_to_groups()


@dp.message(Command("start"))
async def start(message: types.Message):
    if not is_owner(message):
        return
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


@dp.message(Command("help"))
async def help_command(message: types.Message):
    if not is_owner(message):
        return
    await message.answer(
        "/setpost Текст — задать текст\n"
        "/addhere — добавить текущую группу\n"
        "/groups — количество групп\n"
        "/publish — отправить публикацию сейчас\n"
        "/autopost_on — включить расписание\n"
        "/autopost_off — выключить расписание\n"
        "/status — состояние"
    )


@dp.message(Command("setpost"))
async def set_post(message: types.Message):
    global post_text
    if not is_owner(message):
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /setpost Текст публикации")
        return
    post_text = parts[1]
    save_data()
    await message.answer("Текст публикации сохранён.")


@dp.message(Command("addhere"))
async def add_group(message: types.Message):
    if not is_owner(message):
        return
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Отправь /addhere непосредственно в группе.")
        return
    groups.add(message.chat.id)
    save_data()
    await message.answer("Группа добавлена.")


@dp.message(Command("groups"))
async def list_groups(message: types.Message):
    if not is_owner(message):
        return
    await message.answer(f"Подключено групп: {len(groups)}")


@dp.message(Command("publish"))
async def publish_now(message: types.Message):
    if not is_owner(message):
        return
    if not post_text:
        await message.answer("Сначала задай текст через /setpost.")
        return
    if not groups:
        await message.answer("Сначала добавь группы через /addhere.")
        return
    await message.answer("Начинаю публикацию.")
    await publish_to_groups()
    await message.answer("Попытка отправки завершена. Проверь группы и логи.")


@dp.message(Command("autopost_on"))
async def autopost_on(message: types.Message):
    global autopost_enabled
    if not is_owner(message):
        return
    if not post_text or not groups:
        await message.answer("Сначала задай текст через /setpost и добавь группы через /addhere.")
        return
    autopost_enabled = True
    save_data()
    await message.answer("Расписание включено. Интервал — 30 минут, пока сервис работает.")


@dp.message(Command("autopost_off"))
async def autopost_off(message: types.Message):
    global autopost_enabled
    if not is_owner(message):
        return
    autopost_enabled = False
    save_data()
    await message.answer("Расписание выключено.")


@dp.message(Command("status"))
async def status(message: types.Message):
    if not is_owner(message):
        return
    await message.answer(
        "Состояние: работает\n"
        f"Групп: {len(groups)}\n"
        f"Текст: {'задан' if post_text else 'не задан'}\n"
        f"Автопубликация: {'включена' if autopost_enabled else 'выключена'}"
    )


async def on_startup(bot: Bot):
    global scheduler_task
    load_data()
    await bot.set_webhook(
        url=WEBHOOK_URL + WEBHOOK_PATH,
        secret_token=WEBHOOK_SECRET,
    )
    scheduler_task = asyncio.create_task(scheduler())
    logging.info("Bot started")


async def on_shutdown(bot: Bot):
    global scheduler_task
    if scheduler_task:
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass
    await bot.delete_webhook()
    await bot.session.close()


async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    app = web.Application()
    SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
        secret_token=WEBHOOK_SECRET,
    ).register(app, path=WEBHOOK_PATH)

    setup_application(app, dp, bot=bot)
    app.router.add_get(
        "/",
        lambda request: web.Response(text="Spambot is running"),
    )

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()

    logging.info("HTTP server started on port %s", port)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
