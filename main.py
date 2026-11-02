import os import asyncio import logging
from aiogram import Bot, Dispatcher, types from aiogram.filters import Command from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application from aiohttp import web
logging.basicConfig(level=logging.INFO)
BOT_TOKEN = os.environ["BOT_TOKEN"] OWNER_ID = int(os.environ["OWNER_ID"]) WEBHOOK_URL = os.environ["WEBHOOK_URL"] WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]
bot = Bot(token=BOT_TOKEN) dp = Dispatcher()
Список групп, в которых бот зарегистрирован
groups = set()
@dp.message(Command("start")) async def start(message: types.Message): if message.from_user.id == OWNER_ID: await message.answer( "Бот запущен.\n" "/help — список команд\n" "/status — состояние бота\n" "/addhere — зарегистрировать эту группу" ) else: await message.answer("Бот работает.")
@dp.message(Command("help")) async def help_command(message: types.Message): if message.from_user.id != OWNER_ID: return
await message.answer(
    "/status — состояние бота\n"
    "/addhere — добавить текущую группу\n"
    "/groups — список зарегистрированных групп"
)
@dp.message(Command("status")) async def status(message: types.Message): if message.from_user.id == OWNER_ID: await message.answer("Бот работает. Зарегистрировано групп: " + str(len(groups)))
@dp.message(Command("addhere")) async def add_group(message: types.Message): if message.from_user.id != OWNER_ID: return
if message.chat.type not in ("group", "supergroup"):
    await message.answer("Эту команду нужно отправить в группе.")
    return

groups.add(message.chat.id)
await message.answer("Группа зарегистрирована.")
@dp.message(Command("groups")) async def list_groups(message: types.Message): if message.from_user.id == OWNER_ID: await message.answer("Зарегистрировано групп: " + str(len(groups)))
async def on_startup(bot: Bot): await bot.set_webhook( url=WEBHOOK_URL, secret_token=WEBHOOK_SECRET )
async def on_shutdown(bot: Bot): await bot.delete_webhook()
async def main(): app = web.Application()
SimpleRequestHandler(
    dispatcher=dp,
    bot=bot,
    secret_token=WEBHOOK_SECRET
).register(app, path="/webhook")

setup_application(app, dp, bot=bot)

dp.startup.register(on_startup)
dp.shutdown.register(on_shutdown)

app.router.add_get("/", lambda request: web.Response(text="Bot is running"))

runner = web.AppRunner(app)
await runner.setup()

site = web.TCPSite(
    runner,
    "0.0.0.0",
    int(os.environ.get("PORT", 10000))
)

await site.start()

try:
    await asyncio.Event().wait()
finally:
    await runner.cleanup()
    await bot.session.close()
if name == "main": asyncio.run(main())
