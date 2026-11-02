import os import asyncio import logging from aiogram import Bot, Dispatcher from aiogram.filters import Command from aiogram.types import Message from aiogram.client.default import DefaultBotProperties
logging.basicConfig(level=logging.INFO)
TOKEN = os.environ["BOT_TOKEN"] OWNER_ID = int(os.environ["OWNER_ID"])
bot = Bot( TOKEN, default=DefaultBotProperties() ) dp = Dispatcher()
groups = set()
def owner(message): return message.from_user and message.from_user.id == OWNER_ID
@dp.message(Command("start")) async def start(message: Message): await message.answer( "SPAMBOT запущен.\n" "/help — команды\n" "/groups — список групп\n" "/addhere — добавить группу\n" "/status — статус" )
@dp.message(Command("help")) async def help_command(message: Message): await start(message)
@dp.message(Command("addhere")) async def add_group(message: Message): if not owner(message): return if message.chat.type not in ("group", "supergroup"): await message.answer("Вызови команду внутри группы.") return groups.add(message.chat.id) await message.answer("Группа добавлена.")
@dp.message(Command("groups")) async def list_groups(message: Message): if not owner(message): return await message.answer(str(list(groups)) or "Групп пока нет.")
@dp.message(Command("status")) async def status(message: Message): if owner(message): await message.answer("SPAMBOT работает.")
async def main(): await dp.start_polling(bot)
if name == "main": asyncio.run(main())
