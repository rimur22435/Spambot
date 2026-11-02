import asyncio
import json
import logging
import os
import re
import signal
import time

from aiohttp import ClientSession, ClientTimeout, web
from aiogram import Bot, Dispatcher, F, types
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramMigrateToChat,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandObject
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("spambot")


# ---------------------------------------------------------------------------
# Настройки окружения (Render -> Environment)
# ---------------------------------------------------------------------------

def env_str(name):
    return os.environ.get(name, "").strip()


def similar_names(name):
    """Имена переменных, похожие на ожидаемое (значения не показываем)."""
    prefix = name.split("_")[0]
    return [ascii(k) for k in os.environ if prefix in k.upper()]


def missing_message(name):
    text = f"Не найдена переменная окружения {name}."
    similar = similar_names(name)
    if similar:
        text += " Похожие имена в Render: " + ", ".join(similar) + " - проверьте написание."
    else:
        text += " Похожих имён в Render нет."
    return text


_problems = []

BOT_TOKEN = env_str("BOT_TOKEN")
if not BOT_TOKEN:
    _problems.append(missing_message("BOT_TOKEN"))

OWNER_ID = 0
if not env_str("OWNER_ID"):
    _problems.append(missing_message("OWNER_ID"))
else:
    try:
        OWNER_ID = int(env_str("OWNER_ID"))
    except ValueError:
        _problems.append("OWNER_ID должен быть числом (ваш числовой Telegram ID).")

WEBHOOK_SECRET = env_str("WEBHOOK_SECRET")
if not WEBHOOK_SECRET:
    _problems.append(missing_message("WEBHOOK_SECRET"))
elif not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", WEBHOOK_SECRET):
    _problems.append(
        "WEBHOOK_SECRET может содержать только латинские буквы, цифры, _ и -"
        " (до 256 символов)."
    )

# WEBHOOK_URL - это адрес сервиса БЕЗ /webhook. Если переменной нет,
# берём RENDER_EXTERNAL_URL, который Render задаёт сам.
BASE_URL = (env_str("WEBHOOK_URL") or env_str("RENDER_EXTERNAL_URL")).rstrip("/")
if BASE_URL.endswith("/webhook"):
    BASE_URL = BASE_URL[: -len("/webhook")].rstrip("/")
if not BASE_URL:
    _problems.append(missing_message("WEBHOOK_URL"))
elif not BASE_URL.startswith("https://"):
    _problems.append("WEBHOOK_URL должен начинаться с https://")

if _problems:
    for problem in _problems:
        log.error(problem)
    raise SystemExit("Бот не запущен из-за настроек: " + " | ".join(_problems))

WEBHOOK_PATH = "/webhook"

# Постоянное хранилище (бесплатный Upstash Redis через HTTP).
UPSTASH_URL = env_str("UPSTASH_REDIS_REST_URL").rstrip("/")
UPSTASH_TOKEN = env_str("UPSTASH_REDIS_REST_TOKEN")
USE_REMOTE = bool(UPSTASH_URL and UPSTASH_TOKEN)
STATE_KEY = "spambot:state"
DATA_FILE = "/tmp/bot_data.json"  # запасной вариант, на Render может стираться

DEFAULT_INTERVAL_MIN = 30
MIN_INTERVAL_MIN = 10
MAX_INTERVAL_MIN = 1440
MAX_POST_LEN = 4096
PAUSE_BETWEEN_GROUPS = 2
CHECK_EVERY = 60
ANON_ADMIN_ID = 1087968824  # GroupAnonymousBot
GROUP_TYPES = ("group", "supergroup")

START_TS = time.time()

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

state = {}
http_session = None
scheduler_task = None
remote_loaded = not USE_REMOTE
storage_error = ""
publish_lock = asyncio.Lock()
save_lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# Состояние и хранение
# ---------------------------------------------------------------------------

def default_state():
    return {
        "groups": {},  # "chat_id" -> {"title": "..."}
        "post_text": "",
        "autopost_enabled": False,
        "interval_min": DEFAULT_INTERVAL_MIN,
        "last_post_ts": 0.0,
        "last_report": "",
    }


state.update(default_state())


def apply_loaded(data):
    new = default_state()
    groups = data.get("groups", {})
    if isinstance(groups, list):  # старый формат: просто список id
        groups = {str(g): {"title": str(g)} for g in groups}
    if isinstance(groups, dict):
        for key, value in groups.items():
            title = value.get("title") if isinstance(value, dict) else None
            new["groups"][str(key)] = {"title": str(title or key)}
    new["post_text"] = str(data.get("post_text", ""))
    new["autopost_enabled"] = bool(data.get("autopost_enabled", False))
    try:
        interval = int(data.get("interval_min", DEFAULT_INTERVAL_MIN))
    except (TypeError, ValueError):
        interval = DEFAULT_INTERVAL_MIN
    new["interval_min"] = min(max(interval, MIN_INTERVAL_MIN), MAX_INTERVAL_MIN)
    try:
        new["last_post_ts"] = float(data.get("last_post_ts", 0) or 0)
    except (TypeError, ValueError):
        new["last_post_ts"] = 0.0
    new["last_report"] = str(data.get("last_report", ""))
    state.clear()
    state.update(new)


async def remote_cmd(*cmd):
    if http_session is None:
        raise RuntimeError("HTTP-сессия не создана")
    async with http_session.post(
        UPSTASH_URL,
        json=list(cmd),
        headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
    ) as resp:
        data = await resp.json(content_type=None)
        error = data.get("error") if isinstance(data, dict) else None
        if resp.status != 200 or error:
            raise RuntimeError(f"Upstash: HTTP {resp.status} {error or ''}".strip())
        return data.get("result")


async def load_state():
    """True, если настройки успешно загружены (или хранилище пустое)."""
    global remote_loaded, storage_error
    if USE_REMOTE:
        try:
            raw = await remote_cmd("GET", STATE_KEY)
            apply_loaded(json.loads(raw) if raw else {})
            remote_loaded = True
            storage_error = ""
            return True
        except Exception as exc:
            storage_error = f"чтение: {exc}"
            log.error("Не удалось загрузить настройки из Upstash: %s", exc)
            return False
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as file:
            apply_loaded(json.load(file))
    except FileNotFoundError:
        log.info("Сохранённых настроек ещё нет")
    except (OSError, ValueError) as exc:
        storage_error = f"чтение файла: {exc}"
        log.error("Не удалось прочитать файл настроек: %s", exc)
    return True


async def ensure_loaded():
    if remote_loaded:
        return True
    return await load_state()


async def save_state():
    global storage_error
    async with save_lock:
        payload = json.dumps(state, ensure_ascii=False)
        try:
            if USE_REMOTE:
                if not remote_loaded:
                    # Не затираем сохранённые данные пустыми настройками.
                    raise RuntimeError("настройки ещё не загружены из хранилища")
                await remote_cmd("SET", STATE_KEY, payload)
            else:
                tmp = DATA_FILE + ".tmp"
                with open(tmp, "w", encoding="utf-8") as file:
                    file.write(payload)
                os.replace(tmp, DATA_FILE)
            storage_error = ""
            return True
        except Exception as exc:
            storage_error = f"запись: {exc}"
            log.exception("Не удалось сохранить настройки")
            return False


def save_note(ok):
    if ok:
        return ""
    return "\n⚠️ Не удалось сохранить настройки в хранилище, после перезапуска они могут пропасть. Подробности в /status."


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def fmt_duration(seconds):
    seconds = max(int(seconds), 0)
    if seconds < 60:
        return "меньше минуты"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} мин"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours} ч {minutes} мин"
    days, hours = divmod(hours, 24)
    return f"{days} дн {hours} ч"


def group_title(key):
    group = state["groups"].get(str(key))
    return group["title"] if group else str(key)


def next_due_ts():
    return state["last_post_ts"] + state["interval_min"] * 60


async def notify_owner(text):
    try:
        await bot.send_message(OWNER_ID, text)
    except Exception:
        log.exception("Не удалось отправить уведомление владельцу")


def describe_error(exc):
    if isinstance(exc, TelegramForbiddenError):
        return "бот удалён из группы, заблокирован или ему запретили писать"
    if isinstance(exc, TelegramRetryAfter):
        return f"лимит Telegram, повторить через {exc.retry_after} с"
    if isinstance(exc, TelegramBadRequest):
        return f"Telegram отклонил сообщение: {exc.message}"
    if isinstance(exc, TelegramAPIError):
        return f"ошибка Telegram: {exc.message}"
    return f"{type(exc).__name__}: {exc}"


async def guard(message, private_only=True):
    """Пускает только владельца. Остальных молча игнорирует."""
    user = message.from_user
    if user is None:
        return False
    if user.id == ANON_ADMIN_ID:
        await message.answer(
            "Не могу проверить, что это вы: сообщение отправлено анонимным администратором. "
            "Отключите анонимный режим и повторите команду."
        )
        return False
    if user.id != OWNER_ID:
        return False
    if private_only and message.chat.type != "private":
        await message.answer("Эта команда работает только в личном чате со мной.")
        return False
    if not await ensure_loaded():
        await message.answer(
            "⚠️ Хранилище настроек сейчас недоступно, команда не выполнена. "
            "Попробуйте через минуту."
        )
        return False
    return True


# ---------------------------------------------------------------------------
# Публикация
# ---------------------------------------------------------------------------

async def migrate_group(old_id, new_id):
    old_key, new_key = str(old_id), str(new_id)
    group = state["groups"].pop(old_key, None)
    if group is not None:
        state["groups"][new_key] = group
        log.info("Группа %s переехала на id %s", old_id, new_id)


async def send_one(chat_id, text):
    """Возвращает (статус, пояснение): ok / failed / removed."""
    key = str(chat_id)
    title = group_title(key)
    for attempt in range(3):
        try:
            await bot.send_message(chat_id, text)
            return "ok", ""
        except TelegramRetryAfter as exc:
            if attempt < 2 and exc.retry_after <= 60:
                await asyncio.sleep(exc.retry_after + 1)
                continue
            return "failed", describe_error(exc)
        except TelegramMigrateToChat as exc:
            await migrate_group(chat_id, exc.migrate_to_chat_id)
            chat_id = exc.migrate_to_chat_id
            key = str(chat_id)
            continue
        except TelegramForbiddenError as exc:
            state["groups"].pop(key, None)
            return "removed", describe_error(exc)
        except TelegramBadRequest as exc:
            if "chat not found" in (exc.message or "").lower():
                state["groups"].pop(key, None)
                return "removed", "группа не найдена (бот удалён или группа закрыта)"
            return "failed", describe_error(exc)
        except Exception as exc:
            log.exception("Ошибка отправки в группу %s (%s)", title, chat_id)
            return "failed", describe_error(exc)
    return "failed", "не удалось отправить после повторных попыток"


async def publish_to_groups(source, late_min=0):
    """Публикует текст во все группы. Возвращает словарь с итогами."""
    async with publish_lock:
        text = state["post_text"]
        keys = list(state["groups"])
        result = {"ok": 0, "total": len(keys), "failed": [], "removed": []}
        for index, key in enumerate(keys):
            title = group_title(key)
            status, detail = await send_one(int(key), text)
            if status == "ok":
                result["ok"] += 1
            elif status == "removed":
                result["removed"].append((title, detail))
            else:
                result["failed"].append((title, detail))
            if index < len(keys) - 1:
                await asyncio.sleep(PAUSE_BETWEEN_GROUPS)

        state["last_post_ts"] = time.time()
        report = f"{source}: {format_result(result)}"
        if late_min:
            report += f"\n⏰ Публикация с опозданием около {late_min} мин (сервис спал или перезапускался)."
        state["last_report"] = report[:1500]
        await save_state()
        return result


def format_result(result):
    lines = [f"Отправлено: {result['ok']} из {result['total']}"]
    for title, detail in result["failed"]:
        lines.append(f"❌ {title}: {detail}")
    for title, detail in result["removed"]:
        lines.append(
            f"🗑 {title}: {detail}. Группа убрана из списка; после исправления "
            "добавьте её снова командой /addhere."
        )
    return "\n".join(lines)


async def run_autopost():
    if not state["post_text"] or not state["groups"]:
        state["autopost_enabled"] = False
        await save_state()
        await notify_owner(
            "⏸ Автопубликация выключена: нет текста или нет групп. "
            "Задайте /setpost, добавьте группу через /addhere и включите /autopost_on."
        )
        return
    late = time.time() - next_due_ts()
    late_min = int(late // 60) if late > 300 else 0
    log.info("Плановая публикация начата")
    result = await publish_to_groups("авто", late_min)
    if result["failed"] or result["removed"]:
        await notify_owner("⚠️ Автопубликация завершилась с проблемами.\n" + format_result(result))
    if not state["groups"]:
        state["autopost_enabled"] = False
        await save_state()
        await notify_owner("⏸ Автопубликация выключена: в списке не осталось групп.")


async def scheduler():
    while True:
        try:
            await asyncio.sleep(CHECK_EVERY)
            if not await ensure_loaded():
                continue
            if not state["autopost_enabled"]:
                continue
            if time.time() < next_due_ts():
                continue
            await run_autopost()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Ошибка в планировщике, продолжаю работу")


# ---------------------------------------------------------------------------
# Команды
# ---------------------------------------------------------------------------

HELP_TEXT = (
    "/setpost Текст - задать текст публикации\n"
    "/showpost - показать текущий текст\n"
    "/addhere - добавить группу (отправить в самой группе)\n"
    "/groups - список групп\n"
    "/removegroup N - убрать группу (N из /groups)\n"
    "/publish - опубликовать сейчас\n"
    "/autopost_on - включить автопубликацию\n"
    "/autopost_off - выключить автопубликацию\n"
    f"/setinterval N - интервал в минутах ({MIN_INTERVAL_MIN}-{MAX_INTERVAL_MIN})\n"
    "/status - состояние бота"
)


@dp.message(Command("start"))
async def start(message: types.Message):
    if not await guard(message):
        return
    await message.answer("Spambot запущен.\n\n" + HELP_TEXT)


@dp.message(Command("help"))
async def help_command(message: types.Message):
    if not await guard(message):
        return
    await message.answer(HELP_TEXT)


@dp.message(Command("setpost"))
async def set_post(message: types.Message, command: CommandObject):
    if not await guard(message):
        return
    text = (command.args or "").strip()
    if not text:
        await message.answer("Использование: /setpost Текст публикации")
        return
    if len(text) > MAX_POST_LEN:
        await message.answer(
            f"Текст слишком длинный: {len(text)} символов, максимум {MAX_POST_LEN}."
        )
        return
    state["post_text"] = text
    ok = await save_state()
    await message.answer(f"Текст публикации сохранён ({len(text)} символов)." + save_note(ok))


@dp.message(Command("showpost"))
async def show_post(message: types.Message):
    if not await guard(message):
        return
    if not state["post_text"]:
        await message.answer("Текст пока не задан. Используйте /setpost.")
        return
    await message.answer("Текущий текст публикации:\n\n" + state["post_text"])


@dp.message(Command("addhere"))
async def add_group(message: types.Message):
    if not await guard(message, private_only=False):
        return
    if message.chat.type not in GROUP_TYPES:
        await message.answer("Отправьте /addhere непосредственно в группе, где размещение согласовано.")
        return
    key = str(message.chat.id)
    is_new = key not in state["groups"]
    state["groups"][key] = {"title": message.chat.title or key}
    ok = await save_state()
    text = "Группа добавлена." if is_new else "Эта группа уже в списке, название обновлено."
    await message.answer(
        text + " Убедитесь, что у бота есть право писать сообщения в этой группе." + save_note(ok)
    )


@dp.message(Command("groups"))
async def list_groups(message: types.Message):
    if not await guard(message):
        return
    if not state["groups"]:
        await message.answer("Групп пока нет. Добавьте группу командой /addhere в самой группе.")
        return
    lines = [f"Групп: {len(state['groups'])}"]
    for number, (key, group) in enumerate(state["groups"].items(), start=1):
        lines.append(f"{number}. {group['title']} ({key})")
    lines.append("\nУбрать группу: /removegroup номер")
    await message.answer("\n".join(lines))


@dp.message(Command("removegroup"))
async def remove_group_command(message: types.Message, command: CommandObject):
    if not await guard(message, private_only=False):
        return
    arg = (command.args or "").strip()
    keys = list(state["groups"])
    key = None
    if not arg:
        if message.chat.type in GROUP_TYPES and str(message.chat.id) in state["groups"]:
            key = str(message.chat.id)
        else:
            await message.answer("Использование: /removegroup номер (номер из /groups).")
            return
    else:
        try:
            number = int(arg)
        except ValueError:
            await message.answer("Использование: /removegroup номер (номер из /groups).")
            return
        if 1 <= number <= len(keys):
            key = keys[number - 1]
        elif str(number) in state["groups"]:
            key = str(number)
    if key is None:
        await message.answer("Такой группы нет в списке. Смотрите /groups.")
        return
    title = state["groups"].pop(key)["title"]
    ok = await save_state()
    await message.answer(f"Группа убрана из списка: {title}" + save_note(ok))


@dp.message(Command("publish"))
async def publish_now(message: types.Message):
    if not await guard(message):
        return
    if not state["post_text"]:
        await message.answer("Сначала задайте текст через /setpost.")
        return
    if not state["groups"]:
        await message.answer("Сначала добавьте группы через /addhere.")
        return
    await message.answer("Начинаю публикацию.")
    result = await publish_to_groups("вручную")
    await message.answer(format_result(result))


@dp.message(Command("autopost_on"))
async def autopost_on(message: types.Message):
    if not await guard(message):
        return
    if not state["post_text"] or not state["groups"]:
        await message.answer("Сначала задайте текст через /setpost и добавьте группы через /addhere.")
        return
    state["autopost_enabled"] = True
    state["last_post_ts"] = time.time()
    ok = await save_state()
    interval = state["interval_min"]
    await message.answer(
        f"Автопубликация включена. Интервал: {interval} мин, первая публикация примерно "
        f"через {interval} мин (сразу отправить можно через /publish).\n"
        "Работает, пока сервис не спит. На бесплатном Render нужен внешний пингер, "
        f"который открывает {BASE_URL}/ каждые 10 минут." + save_note(ok)
    )


@dp.message(Command("autopost_off"))
async def autopost_off(message: types.Message):
    if not await guard(message):
        return
    state["autopost_enabled"] = False
    ok = await save_state()
    await message.answer("Автопубликация выключена." + save_note(ok))


@dp.message(Command("setinterval"))
async def set_interval(message: types.Message, command: CommandObject):
    if not await guard(message):
        return
    arg = (command.args or "").strip()
    if not re.fullmatch(r"\d{1,5}", arg) or not (MIN_INTERVAL_MIN <= int(arg) <= MAX_INTERVAL_MIN):
        await message.answer(
            f"Использование: /setinterval минуты (от {MIN_INTERVAL_MIN} до {MAX_INTERVAL_MIN}). "
            f"Сейчас: {state['interval_min']} мин."
        )
        return
    state["interval_min"] = int(arg)
    ok = await save_state()
    text = f"Интервал: {state['interval_min']} мин."
    if state["autopost_enabled"]:
        wait = next_due_ts() - time.time()
        if wait > 0:
            text += f" Следующая публикация через {fmt_duration(wait)}."
        else:
            text += " Следующая публикация - при ближайшей проверке (до минуты)."
    await message.answer(text + save_note(ok))


def status_text():
    now = time.time()
    lines = ["🤖 Состояние бота", f"Сервис запущен: {fmt_duration(now - START_TS)} назад"]
    if USE_REMOTE:
        if remote_loaded:
            lines.append("Хранилище: Upstash (постоянное) ✅")
        else:
            lines.append("Хранилище: Upstash, ⚠️ настройки не загружены")
    else:
        lines.append(
            "Хранилище: ⚠️ временный файл, настройки пропадут при перезапуске Render "
            "(подключите Upstash)"
        )
    if storage_error:
        lines.append(f"Ошибка хранилища: {storage_error}")
    lines.append(f"Групп: {len(state['groups'])}")
    post = state["post_text"]
    lines.append(f"Текст: {'задан, ' + str(len(post)) + ' символов' if post else 'не задан'}")
    if state["autopost_enabled"]:
        lines.append(f"Автопубликация: включена, каждые {state['interval_min']} мин")
        wait = next_due_ts() - now
        if wait > 0:
            lines.append(f"Следующая: через {fmt_duration(wait)}")
        else:
            lines.append("Следующая: при ближайшей проверке (до минуты)")
    else:
        lines.append(f"Автопубликация: выключена (интервал {state['interval_min']} мин)")
    if state["last_post_ts"] and state["last_report"]:
        lines.append(f"Последняя публикация: {fmt_duration(now - state['last_post_ts'])} назад")
        lines.append(state["last_report"])
    else:
        lines.append("Публикаций ещё не было")
    lines.append(
        "\nБесплатный Render усыпляет сервис без внешних запросов; "
        "пока он спит, автопубликации нет."
    )
    return "\n".join(lines)


@dp.message(Command("status"))
async def status(message: types.Message):
    if not await guard(message):
        return
    await message.answer(status_text())


# ---------------------------------------------------------------------------
# События чатов: удаление бота, смена id группы
# ---------------------------------------------------------------------------

@dp.my_chat_member()
async def on_my_chat_member(event: types.ChatMemberUpdated):
    if event.chat.type not in GROUP_TYPES:
        return
    new_status = event.new_chat_member.status
    key = str(event.chat.id)
    title = event.chat.title or key

    if new_status in ("left", "kicked"):
        if not await ensure_loaded():
            await notify_owner(
                f"⚠️ Бота убрали из группы «{title}», но хранилище недоступно. "
                "Позже уберите её из списка командой /removegroup."
            )
            return
        if key in state["groups"]:
            state["groups"].pop(key, None)
            ok = await save_state()
            await notify_owner(
                f"🗑 Бота убрали из группы «{title}». Группа удалена из списка." + save_note(ok)
            )
    elif new_status == "restricted" and not getattr(event.new_chat_member, "can_send_messages", True):
        if await ensure_loaded() and key in state["groups"]:
            await notify_owner(
                f"⚠️ В группе «{title}» боту запретили писать сообщения. "
                "Публикация туда не пройдёт, пока права не вернут."
            )


@dp.message(F.migrate_to_chat_id)
async def on_group_migrated(message: types.Message):
    if not await ensure_loaded():
        return
    key = str(message.chat.id)
    if key in state["groups"]:
        await migrate_group(message.chat.id, message.migrate_to_chat_id)
        await save_state()
        await notify_owner(
            "ℹ️ Группа стала супергруппой, её адрес обновлён автоматически, публикации продолжатся."
        )


# ---------------------------------------------------------------------------
# Запуск
# ---------------------------------------------------------------------------

async def on_startup(bot: Bot):
    global http_session, scheduler_task
    http_session = ClientSession(timeout=ClientTimeout(total=10))

    for _ in range(3):
        if await load_state():
            break
        await asyncio.sleep(2)

    for attempt in range(1, 4):
        try:
            await bot.set_webhook(
                url=BASE_URL + WEBHOOK_PATH,
                secret_token=WEBHOOK_SECRET,
                allowed_updates=dp.resolve_used_update_types(),
            )
            break
        except Exception:
            log.exception("Не удалось установить webhook (попытка %s из 3)", attempt)
            if attempt == 3:
                raise
            await asyncio.sleep(3)

    scheduler_task = asyncio.create_task(scheduler())
    log.info("Бот запущен. Webhook: %s. Постоянное хранилище: %s", BASE_URL + WEBHOOK_PATH, USE_REMOTE)

    if USE_REMOTE and not remote_loaded:
        await notify_owner(
            "⚠️ Бот запустился, но не смог загрузить настройки из Upstash. "
            "Команды временно не выполняются, я буду пробовать снова. Детали: /status."
        )


async def on_shutdown(bot: Bot):
    # webhook НЕ удаляем: именно он будит спящий сервис на бесплатном Render.
    global scheduler_task
    if scheduler_task:
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass
    if http_session is not None:
        await http_session.close()
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

    async def index(request):
        return web.Response(text="Spambot is running")

    app.router.add_get("/", index)

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, host="0.0.0.0", port=port)
    await site.start()
    log.info("HTTP-сервер запущен на порту %s", port)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    await stop.wait()
    await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
