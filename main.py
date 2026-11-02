import asyncio
import json
import logging
import os
import random
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

try:
    from telethon import TelegramClient
    from telethon import utils as tl_utils
    from telethon.sessions import StringSession
    from telethon.tl.functions.messages import CheckChatInviteRequest
    from telethon.tl.types import ChatInviteAlready
except ImportError:  # telethon нужен только для публикации от аккаунта
    TelegramClient = tl_utils = StringSession = None
    CheckChatInviteRequest = ChatInviteAlready = None

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

# Публикация от отдельного пользовательского аккаунта (необязательно).
TG_API_ID = 0
TG_API_HASH = env_str("TG_API_HASH")
TG_SESSION = env_str("TG_SESSION")
ACCOUNT_MODE = False
_api_id_raw = env_str("TG_API_ID")
if _api_id_raw or TG_API_HASH or TG_SESSION:
    if not (_api_id_raw and TG_API_HASH and TG_SESSION):
        _problems.append(
            "Для публикации от аккаунта нужны все три переменные: "
            "TG_API_ID, TG_API_HASH, TG_SESSION."
        )
    elif not _api_id_raw.isdigit():
        _problems.append("TG_API_ID должен быть числом.")
    elif TelegramClient is None:
        _problems.append("Не установлена библиотека telethon (добавьте её в requirements.txt).")
    else:
        TG_API_ID = int(_api_id_raw)
        ACCOUNT_MODE = True

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
# От аккаунта: интервал от 10 минут и короткие случайные паузы между группами.
# При первом же лимите Telegram публикация останавливается (см. account_publish).
ACCOUNT_MIN_INTERVAL_MIN = 10
ACCOUNT_PAUSE = (5, 12)  # случайная пауза между группами, секунды
ACCOUNT_SAFE_INTERVAL_MIN = 60  # ниже этого значения бот предупреждает о риске
MAX_POST_LEN = 4096
PAUSE_BETWEEN_GROUPS = 2
CHECK_EVERY = 60
ANON_ADMIN_ID = 1087968824  # GroupAnonymousBot
GROUP_TYPES = ("group", "supergroup")

START_TS = time.time()


def min_interval():
    return ACCOUNT_MIN_INTERVAL_MIN if ACCOUNT_MODE else MIN_INTERVAL_MIN


def default_interval():
    return max(DEFAULT_INTERVAL_MIN, min_interval())


bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

state = {}
http_session = None
scheduler_task = None
remote_loaded = not USE_REMOTE
storage_error = ""
publish_lock = asyncio.Lock()
save_lock = asyncio.Lock()
account_lock = asyncio.Lock()
account_id = None


# ---------------------------------------------------------------------------
# Отдельный пользовательский аккаунт (необязательный режим)
# ---------------------------------------------------------------------------

class AccountSession:
    """Короткое подключение к аккаунту: открыли, сделали дело, закрыли.

    Так сессия не висит в Telegram постоянно, и перезапуск Render не мешает ей.
    """

    client = None

    async def __aenter__(self):
        await account_lock.acquire()
        try:
            self.client = TelegramClient(
                StringSession(TG_SESSION), TG_API_ID, TG_API_HASH, receive_updates=False
            )
            await self.client.connect()
            if not await self.client.is_user_authorized():
                raise RuntimeError(
                    "сессия аккаунта недействительна: создайте новую строку TG_SESSION"
                )
            return self.client
        except BaseException:
            await self._close()
            account_lock.release()
            raise

    async def _close(self):
        if self.client is not None:
            try:
                await self.client.disconnect()
            except Exception:
                log.exception("Не удалось закрыть подключение аккаунта")

    async def __aexit__(self, *exc_info):
        try:
            await self._close()
        finally:
            account_lock.release()
        return False


STOP_ERRORS = {"FloodWaitError", "PeerFloodError"}
AUTH_ERRORS = {
    "AuthKeyUnregisteredError", "AuthKeyDuplicatedError", "SessionRevokedError",
    "SessionExpiredError", "UserDeactivatedError", "UserDeactivatedBanError",
}
NO_FORWARD_ERRORS = {"ChatForwardsRestrictedError", "MessageIdInvalidError"}
NO_WRITE_ERRORS = {
    "ChatWriteForbiddenError", "UserBannedInChannelError", "ChatRestrictedError",
    "ChatAdminRequiredError", "ChannelBannedError",
}
GONE_ERRORS = {"ChannelPrivateError", "ChatIdInvalidError", "PeerIdInvalidError", "ChannelInvalidError"}


def classify_account_error(exc):
    """Возвращает (статус, пояснение): failed / removed / stop."""
    name = type(exc).__name__
    if name == "FloodWaitError":
        seconds = getattr(exc, "seconds", 0)
        return "stop", f"Telegram просит подождать {fmt_duration(seconds)} (лимит); публикация остановлена"
    if name == "PeerFloodError":
        return "stop", "Telegram ограничил аккаунт за рассылку; публикация остановлена, проверьте @SpamBot"
    if name in AUTH_ERRORS:
        return "stop", "аккаунт разлогинен или заблокирован; нужна новая строка TG_SESSION"
    if name == "SlowModeWaitError":
        return "failed", f"в группе включён медленный режим, подождать {fmt_duration(getattr(exc, 'seconds', 0))}"
    if name in NO_FORWARD_ERRORS:
        return "failed", "пересылка запрещена (защита от копирования) или пост недоступен; задайте /setpost заново"
    if name in NO_WRITE_ERRORS:
        return "failed", "аккаунту запрещено писать в этой группе"
    if name in GONE_ERRORS:
        return "removed", "аккаунт больше не состоит в этой группе"
    return "failed", f"{name}: {exc}"


async def get_account_id():
    global account_id
    if account_id is None:
        async with AccountSession() as client:
            me = await client.get_me()
            if getattr(me, "bot", False):
                raise RuntimeError("TG_SESSION принадлежит боту, нужен обычный аккаунт")
            account_id = me.id
    return account_id


async def account_publish(keys, result):
    """Публикация от аккаунта. Заполняет result; при лимитах останавливается."""
    try:
        async with AccountSession() as client:
            await client.get_dialogs()
            msg = None
            forward = False
            if state["post_source"]:
                source = state["ub_source"]
                if not source:
                    result["stopped"] = (
                        "пост не передан аккаунту: отправьте боту пост и снова ответьте на него /setpost"
                    )
                    return
                me = await bot.me()
                msg = await client.get_messages(me.username, ids=source["message_id"])
                if not msg:
                    result["stopped"] = (
                        "пост не найден в чате аккаунта с ботом (его удалили?); задайте /setpost заново"
                    )
                    return
                forward = True
            for index, key in enumerate(keys):
                title = group_title(key)
                try:
                    entity = await client.get_entity(int(key))
                    if forward:
                        await client.forward_messages(entity, msg)
                    else:
                        await client.send_message(entity, state["post_text"])
                    result["ok"] += 1
                except Exception as exc:
                    status, detail = classify_account_error(exc)
                    if status == "stop":
                        result["failed"].append((title, detail))
                        result["stopped"] = detail
                        result["not_tried"] = len(keys) - index - 1
                        return
                    if status == "removed":
                        state["groups"].pop(key, None)
                        result["removed"].append((title, detail))
                    else:
                        result["failed"].append((title, detail))
                if index < len(keys) - 1:
                    await asyncio.sleep(random.uniform(*ACCOUNT_PAUSE))
    except Exception as exc:
        log.exception("Ошибка публикации от аккаунта")
        status, detail = classify_account_error(exc)
        result["stopped"] = result["stopped"] or detail


async def account_resolve(client, kind, value):
    """Проверяет группу глазами аккаунта. Возвращает (статус, название, пояснение)."""
    label = f"@{value}" if kind == "user" else str(value)
    try:
        if kind == "private":
            invite = value.split("/")[-1].lstrip("+")
            info = await client(CheckChatInviteRequest(invite))
            if not isinstance(info, ChatInviteAlready):
                return "failed", label, "аккаунт не состоит в этой группе: вступите в неё вручную"
            entity = info.chat
        else:
            entity = await client.get_entity(f"@{value}" if kind == "user" else value)
        return await account_check_entity(client, entity, label)
    except Exception as exc:
        name = type(exc).__name__
        if name in ("UsernameNotOccupiedError", "UsernameInvalidError", "InviteHashInvalidError", "InviteHashExpiredError"):
            return "failed", label, "группа или ссылка не найдена"
        log.exception("Не удалось проверить группу %s", label)
        return "failed", label, f"{name}: {exc}"


async def account_check_entity(client, entity, label):
    """Проверяет, что аккаунт состоит в группе и может писать; добавляет её в список."""
    if type(entity).__name__ not in ("Channel", "Chat"):
        return "failed", label, "это не группа и не канал"
    perms = await client.get_permissions(entity)
    if perms is None:
        return "failed", label, "аккаунт не состоит в этой группе: вступите в неё вручную"
    if not perms.send_messages:
        return "failed", label, "аккаунту запрещено писать в этой группе"
    key = str(tl_utils.get_peer_id(entity))
    title = getattr(entity, "title", None) or label
    if key in state["groups"]:
        state["groups"][key]["title"] = title
        return "exists", title, ""
    state["groups"][key] = {"title": title}
    return "added", title, ""


def folder_title(folder):
    title = getattr(folder, "title", None)
    return str(getattr(title, "text", title) or "").strip()


async def account_folders(client):
    """Папки чатов аккаунта: список (название, include_peers)."""
    from telethon.tl.functions.messages import GetDialogFiltersRequest

    res = await client(GetDialogFiltersRequest())
    filters = getattr(res, "filters", res)
    folders = []
    for item in filters:
        name = folder_title(item)
        peers = list(getattr(item, "include_peers", None) or [])
        if name and peers:
            folders.append((name, peers))
    return folders


# ---------------------------------------------------------------------------
# Состояние и хранение
# ---------------------------------------------------------------------------

def default_state():
    return {
        "groups": {},  # "chat_id" -> {"title": "..."}
        "post_text": "",
        "post_source": None,  # {"chat_id": ..., "message_id": ...} - сообщение для пересылки
        "ub_source": None,  # {"message_id": ...} - копия поста в чате аккаунта с ботом
        "autopost_enabled": False,
        "interval_min": default_interval(),
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
    source = data.get("post_source")
    if isinstance(source, dict):
        try:
            new["post_source"] = {
                "chat_id": int(source["chat_id"]),
                "message_id": int(source["message_id"]),
            }
        except (KeyError, TypeError, ValueError):
            new["post_source"] = None
    ub_source = data.get("ub_source")
    if isinstance(ub_source, dict):
        try:
            new["ub_source"] = {"message_id": int(ub_source["message_id"])}
        except (KeyError, TypeError, ValueError):
            new["ub_source"] = None
    new["autopost_enabled"] = bool(data.get("autopost_enabled", False))
    try:
        interval = int(data.get("interval_min", default_interval()))
    except (TypeError, ValueError):
        interval = default_interval()
    new["interval_min"] = min(max(interval, min_interval()), MAX_INTERVAL_MIN)
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


def has_post():
    return bool(state["post_source"] or state["post_text"])


def post_description():
    if state["post_source"]:
        return "пересылаемое сообщение"
    if state["post_text"]:
        return f"текст, {len(state['post_text'])} символов"
    return "не задан"


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


async def deliver(chat_id):
    """Пересылает сохранённое сообщение или отправляет сохранённый текст."""
    source = state["post_source"]
    if source:
        await bot.forward_message(
            chat_id=chat_id,
            from_chat_id=source["chat_id"],
            message_id=source["message_id"],
        )
    else:
        await bot.send_message(chat_id, state["post_text"])


async def send_one(chat_id):
    """Возвращает (статус, пояснение): ok / failed / removed."""
    key = str(chat_id)
    title = group_title(key)
    for attempt in range(3):
        try:
            await deliver(chat_id)
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
            lowered = (exc.message or "").lower()
            if "message to forward not found" in lowered or "can't be forwarded" in lowered:
                return "failed", (
                    "исходный пост удалён или его нельзя пересылать - "
                    "задайте пост заново через /setpost"
                )
            if "chat not found" in lowered:
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
        keys = list(state["groups"])
        result = {"ok": 0, "total": len(keys), "failed": [], "removed": [], "stopped": "", "not_tried": 0}
        if ACCOUNT_MODE:
            await account_publish(keys, result)
        else:
            for index, key in enumerate(keys):
                title = group_title(key)
                status, detail = await send_one(int(key))
                if status == "ok":
                    result["ok"] += 1
                elif status == "removed":
                    result["removed"].append((title, detail))
                else:
                    result["failed"].append((title, detail))
                if index < len(keys) - 1:
                    await asyncio.sleep(PAUSE_BETWEEN_GROUPS)

        if result["stopped"]:
            state["autopost_enabled"] = False
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
    if result.get("stopped"):
        text = f"⛔ Публикация остановлена: {result['stopped']}. Автопубликация выключена."
        if result.get("not_tried"):
            text += f" Не отправлено в группы: {result['not_tried']}."
        lines.append(text)
    return "\n".join(lines)


async def run_autopost():
    if not has_post() or not state["groups"]:
        state["autopost_enabled"] = False
        await save_state()
        await notify_owner(
            "⏸ Автопубликация выключена: нет поста или нет групп. "
            "Задайте /setpost, добавьте группу через /addhere и включите /autopost_on."
        )
        return
    late = time.time() - next_due_ts()
    late_min = int(late // 60) if late > 300 else 0
    log.info("Плановая публикация начата")
    result = await publish_to_groups("авто", late_min)
    if result["failed"] or result["removed"] or result["stopped"]:
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
    "/setpost - ответьте этой командой на готовое сообщение, и я буду пересылать его в группы\n"
    "/setpost Текст - или задайте обычный текст\n"
    "/showpost - показать текущий пост\n"
    "Группы добавляются сами, когда вы добавляете бота в группу. Ещё способы:\n"
    "/addhere - добавить группу (отправить в самой группе)\n"
    "/addgroups список - проверить и добавить сразу много групп (@имена и ссылки t.me)\n"
    "/addfolder Название - (режим аккаунта) добавить группы из папки чатов аккаунта\n"
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
    reply = message.reply_to_message
    if reply is not None and not text:
        ub_source = None
        if ACCOUNT_MODE:
            # Бот пересылает пост аккаунту, а аккаунт пересылает его дальше в группы.
            try:
                target = await get_account_id()
                forwarded = await bot.forward_message(
                    chat_id=target, from_chat_id=reply.chat.id, message_id=reply.message_id
                )
                ub_source = {"message_id": forwarded.message_id}
            except TelegramForbiddenError:
                await message.answer(
                    "Аккаунт ещё не открывал чат с ботом. Зайдите с аккаунта в Telegram, "
                    "найдите этого бота, нажмите Start и повторите /setpost."
                )
                return
            except Exception as exc:
                log.exception("Не удалось передать пост аккаунту")
                await message.answer(f"Не удалось передать пост аккаунту: {exc}")
                return
        state["post_source"] = {"chat_id": reply.chat.id, "message_id": reply.message_id}
        state["ub_source"] = ub_source
        state["post_text"] = ""
        ok = await save_state()
        await message.answer(
            "Пост сохранён: буду пересылать именно это сообщение. "
            "Не удаляйте его из этого чата. Проверить: /showpost." + save_note(ok)
        )
        return
    if not text:
        await message.answer(
            "Как задать пост:\n"
            "1) отправьте мне готовое сообщение (или перешлите пост из канала) "
            "и ответьте на него командой /setpost;\n"
            "2) или обычным текстом: /setpost Текст публикации"
        )
        return
    if len(text) > MAX_POST_LEN:
        await message.answer(
            f"Текст слишком длинный: {len(text)} символов, максимум {MAX_POST_LEN}."
        )
        return
    state["post_text"] = text
    state["post_source"] = None
    state["ub_source"] = None
    ok = await save_state()
    await message.answer(f"Текст публикации сохранён ({len(text)} символов)." + save_note(ok))


@dp.message(Command("showpost"))
async def show_post(message: types.Message):
    if not await guard(message):
        return
    source = state["post_source"]
    if source:
        try:
            await bot.forward_message(
                chat_id=message.chat.id,
                from_chat_id=source["chat_id"],
                message_id=source["message_id"],
            )
        except Exception:
            log.exception("Не удалось показать сохранённый пост")
            await message.answer(
                "Не получилось показать пост: исходное сообщение, похоже, удалено. "
                "Задайте пост заново через /setpost."
            )
        return
    if not state["post_text"]:
        await message.answer("Пост пока не задан. Используйте /setpost.")
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
    if not has_post():
        await message.answer("Сначала задайте пост через /setpost.")
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
    if not has_post() or not state["groups"]:
        await message.answer("Сначала задайте пост через /setpost и добавьте группы через /addhere.")
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
    if not re.fullmatch(r"\d{1,5}", arg) or not (min_interval() <= int(arg) <= MAX_INTERVAL_MIN):
        extra = ""
        await message.answer(
            f"Использование: /setinterval минуты (от {min_interval()} до {MAX_INTERVAL_MIN}). "
            f"Сейчас: {state['interval_min']} мин." + extra
        )
        return
    state["interval_min"] = int(arg)
    ok = await save_state()
    text = f"Интервал: {state['interval_min']} мин."
    if ACCOUNT_MODE and state["interval_min"] < ACCOUNT_SAFE_INTERVAL_MIN:
        text += (
            "\n⚠️ Такой частый повтор одного сообщения повышает риск, что Telegram ограничит или "
            "заблокирует аккаунт. Если придёт лимит, бот остановит публикацию и сообщит вам."
        )
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
    if ACCOUNT_MODE:
        lines.append(
            f"Режим: пересылка от аккаунта (интервал не меньше {min_interval()} мин, "
            "паузы между группами, остановка при лимитах Telegram)"
        )
    else:
        lines.append("Режим: публикация от имени бота")
    lines.append(f"Групп: {len(state['groups'])}")
    lines.append(f"Пост: {post_description()}")
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


MAX_BULK = 30

REF_RE = re.compile(
    r"(?:https?://)?(?:t|telegram)\.me/(\+[A-Za-z0-9_-]+|joinchat/[A-Za-z0-9_-]+|[A-Za-z][A-Za-z0-9_]{3,31})"
    r"|@([A-Za-z][A-Za-z0-9_]{3,31})"
    r"|(?<![0-9])(-100[0-9]{5,})"
)


def parse_group_refs(text):
    """Достаёт из текста ссылки t.me, @имена и числовые id. Без повторов."""
    refs = []
    seen = set()
    for found in REF_RE.finditer(text or ""):
        link, username, chat_id = found.groups()
        if link:
            if link.startswith("+") or link.startswith("joinchat/"):
                ref = ("private", link)
            else:
                ref = ("user", link)
        elif username:
            ref = ("user", username)
        else:
            ref = ("id", int(chat_id))
        marker = (ref[0], str(ref[1]).lower())
        if marker not in seen:
            seen.add(marker)
            refs.append(ref)
    return refs


NOT_THERE = "бота в этой группе нет - добавьте его в группу вручную"


async def resolve_group(kind, value):
    """Проверяет группу и добавляет в список. Возвращает (статус, название, пояснение)."""
    target = f"@{value}" if kind == "user" else value
    label = target if kind == "user" else str(value)
    try:
        chat = await bot.get_chat(target)
        if chat.type not in GROUP_TYPES:
            return "failed", label, "это не группа (канал или личный чат)"
        member = await bot.get_chat_member(chat.id, bot.id)
    except TelegramForbiddenError:
        return "failed", label, NOT_THERE
    except TelegramBadRequest as exc:
        if "not found" in (exc.message or "").lower():
            return "failed", label, "группа не найдена, закрытая, или бота в ней нет - добавьте бота вручную"
        return "failed", label, f"ошибка Telegram: {exc.message}"
    except Exception as exc:
        log.exception("Не удалось проверить группу %s", label)
        return "failed", label, describe_error(exc)

    if member.status in ("left", "kicked"):
        return "failed", label, NOT_THERE
    if member.status == "restricted" and not getattr(member, "can_send_messages", True):
        return "failed", label, "боту запрещено писать в этой группе"
    key = str(chat.id)
    title = chat.title or label
    if key in state["groups"]:
        state["groups"][key]["title"] = title
        return "exists", title, ""
    state["groups"][key] = {"title": title}
    return "added", title, ""


@dp.message(Command("addgroups"))
async def add_groups(message: types.Message, command: CommandObject):
    if not await guard(message):
        return
    refs = parse_group_refs(command.args or "")
    if not refs:
        await message.answer(
            "Пришлите список после команды, например:\n"
            "/addgroups @group_one https://t.me/group_two\n"
            "Подойдут ссылки t.me, @имена публичных групп и номера вида -100...\n"
            "Бот уже должен быть в этих группах: сам вступить он не может."
        )
        return
    skipped = max(len(refs) - MAX_BULK, 0)
    refs = refs[:MAX_BULK]
    await message.answer(f"Проверяю групп: {len(refs)}.")
    added, existing, failed = [], [], []

    async def run_all(resolve):
        for kind, value in refs:
            if kind == "private" and not ACCOUNT_MODE:
                failed.append((
                    value,
                    "закрытая ссылка-приглашение: бот не может вступить сам. Добавьте бота в группу "
                    "вручную, она зарегистрируется автоматически",
                ))
                continue
            status, title, detail = await resolve(kind, value)
            if status == "added":
                added.append(title)
            elif status == "exists":
                existing.append(title)
            else:
                failed.append((title, detail))
            await asyncio.sleep(0.3)

    if ACCOUNT_MODE:
        try:
            async with AccountSession() as client:
                await client.get_dialogs()

                async def resolver(kind, value):
                    return await account_resolve(client, kind, value)

                await run_all(resolver)
        except Exception as exc:
            log.exception("Ошибка проверки групп через аккаунт")
            failed.append(("аккаунт", classify_account_error(exc)[1]))
    else:
        await run_all(resolve_group)
    ok = await save_state()

    lines = []
    if added:
        lines.append("✅ Добавлено: " + ", ".join(added))
    if existing:
        lines.append("➖ Уже в списке: " + ", ".join(existing))
    for title, detail in failed:
        lines.append(f"❌ {title}: {detail}")
    if skipped:
        lines.append(f"Пропущено из-за лимита {MAX_BULK} за раз: {skipped}. Отправьте их отдельной командой.")
    await message.answer("\n".join(lines) + save_note(ok))


@dp.message(Command("addfolder"))
async def add_folder(message: types.Message, command: CommandObject):
    if not await guard(message):
        return
    if not ACCOUNT_MODE:
        await message.answer("Импорт папки работает только в режиме публикации от аккаунта.")
        return
    wanted = (command.args or "").strip().lower()
    added, existing, failed = [], [], []
    try:
        async with AccountSession() as client:
            await client.get_dialogs()
            folders = await account_folders(client)
            if not wanted:
                names = ", ".join(f"«{name}» ({len(peers)})" for name, peers in folders) or "папок нет"
                await message.answer(
                    "Папки аккаунта: " + names + "\nИмпорт: /addfolder Название папки"
                )
                return
            match = [item for item in folders if item[0].lower() == wanted]
            if not match:
                await message.answer("Папка с таким названием не найдена. Список: /addfolder")
                return
            peers = match[0][1][:MAX_BULK]
            skipped = len(match[0][1]) - len(peers)
            for peer in peers:
                try:
                    entity = await client.get_entity(peer)
                except Exception as exc:
                    failed.append((str(peer), classify_account_error(exc)[1]))
                    continue
                label = getattr(entity, "title", None) or str(peer)
                status, title, detail = await account_check_entity(client, entity, label)
                if status == "added":
                    added.append(title)
                elif status == "exists":
                    existing.append(title)
                else:
                    failed.append((title, detail))
                await asyncio.sleep(0.3)
    except Exception as exc:
        log.exception("Ошибка импорта папки")
        await message.answer("Не удалось прочитать папки аккаунта: " + classify_account_error(exc)[1])
        return
    ok = await save_state()
    lines = []
    if added:
        lines.append("✅ Добавлено: " + ", ".join(added))
    if existing:
        lines.append("➖ Уже в списке: " + ", ".join(existing))
    for title, detail in failed:
        lines.append(f"❌ {title}: {detail}")
    if skipped:
        lines.append(f"Пропущено из-за лимита {MAX_BULK} за раз: {skipped}.")
    await message.answer("\n".join(lines or ["В папке нет подходящих групп."]) + save_note(ok))


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
    elif new_status in ("member", "administrator") and event.from_user.id == OWNER_ID:
        # Бота в группу добавили вы - регистрируем группу автоматически.
        if not await ensure_loaded():
            await notify_owner(
                f"⚠️ Вы добавили бота в группу «{title}», но хранилище недоступно. "
                "Позже отправьте в этой группе /addhere."
            )
            return
        if key not in state["groups"]:
            state["groups"][key] = {"title": title}
            ok = await save_state()
            await notify_owner(
                f"✅ Группа «{title}» добавлена в список автоматически (бота добавили вы). "
                "Убедитесь, что у бота есть право писать сообщения." + save_note(ok)
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
