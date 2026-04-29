#!/usr/bin/env python3
"""
Telegram Bot - Умная база данных контента
Автор: Professional Bot Developer
Версия: 2.0.0

Бот позволяет хранить любой контент (текст, фото, видео, документы, аудио)
в структурированных JSON базах данных.
"""

import asyncio
import html
import json
import logging
import os
import re
import tempfile
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Union
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

import psycopg2
import psycopg2.extras

try:
    from groq import Groq
except Exception:  # pragma: no cover
    Groq = None

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    FSInputFile,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# ─────────────────────────────────────────────────────────
# КОНФИГУРАЦИЯ
# ─────────────────────────────────────────────────────────

def _load_dotenv_from_project_root() -> None:
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    except OSError:
        return


_load_dotenv_from_project_root()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Не найден BOT_TOKEN. Добавьте его в переменные окружения или в файл .env рядом с base.py.")

BASE_DIR = Path("base")            # Папка для хранения баз данных

def _parse_int_list(value: str) -> list[int]:
    parts = re.split(r"[,\s;]+", (value or "").strip())
    out: list[int] = []
    for p in parts:
        if not p:
            continue
        try:
            out.append(int(p))
        except ValueError:
            continue
    return out


ALLOWED_USER_IDS: list[int] = _parse_int_list(os.getenv("ALLOWED_USER_IDS", ""))

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_SUPABASE = bool(DATABASE_URL)
SUPABASE_TABLE = (os.getenv("SUPABASE_TABLE", "content_bases") or "content_bases").strip()

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()  # например: https://your-service.onrender.com
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook").strip() or "/webhook"
WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN", "").strip()
WEBAPP_HOST = os.getenv("WEBAPP_HOST", "0.0.0.0").strip() or "0.0.0.0"
WEBAPP_PORT = int(os.getenv("PORT", os.getenv("WEBAPP_PORT", "10000")))

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
GROQ_BASE_URL = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1")
SAY_MAX_ITEMS = int(os.getenv("SAY_MAX_ITEMS", "600"))
GROQ_REASONING_EFFORT = os.getenv("GROQ_REASONING_EFFORT", "medium")


def _groq_sdk_base_url(raw_base_url: str) -> str:
    """
    Groq SDK сам добавляет префикс /openai/v1, поэтому base_url должен быть без него.
    Принимаем оба варианта (https://api.groq.com и https://api.groq.com/openai/v1).
    """
    base = (raw_base_url or "").rstrip("/")
    suffix = "/openai/v1"
    if base.endswith(suffix):
        base = base[: -len(suffix)]
    return base


def _groq_http_base_url(raw_base_url: str) -> str:
    """
    Для ручного HTTP-вызова нам нужен base_url с /openai/v1.
    Принимаем оба варианта (https://api.groq.com и https://api.groq.com/openai/v1).
    """
    base = (raw_base_url or "").rstrip("/")
    suffix = "/openai/v1"
    if not base.endswith(suffix):
        base = base + suffix
    return base


def _normalize_database_url(raw_url: str) -> str:
    url = (raw_url or "").strip()
    if not url:
        return ""
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.setdefault("sslmode", "require")
    return urlunparse(parsed._replace(query=urlencode(query)))


def _pg_connect():
    dsn = _normalize_database_url(DATABASE_URL)
    return psycopg2.connect(dsn)


def _supabase_ensure_schema() -> None:
    if not USE_SUPABASE:
        return
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SUPABASE_TABLE} (
                    name text PRIMARY KEY,
                    data jsonb NOT NULL,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    updated_at timestamptz NOT NULL DEFAULT now()
                );
                """
            )
            conn.commit()


def _supabase_healthcheck() -> bool:
    if not USE_SUPABASE:
        return False
    try:
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return True
    except Exception as e:
        logger.error(f"Supabase healthcheck failed: {e}")
        return False

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Текст кнопки завершения сессии добавления
FINISH_ADD_SESSION_TEXT = "✅ Закончить"
BACKUP_ALL_BUTTON_TEXT = "📦 Спустить всё"

# ─────────────────────────────────────────────────────────
# ИНИЦИАЛИЗАЦИЯ
# ─────────────────────────────────────────────────────────

BASE_DIR.mkdir(exist_ok=True)  # Автоматически создаём папку base/

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# ─────────────────────────────────────────────────────────
# СОСТОЯНИЯ FSM (Finite State Machine)
# ─────────────────────────────────────────────────────────

class AddRecord(StatesGroup):
    waiting_db_name       = State()  # Ожидание имени базы данных
    waiting_content       = State()  # Ожидание контента для добавления
    waiting_more_content  = State()  # Ожидание дополнительного контента (медиагруппа)
    confirming_save       = State()  # Подтверждение сохранения


class OpenBase(StatesGroup):
    waiting_db_name    = State()  # Ожидание имени базы для открытия
    browsing           = State()  # Просмотр записей
    waiting_page       = State()  # Ввод номера страницы


class SearchRecords(StatesGroup):
    waiting_db_name = State()  # Выбор базы для поиска
    waiting_query   = State()  # Ввод поискового запроса


class SayAI(StatesGroup):
    waiting_query = State()  # Ввод запроса для /say


class AskAI(StatesGroup):
    waiting_message = State()  # Диалог с ИИ (/ask)
    confirming = State()       # Подтверждение действий (удаление)


class DeleteRecord(StatesGroup):
    waiting_db_name    = State()  # Выбор базы
    waiting_record_id  = State()  # Ввод ID записи для удаления
    confirming         = State()  # Подтверждение удаления


class DeleteDatabase(StatesGroup):
    waiting_db_name = State()  # Выбор базы
    confirming = State()       # Подтверждение удаления файла базы


class EditRecord(StatesGroup):
    waiting_db_name    = State()  # Выбор базы
    waiting_record_id  = State()  # Ввод ID записи
    waiting_new_text   = State()  # Ввод нового текста


class GetMedia(StatesGroup):
    waiting_db_name    = State()  # Выбор базы
    waiting_record_id  = State()  # Ввод ID записи


# ─────────────────────────────────────────────────────────
# РАБОТА С JSON БАЗАМИ ДАННЫХ
# ─────────────────────────────────────────────────────────

def get_db_path(db_name: str) -> Path:
    """Возвращает путь к файлу базы данных."""
    key = normalize_db_key(db_name)
    return BASE_DIR / f"{key}.json"


def normalize_db_key(db_name: str) -> str:
    """Нормализует имя базы для файлов/ключей в БД (без .json)."""
    safe = re.sub(r"[^\w\-.]", "_", (db_name or "").strip())
    if safe.lower().endswith(".json"):
        safe = safe[:-5]
    return safe or "db"


def load_db(db_name: str) -> dict:
    """Загружает базу данных из JSON файла."""
    key = normalize_db_key(db_name)

    if USE_SUPABASE:
        try:
            with _pg_connect() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(f"SELECT data FROM {SUPABASE_TABLE} WHERE name=%s", (key,))
                    row = cur.fetchone()
                    if not row:
                        return {"name": key, "created_at": now(), "records": [], "next_id": 1}
                    data = row["data"]
                    if isinstance(data, str):
                        return json.loads(data)
                    return data
        except Exception as e:
            logger.error(f"Ошибка загрузки БД (Supabase) {key}: {e}")
            return {"name": key, "created_at": now(), "records": [], "next_id": 1}

    path = get_db_path(key)
    if not path.exists():
        return {"name": key, "created_at": now(), "records": [], "next_id": 1}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Ошибка загрузки БД {key}: {e}")
        return {"name": key, "created_at": now(), "records": [], "next_id": 1}


def save_db(db_name: str, data: dict) -> bool:
    """Сохраняет базу данных в JSON файл."""
    key = normalize_db_key(db_name)

    if USE_SUPABASE:
        try:
            payload = json.dumps(data, ensure_ascii=False)
            with _pg_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {SUPABASE_TABLE} (name, data, updated_at)
                        VALUES (%s, %s::jsonb, now())
                        ON CONFLICT (name) DO UPDATE SET
                            data = EXCLUDED.data,
                            updated_at = now()
                        """,
                        (key, payload),
                    )
                    conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения БД (Supabase) {key}: {e}")
            return False

    path = get_db_path(key)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except OSError as e:
        logger.error(f"Ошибка сохранения БД {key}: {e}")
        return False


def get_all_databases() -> list[dict]:
    """Возвращает список всех баз данных с метаинформацией."""
    if USE_SUPABASE:
        try:
            with _pg_connect() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        f"""
                        SELECT
                            name,
                            COALESCE(jsonb_array_length(data->'records'), 0) AS records_count,
                            pg_column_size(data) AS size,
                            COALESCE(data->>'created_at', '') AS created_at
                        FROM {SUPABASE_TABLE}
                        ORDER BY name
                        """
                    )
                    rows = cur.fetchall() or []
            return [
                {
                    "file": f"{r['name']}.json",
                    "name": r["name"],
                    "records_count": int(r["records_count"] or 0),
                    "created_at": r.get("created_at") or "Неизвестно",
                    "size": int(r["size"] or 0),
                }
                for r in rows
            ]
        except Exception as e:
            logger.error(f"Ошибка получения списка баз (Supabase): {e}")
            return []

    databases = []
    for json_file in sorted(BASE_DIR.glob("*.json")):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            databases.append(
                {
                    "file": json_file.name,
                    "name": data.get("name", json_file.stem),
                    "records_count": len(data.get("records", [])),
                    "created_at": data.get("created_at", "Неизвестно"),
                    "size": json_file.stat().st_size,
                }
            )
        except Exception:
            databases.append(
                {
                    "file": json_file.name,
                    "name": json_file.stem,
                    "records_count": 0,
                    "created_at": "Неизвестно",
                    "size": json_file.stat().st_size,
                }
            )
    return databases


def load_all_dbs_records() -> list[dict]:
    """
    Загружает все базы из папки BASE_DIR и возвращает плоский список:
    [{"db_name": str, "record": dict}, ...]
    """
    out: list[dict] = []

    if USE_SUPABASE:
        try:
            with _pg_connect() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(f"SELECT name, data FROM {SUPABASE_TABLE} ORDER BY name")
                    rows = cur.fetchall() or []
            for row in rows:
                name = row["name"]
                data = row["data"]
                if isinstance(data, str):
                    data = json.loads(data)
                for record in (data.get("records", []) or []):
                    out.append({"db_name": name, "record": record})
            return out
        except Exception as e:
            logger.error(f"Ошибка чтения баз (Supabase): {e}")
            return []

    for json_file in sorted(BASE_DIR.glob("*.json")):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            db_name = data.get("name", json_file.stem)
            for record in data.get("records", []) or []:
                out.append({"db_name": db_name, "record": record})
        except Exception:
            continue
    return out


def _build_bases_zip() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_tmp = Path(tempfile.gettempdir()) / f"bases_backup_{timestamp}.zip"

    with tempfile.TemporaryDirectory() as td:
        workdir = Path(td) / "base"
        workdir.mkdir(parents=True, exist_ok=True)

        if USE_SUPABASE:
            for db in get_all_databases():
                key = (db.get("file") or "").replace(".json", "")
                if not key:
                    continue
                payload = json.dumps(load_db(key), ensure_ascii=False, indent=2)
                (workdir / f"{key}.json").write_text(payload, encoding="utf-8")
        else:
            BASE_DIR.mkdir(parents=True, exist_ok=True)
            for json_file in BASE_DIR.glob("*.json"):
                try:
                    shutil.copy2(json_file, workdir / json_file.name)
                except OSError:
                    continue

        with zipfile.ZipFile(zip_tmp, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for file in sorted(workdir.glob("*.json")):
                zf.write(file, arcname=f"base/{file.name}")

    return zip_tmp


def _get_source_file_path(name: str) -> Optional[Path]:
    safe = (name or "").strip().lower()
    allowed = {
        "base.py": Path(__file__).resolve(),
        "bot.py": (Path(__file__).resolve().parent / "bot.py"),
    }
    return allowed.get(safe)


async def send_local_file(message: Message, path: Path, caption: str) -> None:
    await message.answer_document(FSInputFile(str(path)), caption=caption)


def now() -> str:
    """Возвращает текущее время в читаемом формате."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_zip_name(value: str) -> str:
    safe = re.sub(r"[^\w\-.]+", "_", (value or "").strip())
    return safe or "item"


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    for i in range(2, 10_000):
        candidate = parent / f"{stem}_{i}{suffix}"
        if not candidate.exists():
            return candidate
    return parent / f"{stem}_{datetime.now().strftime('%H%M%S')}{suffix}"


def _record_meta_text(db_name: str, record: dict) -> str:
    meta = record.get("metadata", {}) or {}
    lines = [
        f"db: {db_name}",
        f"id: {record.get('id')}",
        f"type: {record.get('type')}",
        f"timestamp: {record.get('timestamp')}",
    ]
    text_value = (record.get("text") or "").strip()
    if text_value:
        lines.append("")
        lines.append("text:")
        lines.append(text_value)
    if meta:
        lines.append("")
        lines.append("metadata:")
        try:
            lines.append(json.dumps(meta, ensure_ascii=False, indent=2))
        except Exception:
            lines.append(str(meta))
    return "\n".join(lines).strip() + "\n"


def _guess_extension(record: dict, tg_file_path: Optional[str]) -> str:
    meta = record.get("metadata", {}) or {}
    name = meta.get("file_name")
    if isinstance(name, str) and "." in name:
        ext = Path(name).suffix
        if ext:
            return ext
    if tg_file_path:
        ext = Path(str(tg_file_path)).suffix
        if ext:
            return ext
    rtype = (record.get("type") or "").lower()
    return {
        "photo": ".jpg",
        "video": ".mp4",
        "video_note": ".mp4",
        "voice": ".ogg",
        "audio": ".mp3",
        "document": ".bin",
        "sticker": ".webp",
    }.get(rtype, ".bin")


async def _build_bases_files_zip(bot: Bot) -> Path:
    """
    Собирает ZIP со всем содержимым баз:
    - для текстовых записей создаёт .txt
    - для медиа/файлов скачивает реальные файлы из Telegram
    - для каждой записи добавляет companion meta .txt
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_tmp = Path(tempfile.gettempdir()) / f"bases_content_{timestamp}.zip"

    all_items = load_all_dbs_records()

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "base"
        root.mkdir(parents=True, exist_ok=True)

        errors: list[str] = []
        for item in all_items:
            raw_db_name = str(item.get("db_name", "") or "")
            db_dir = root / _safe_zip_name(raw_db_name)
            db_dir.mkdir(parents=True, exist_ok=True)

            record = item.get("record") or {}
            rid = record.get("id")
            rtype = str(record.get("type", "unknown") or "unknown")
            rid_str = str(rid) if rid is not None else "noid"

            base_name = f"record_{rid_str}_{_safe_zip_name(rtype)}"

            # Meta info (always)
            meta_path = _unique_path(db_dir / f"{base_name}_meta.txt")
            try:
                meta_path.write_text(_record_meta_text(raw_db_name, record), encoding="utf-8")
            except OSError as e:
                errors.append(f"{raw_db_name}:#{rid_str} meta write failed: {e}")

            # Text-only record
            if rtype == "text":
                text_path = _unique_path(db_dir / f"{base_name}.txt")
                try:
                    text_value = (record.get("text") or "").rstrip() + "\n"
                    text_path.write_text(text_value, encoding="utf-8")
                except OSError as e:
                    errors.append(f"{raw_db_name}:#{rid_str} text write failed: {e}")
                continue

            file_id = record.get("file_id")
            if not file_id:
                continue

            try:
                tg_file = await bot.get_file(str(file_id))
                tg_path = getattr(tg_file, "file_path", None)
                ext = _guess_extension(record, tg_path)
                file_path = _unique_path(db_dir / f"{base_name}{ext}")
                await bot.download_file(tg_path, destination=str(file_path))
            except Exception as e:
                errors.append(f"{raw_db_name}:#{rid_str} download failed: {e}")

        if errors:
            (root / "_errors.txt").write_text("\n".join(errors) + "\n", encoding="utf-8")

        with zipfile.ZipFile(zip_tmp, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for file in sorted(root.rglob("*")):
                if file.is_file():
                    zf.write(file, arcname=str(file.relative_to(Path(td))))

    return zip_tmp


def db_exists(db_name: str) -> bool:
    """Проверяет существование базы данных."""
    key = normalize_db_key(db_name)
    if USE_SUPABASE:
        try:
            with _pg_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT 1 FROM {SUPABASE_TABLE} WHERE name=%s", (key,))
                    return cur.fetchone() is not None
        except Exception:
            return False
    return get_db_path(key).exists()


def delete_db_file(db_name: str) -> bool:
    """Удаляет файл базы данных целиком. Возвращает True при успехе."""
    key = normalize_db_key(db_name)
    if USE_SUPABASE:
        try:
            with _pg_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {SUPABASE_TABLE} WHERE name=%s", (key,))
                    deleted = cur.rowcount > 0
                    conn.commit()
            return deleted
        except Exception as e:
            logger.error(f"Ошибка удаления БД (Supabase) {key}: {e}")
            return False

    path = get_db_path(key)
    try:
        if not path.exists():
            return False
        path.unlink()
        return True
    except OSError as e:
        logger.error(f"Ошибка удаления файла БД {key}: {e}")
        return False


# ─────────────────────────────────────────────────────────
# РАБОТА С ЗАПИСЯМИ
# ─────────────────────────────────────────────────────────

def extract_record_data(message: Message) -> dict:
    """
    Извлекает данные из сообщения Telegram и формирует структуру записи.
    Поддерживает: текст, фото, видео, документ, аудио, голос, стикер.
    """
    record: dict[str, Any] = {
        "timestamp": now(),
        "metadata": {},
    }

    # ── Фото ──────────────────────────────────────────────
    if message.photo:
        photo = message.photo[-1]  # Берём максимальное качество
        record["type"] = "photo"
        record["file_id"] = photo.file_id
        record["file_unique_id"] = photo.file_unique_id
        record["text"] = message.caption or ""
        record["metadata"] = {
            "width": photo.width,
            "height": photo.height,
            "file_size": photo.file_size,
        }

    # ── Видео ─────────────────────────────────────────────
    elif message.video:
        video = message.video
        record["type"] = "video"
        record["file_id"] = video.file_id
        record["file_unique_id"] = video.file_unique_id
        record["text"] = message.caption or ""
        record["metadata"] = {
            "duration": video.duration,
            "width": video.width,
            "height": video.height,
            "mime_type": video.mime_type,
            "file_size": video.file_size,
            "file_name": video.file_name,
        }

    # ── Документ ──────────────────────────────────────────
    elif message.document:
        doc = message.document
        record["type"] = "document"
        record["file_id"] = doc.file_id
        record["file_unique_id"] = doc.file_unique_id
        record["text"] = message.caption or ""
        record["metadata"] = {
            "file_name": doc.file_name,
            "mime_type": doc.mime_type,
            "file_size": doc.file_size,
        }

    # ── Аудио ─────────────────────────────────────────────
    elif message.audio:
        audio = message.audio
        record["type"] = "audio"
        record["file_id"] = audio.file_id
        record["file_unique_id"] = audio.file_unique_id
        record["text"] = message.caption or audio.title or ""
        record["metadata"] = {
            "duration": audio.duration,
            "performer": audio.performer,
            "title": audio.title,
            "mime_type": audio.mime_type,
            "file_size": audio.file_size,
            "file_name": audio.file_name,
        }

    # ── Голосовое сообщение ───────────────────────────────
    elif message.voice:
        voice = message.voice
        record["type"] = "voice"
        record["file_id"] = voice.file_id
        record["file_unique_id"] = voice.file_unique_id
        record["text"] = message.caption or ""
        record["metadata"] = {
            "duration": voice.duration,
            "mime_type": voice.mime_type,
            "file_size": voice.file_size,
        }

    # ── Видео-заметка (кружочек) ──────────────────────────
    elif message.video_note:
        vn = message.video_note
        record["type"] = "video_note"
        record["file_id"] = vn.file_id
        record["file_unique_id"] = vn.file_unique_id
        record["text"] = ""
        record["metadata"] = {
            "duration": vn.duration,
            "length": vn.length,
            "file_size": vn.file_size,
        }

    # ── Стикер ────────────────────────────────────────────
    elif message.sticker:
        sticker = message.sticker
        record["type"] = "sticker"
        record["file_id"] = sticker.file_id
        record["file_unique_id"] = sticker.file_unique_id
        record["text"] = sticker.emoji or ""
        record["metadata"] = {
            "set_name": sticker.set_name,
            "is_animated": sticker.is_animated,
            "is_video": sticker.is_video,
        }

    # ── Только текст ──────────────────────────────────────
    elif message.text:
        record["type"] = "text"
        record["file_id"] = None
        record["file_unique_id"] = None
        record["text"] = message.text
        # Извлекаем ссылки из текста
        urls = re.findall(r'https?://\S+', message.text)
        record["metadata"] = {"urls": urls}

    else:
        record["type"] = "unknown"
        record["file_id"] = None
        record["file_unique_id"] = None
        record["text"] = message.caption or ""

    return record


def add_record_to_db(db_name: str, record_data: dict) -> int:
    """
    Добавляет запись в базу данных и возвращает присвоенный ID.
    """
    db = load_db(db_name)
    record_id = db.get("next_id", 1)
    record_data["id"] = record_id
    db["records"].append(record_data)
    db["next_id"] = record_id + 1
    db["updated_at"] = now()
    save_db(db_name, db)
    return record_id


def get_record_by_id(db_name: str, record_id: int) -> Optional[dict]:
    """Возвращает запись по ID или None."""
    db = load_db(db_name)
    for record in db["records"]:
        if record.get("id") == record_id:
            return record
    return None


def delete_record_by_id(db_name: str, record_id: int) -> bool:
    """Удаляет запись по ID. Возвращает True при успехе."""
    db = load_db(db_name)
    original_count = len(db["records"])
    db["records"] = [r for r in db["records"] if r.get("id") != record_id]
    if len(db["records"]) < original_count:
        db["updated_at"] = now()
        return save_db(db_name, db)
    return False


def update_record_text(db_name: str, record_id: int, new_text: str) -> bool:
    """Обновляет текст/подпись записи."""
    db = load_db(db_name)
    for record in db["records"]:
        if record.get("id") == record_id:
            record["text"] = new_text
            record["updated_at"] = now()
            db["updated_at"] = now()
            return save_db(db_name, db)
    return False


def search_records(db_name: str, query: str) -> list[dict]:
    """Поиск записей по тексту (без учёта регистра)."""
    db = load_db(db_name)
    query_lower = query.lower()
    results = []
    for record in db["records"]:
        text = record.get("text", "").lower()
        metadata_str = json.dumps(record.get("metadata", {}), ensure_ascii=False).lower()
        if query_lower in text or query_lower in metadata_str:
            results.append(record)
    return results


def _tokenize(text: str) -> set[str]:
    return set(re.findall(r"[0-9a-zа-я_]+", text.lower()))


def _record_to_rank_text(record: dict) -> str:
    text = record.get("text") or ""
    metadata_str = ""
    try:
        metadata_str = json.dumps(record.get("metadata", {}), ensure_ascii=False)
    except Exception:
        metadata_str = str(record.get("metadata", ""))
    parts = [
        str(record.get("type", "")),
        str(record.get("timestamp", "")),
        text,
        metadata_str,
    ]
    return " ".join(p for p in parts if p)


def _select_semantic_candidates(records: list[dict], query: str, limit: int = 40) -> list[dict]:
    if len(records) <= limit:
        return records

    query_tokens = _tokenize(query)
    if not query_tokens:
        return records[:limit]

    scored: list[tuple[int, dict]] = []
    for record in records:
        hay_tokens = _tokenize(_record_to_rank_text(record))
        score = len(query_tokens & hay_tokens)
        scored.append((score, record))

    scored.sort(key=lambda x: x[0], reverse=True)
    # Если всё по 0 — просто берём последние добавленные.
    if scored and scored[0][0] == 0:
        return records[-limit:]
    return [r for _, r in scored[:limit]]


async def _groq_chat_completion(messages: list[dict], *, temperature: float = 0.1, max_tokens: int = 600) -> dict:
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY не задан")

    # Prefer official Groq SDK when available (more compatible with Groq-specific params).
    if Groq is not None:
        def _do_sdk_request() -> dict:
            client = Groq(api_key=GROQ_API_KEY, base_url=_groq_sdk_base_url(GROQ_BASE_URL))
            completion = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                top_p=1,
                stream=False,
                extra_body={"reasoning_effort": GROQ_REASONING_EFFORT},
            )
            # groq sdk returns pydantic-like objects; normalize to dict
            if hasattr(completion, "model_dump"):
                return completion.model_dump()
            if hasattr(completion, "model_dump_json"):
                return json.loads(completion.model_dump_json())
            if hasattr(completion, "to_dict"):
                return completion.to_dict()
            return json.loads(str(completion))

        try:
            return await asyncio.to_thread(_do_sdk_request)
        except Exception as e:
            raise RuntimeError(f"Ошибка запроса к Groq SDK: {e}") from e

    url = f"{_groq_http_base_url(GROQ_BASE_URL)}/chat/completions"
    payload = {
        "model": GROQ_MODEL,
        "messages": messages,
        "temperature": temperature,
        # На разных версиях API встречаются оба параметра, поэтому передаём оба.
        "max_tokens": max_tokens,
        "max_completion_tokens": max_tokens,
        "top_p": 1,
        "reasoning_effort": GROQ_REASONING_EFFORT,
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }

    def _do_request() -> dict:
        req = urlrequest.Request(url, data=body, headers=headers, method="POST")
        with urlrequest.urlopen(req, timeout=45) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw)

    try:
        return await asyncio.to_thread(_do_request)
    except HTTPError as e:
        details = ""
        try:
            details = e.read().decode("utf-8", errors="replace")
        except Exception:
            details = ""
        raise RuntimeError(f"Ошибка запроса к Groq: HTTP {e.code} {details}".strip()) from e
    except (URLError, TimeoutError, json.JSONDecodeError) as e:
        raise RuntimeError(f"Ошибка запроса к Groq: {e}") from e


def _extract_ids_from_ai_response(text: str) -> list[int]:
    text = (text or "").strip()
    if not text:
        return []

    # Ожидаем JSON: {"ids":[1,2,3]} или [1,2,3]
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict) and isinstance(parsed.get("ids"), list):
            return [int(x) for x in parsed["ids"] if str(x).lstrip("-").isdigit()]
        if isinstance(parsed, list):
            return [int(x) for x in parsed if str(x).lstrip("-").isdigit()]
    except Exception:
        pass

    # Фолбек: вытащим числа из текста
    return [int(x) for x in re.findall(r"\b\d+\b", text)][:15]


def _extract_json_object(text: str) -> Optional[dict]:
    s = (text or "").strip()
    if not s:
        return None
    # Попытка напрямую
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj
        return None
    except Exception:
        pass
    # Попытка вытащить первый JSON-объект из текста
    m = re.search(r"\{.*\}", s, flags=re.DOTALL)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


async def ai_assistant_decide(query: str, *, databases: list[dict]) -> dict:
    """
    Просит модель вернуть JSON-команду.
    Формат ответа (строго JSON):
    - {"action":"answer","text":"..."}
    - {"action":"search","query":"..."}
    - {"action":"send_db","db":"<db_name>"}
    - {"action":"send_source","file":"base.py|bot.py"}
    - {"action":"delete_db","db":"<db_name>"}
    """
    db_names = [d.get("file", "").replace(".json", "") for d in databases]
    db_overview = [
        {
            "db": d.get("file", "").replace(".json", ""),
            "name": d.get("name"),
            "records": d.get("records_count"),
            "size": d.get("size"),
        }
        for d in databases
    ]

    sys = (
        "Ты — помощник Telegram-бота с локальными JSON-базами.\n"
        "Ты НЕ имеешь прямого доступа к файловой системе: тебе дают список баз и их метаданные.\n"
        "Твоя задача — выбрать действие и вернуть СТРОГО JSON без текста вокруг."
    )

    user = (
        f"Запрос пользователя: {query}\n\n"
        f"Доступные базы (db = имя файла без .json): {db_names}\n"
        f"Метаданные баз (JSON): {json.dumps(db_overview, ensure_ascii=False)}\n\n"
        "Правила:\n"
        "- Если пользователь хочет найти что-то в базе (где/найди/поиск/в каком отсеке) -> action=search.\n"
        "- Если просит скинуть файл базы -> action=send_db.\n"
        "- Если просит удалить базу целиком -> action=delete_db.\n"
        "- Если просит скинуть код -> action=send_source (только base.py или bot.py).\n"
        "- Иначе -> action=answer.\n\n"
        "Дополнительно:\n"
        "- В поле db используй ТОЛЬКО значение из списка db.\n"
        "- Если не уверен с db, выбери action=search.\n\n"
        "Верни JSON одной строкой."
    )

    resp = await _groq_chat_completion(
        [{"role": "system", "content": sys}, {"role": "user", "content": user}],
        temperature=0.2,
        max_tokens=350,
    )

    content = ""
    try:
        content = resp["choices"][0]["message"]["content"]
    except Exception:
        content = ""

    obj = _extract_json_object(content) or {}
    # Нормализация
    if not isinstance(obj.get("action"), str):
        obj["action"] = "answer"
    return obj


def _guess_db_name_from_text(text: str, databases: list[dict]) -> str:
    q = (text or "").strip().lower()
    if not q:
        return ""
    candidates = [d.get("file", "").replace(".json", "") for d in databases if d.get("file")]
    # Прямое вхождение имени базы
    for name in candidates:
        if name and name.lower() in q:
            return name
    # Частый случай: "удали базу 1"
    m = re.search(r"\b(?:баз[ауеы]?|base)\s*[:#]?\s*([0-9A-Za-z_.-]+)\b", q)
    if m:
        token = m.group(1)
        for name in candidates:
            if name.lower() == token.lower():
                return name
    return ""


async def semantic_search_records_ai(db_name: str, query: str, *, limit: int = 10) -> list[dict]:
    """Умный поиск через Groq: возвращает список записей (record dict)."""
    db = load_db(db_name)
    records = db.get("records", []) or []
    if not records:
        return []

    # Модель не может безопасно "увидеть" тысячи записей за 1 запрос, поэтому делаем проход по чанкам,
    # чтобы поиск охватывал всю базу, а затем делаем финальную переоценку.
    chunk_size = 40
    per_chunk = 5

    def _to_payload(r: dict) -> dict:
        return {
            "id": r.get("id"),
            "type": r.get("type"),
            "timestamp": r.get("timestamp"),
            "text": (r.get("text") or "")[:1200],
            "metadata": r.get("metadata", {}),
        }

    preselected: list[dict] = []
    if len(records) <= chunk_size:
        preselected = records
    else:
        for start in range(0, len(records), chunk_size):
            chunk = records[start:start + chunk_size]
            chunk_payload = [_to_payload(r) for r in chunk]

            messages = [
                {"role": "system", "content": "Ты — умный поисковик по локальной базе записей Telegram-бота."},
                {
                    "role": "user",
                    "content": (
                        f"Запрос: {query}\n\n"
                        f"База: {db_name}\n\n"
                        "candidates (JSON):\n"
                        f"{json.dumps(chunk_payload, ensure_ascii=False)}\n\n"
                        f"Верни строго JSON без пояснений: {{\"ids\": [..]}} (до {per_chunk} id) в порядке релевантности."
                    ),
                },
            ]

            resp = await _groq_chat_completion(messages, max_tokens=350)
            content = ""
            try:
                content = resp["choices"][0]["message"]["content"]
            except Exception:
                content = ""

            ids = _extract_ids_from_ai_response(content)
            if not ids:
                continue

            by_id_chunk = {r.get("id"): r for r in chunk}
            for rid in ids:
                r = by_id_chunk.get(rid)
                if r:
                    preselected.append(r)

    # Финальная переоценка по объединённому shortlist.
    candidates = _select_semantic_candidates(preselected, query, limit=60)
    candidates_payload = [_to_payload(r) for r in candidates]

    code_context = (
        "Схема записи: {id:int, type:str, timestamp:str, text:str, file_id?:str, file_unique_id?:str, metadata:object}.\n"
        "Задача: выбрать самые релевантные записи к запросу пользователя.\n"
        "Нельзя придумывать ID — используй только те, что есть в candidates."
    )

    messages = [
        {"role": "system", "content": "Ты — умный поисковик по локальной базе записей Telegram-бота."},
        {"role": "system", "content": code_context},
        {
            "role": "user",
            "content": (
                f"Запрос: {query}\n\n"
                f"База: {db_name}\n\n"
                "candidates (JSON):\n"
                f"{json.dumps(candidates_payload, ensure_ascii=False)}\n\n"
                f"Верни строго JSON без пояснений: {{\"ids\": [..]}} (до {limit} id) в порядке релевантности."
            ),
        },
    ]

    resp = await _groq_chat_completion(messages)
    content = ""
    try:
        content = resp["choices"][0]["message"]["content"]
    except Exception:
        content = ""

    ids = _extract_ids_from_ai_response(content)
    if not ids:
        return []

    by_id = {r.get("id"): r for r in records}
    out: list[dict] = []
    for rid in ids:
        record = by_id.get(rid)
        if record:
            out.append(record)
        if len(out) >= limit:
            break
    return out


async def semantic_search_all_dbs_ai(query: str, *, limit: int = 10) -> list[dict]:
    """
    Умный поиск по всем базам.
    Возвращает список элементов вида: {"db_name": str, "record": dict}
    """
    items = load_all_dbs_records()
    if not items:
        return []

    chunk_size = 35
    per_chunk = 4

    def _to_payload(item: dict) -> dict:
        r = item["record"]
        return {
            "db_name": item["db_name"],
            "id": r.get("id"),
            "type": r.get("type"),
            "timestamp": r.get("timestamp"),
            "text": (r.get("text") or "")[:1000],
            "metadata": r.get("metadata", {}),
        }

    shortlisted: list[dict] = []
    query_lower = (query or "").lower()
    type_hint: set[str] = set()
    if any(x in query_lower for x in ("аудио", "музык", "песня", "mp3")):
        type_hint.add("audio")
    if any(x in query_lower for x in ("голос", "войс", "voice")):
        type_hint.add("voice")

    if type_hint:
        items = [it for it in items if (it.get("record") or {}).get("type") in type_hint] or items

    # Быстрая предварительная фильтрация, чтобы не делать тысячи запросов к API на больших базах.
    if SAY_MAX_ITEMS > 0 and len(items) > SAY_MAX_ITEMS:
        query_tokens = _tokenize(query)
        if query_tokens:
            scored: list[tuple[int, dict]] = []
            for it in items:
                r = it.get("record") or {}
                hay = _record_to_rank_text(r)
                score = len(query_tokens & _tokenize(hay))
                scored.append((score, it))
            scored.sort(key=lambda x: x[0], reverse=True)
            # Если всё по 0 — берём последние добавленные
            if scored and scored[0][0] == 0:
                items = items[-SAY_MAX_ITEMS:]
            else:
                items = [it for _, it in scored[:SAY_MAX_ITEMS]]
        else:
            items = items[-SAY_MAX_ITEMS:]

    code_context = (
        "Данные: в папке base/ лежат JSON-файлы баз. Каждая база содержит records[] с полями:\n"
        "- id (int)\n"
        "- type (text/photo/video/document/audio/voice/video_note/sticker/unknown)\n"
        "- timestamp (str)\n"
        "- text (str)\n"
        "- file_id (для медиа)\n"
        "- metadata (объект)\n"
        "Задача: по запросу пользователя выбрать наиболее релевантные элементы (db_name + id)."
    )

    for start in range(0, len(items), chunk_size):
        chunk = items[start:start + chunk_size]
        chunk_payload = [_to_payload(it) for it in chunk]
        messages = [
            {"role": "system", "content": "Ты — умный поисковик по всем JSON-базам Telegram-бота."},
            {"role": "system", "content": code_context},
            {
                "role": "user",
                "content": (
                    f"Запрос: {query}\n\n"
                    "candidates (JSON):\n"
                    f"{json.dumps(chunk_payload, ensure_ascii=False)}\n\n"
                    f"Верни строго JSON без пояснений: {{\"items\": [{{\"db_name\":\"...\",\"id\":123}}]}} (до {per_chunk} элементов) "
                    "в порядке релевантности. Используй только db_name/id из candidates."
                ),
            },
        ]

        resp = await _groq_chat_completion(messages, max_tokens=350)
        content = ""
        try:
            content = resp["choices"][0]["message"]["content"]
        except Exception:
            content = ""

        parsed_items: list[dict] = []
        try:
            parsed = json.loads((content or "").strip())
            if isinstance(parsed, dict) and isinstance(parsed.get("items"), list):
                parsed_items = [x for x in parsed["items"] if isinstance(x, dict)]
        except Exception:
            parsed_items = []

        if not parsed_items:
            # Фолбек: попробуем вытащить id и db_name грубо
            ids = _extract_ids_from_ai_response(content)
            if ids:
                # если db_name не указан, то просто добавим из текущего чанка
                by_id = {(it["db_name"], it["record"].get("id")): it for it in chunk}
                for rid in ids:
                    for (dbn, rec_id), it in by_id.items():
                        if rec_id == rid:
                            shortlisted.append(it)
            continue

        by_key = {(it["db_name"], it["record"].get("id")): it for it in chunk}
        for x in parsed_items:
            dbn = x.get("db_name")
            rid = x.get("id")
            try:
                rid_int = int(rid)
            except Exception:
                continue
            it = by_key.get((dbn, rid_int))
            if it:
                shortlisted.append(it)

    if not shortlisted:
        return []

    # Финальная переоценка уже на коротком списке
    final_payload = []
    for it in shortlisted[:120]:
        final_payload.append(_to_payload(it))

    messages = [
        {"role": "system", "content": "Ты — умный поисковик по всем JSON-базам Telegram-бота."},
        {"role": "system", "content": code_context},
        {
            "role": "user",
            "content": (
                f"Запрос: {query}\n\n"
                "candidates (JSON):\n"
                f"{json.dumps(final_payload, ensure_ascii=False)}\n\n"
                f"Верни строго JSON без пояснений: {{\"items\": [{{\"db_name\":\"...\",\"id\":123}}]}} (до {limit} элементов) "
                "в порядке релевантности. Используй только db_name/id из candidates."
            ),
        },
    ]

    resp = await _groq_chat_completion(messages, max_tokens=450)
    content = ""
    try:
        content = resp["choices"][0]["message"]["content"]
    except Exception:
        content = ""

    parsed_items: list[dict] = []
    try:
        parsed = json.loads((content or "").strip())
        if isinstance(parsed, dict) and isinstance(parsed.get("items"), list):
            parsed_items = [x for x in parsed["items"] if isinstance(x, dict)]
    except Exception:
        parsed_items = []

    if not parsed_items:
        return shortlisted[:limit]

    by_key = {(it["db_name"], it["record"].get("id")): it for it in shortlisted}
    out: list[dict] = []
    for x in parsed_items:
        dbn = x.get("db_name")
        rid = x.get("id")
        try:
            rid_int = int(rid)
        except Exception:
            continue
        it = by_key.get((dbn, rid_int))
        if it and it not in out:
            out.append(it)
        if len(out) >= limit:
            break

    return out


def get_statistics(db_name: str) -> dict:
    """Возвращает статистику по базе данных."""
    key = normalize_db_key(db_name)
    db = load_db(key)
    records = db.get("records", [])

    type_counts: dict[str, int] = {}
    for record in records:
        rtype = record.get("type", "unknown")
        type_counts[rtype] = type_counts.get(rtype, 0) + 1

    file_size = 0
    if USE_SUPABASE:
        try:
            with _pg_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT pg_column_size(data) FROM {SUPABASE_TABLE} WHERE name=%s", (key,))
                    row = cur.fetchone()
                    file_size = int(row[0]) if row and row[0] is not None else 0
        except Exception:
            file_size = 0
    else:
        file_size = get_db_path(key).stat().st_size if db_exists(key) else 0

    return {
        "name": db.get("name", key),
        "total_records": len(records),
        "type_counts": type_counts,
        "created_at": db.get("created_at", "Неизвестно"),
        "updated_at": db.get("updated_at", "Никогда"),
        "file_size": file_size,
    }


# ─────────────────────────────────────────────────────────
# КЛАВИАТУРЫ
# ─────────────────────────────────────────────────────────

def main_menu_keyboard() -> ReplyKeyboardMarkup:
    """Главное меню бота (reply keyboard)."""
    buttons = [
        [KeyboardButton(text="➕ Добавить запись"), KeyboardButton(text="📂 Открыть базу")],
        [KeyboardButton(text="🔍 Поиск"),           KeyboardButton(text="🖼 Получить запись")],
        [KeyboardButton(text="🗑 Удалить запись"),  KeyboardButton(text="🧨 Удалить базу")],
        [KeyboardButton(text="✏️ Изменить запись"), KeyboardButton(text="📊 Статистика")],
        [KeyboardButton(text="📁 Список баз"),      KeyboardButton(text=BACKUP_ALL_BUTTON_TEXT)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def cancel_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура с кнопкой отмены."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True,
    )


def add_session_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура для режима добавления (завершить/отмена)."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=FINISH_ADD_SESSION_TEXT)],
            [KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def confirm_keyboard() -> InlineKeyboardMarkup:
    """Inline клавиатура подтверждения."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, сохранить",  callback_data="confirm_yes"),
            InlineKeyboardButton(text="❌ Отмена",         callback_data="confirm_no"),
        ]
    ])


def confirm_delete_keyboard(record_id: int) -> InlineKeyboardMarkup:
    """Inline клавиатура подтверждения удаления."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🗑 Удалить",  callback_data=f"delete_yes_{record_id}"),
            InlineKeyboardButton(text="❌ Отмена",   callback_data="delete_no"),
        ]
    ])


def confirm_delete_db_keyboard(db_name: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🧨 Удалить базу", callback_data=f"dbdrop_yes_{db_name}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="dbdrop_no"),
        ]
    ])


def confirm_ai_delete_db_keyboard(db_name: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🧨 Удалить базу", callback_data=f"ask_dbdel_yes_{db_name}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="ask_dbdel_no"),
        ]
    ])


def databases_keyboard(databases: list[dict], action: str = "open") -> InlineKeyboardMarkup:
    """Inline клавиатура со списком баз данных."""
    buttons = []
    for db in databases:
        label = f"📄 {db['name']} ({db['records_count']} зап.)"
        buttons.append([
            InlineKeyboardButton(
                text=label,
                callback_data=f"db_{action}_{db['file'].replace('.json', '')}",
            )
        ])
    if not buttons:
        buttons.append([
            InlineKeyboardButton(text="Нет баз данных", callback_data="noop")
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def browse_keyboard(current_page: int, total_pages: int, db_name: str) -> InlineKeyboardMarkup:
    """Inline клавиатура навигации по записям."""
    nav_row = []
    if current_page > 1:
        nav_row.append(InlineKeyboardButton(
            text="◀️ Назад",
            callback_data=f"browse_page_{current_page - 1}",
        ))
    nav_row.append(InlineKeyboardButton(
        text=f"{current_page}/{total_pages}",
        callback_data="noop",
    ))
    if current_page < total_pages:
        nav_row.append(InlineKeyboardButton(
            text="Вперёд ▶️",
            callback_data=f"browse_page_{current_page + 1}",
        ))

    return InlineKeyboardMarkup(inline_keyboard=[
        nav_row,
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="go_home")],
    ])


def _record_line_preview(record: dict) -> str:
    type_emoji = {
        "text": "📝",
        "photo": "🖼",
        "video": "🎬",
        "document": "📄",
        "audio": "🎵",
        "voice": "🎤",
        "video_note": "⭕",
        "sticker": "🎭",
        "unknown": "❓",
    }
    emoji = type_emoji.get(record.get("type", "unknown"), "❓")
    rid = record.get("id", "—")
    ts = record.get("timestamp", "—")
    text_preview = (record.get("text") or "").replace("\n", " ").strip()
    if len(text_preview) > 50:
        text_preview = text_preview[:50] + "…"
    if text_preview:
        return f"{emoji} <b>#{rid}</b> | {ts}\n<code>{html.escape(text_preview)}</code>"
    return f"{emoji} <b>#{rid}</b> | {ts}"


def browse_list_keyboard(page_records: list[dict], current_page: int, total_pages: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for record in page_records:
        rid = record.get("id")
        rtype = record.get("type", "unknown")
        label = f"#{rid} · {rtype}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"browse_open_{rid}")])
    rows.append(browse_keyboard(current_page, total_pages, db_name="").inline_keyboard[0])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="go_home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def browse_record_keyboard(record_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📤 Показать", callback_data=f"browse_send_{record_id}"),
            InlineKeyboardButton(text="⬅️ Назад", callback_data="browse_back"),
        ],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="go_home")],
    ])


# ─────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ─────────────────────────────────────────────────────────

def is_allowed(user_id: int) -> bool:
    """Проверяет доступ пользователя."""
    return user_id in ALLOWED_USER_IDS


def format_record_info(record: dict) -> str:
    """Форматирует информацию о записи в читаемый текст."""
    type_emoji = {
        "text":       "📝",
        "photo":      "🖼",
        "video":      "🎬",
        "document":   "📄",
        "audio":      "🎵",
        "voice":      "🎤",
        "video_note": "⭕",
        "sticker":    "🎭",
        "unknown":    "❓",
    }
    emoji = type_emoji.get(record.get("type", "unknown"), "❓")
    text_preview = (record.get("text") or "")[:100]
    if len(record.get("text", "")) > 100:
        text_preview += "..."
    text_preview = html.escape(text_preview) if text_preview else ""

    lines = [
        f"{emoji} <b>Запись #{record.get('id')}</b>",
        f"📌 Тип: <code>{record.get('type', 'unknown')}</code>",
        f"🕒 Дата: {record.get('timestamp', '—')}",
    ]
    if text_preview:
        lines.append(f"📄 Текст: {text_preview}")

    meta = record.get("metadata", {})
    if meta.get("file_name"):
        lines.append(f"📎 Файл: {html.escape(str(meta['file_name']))}")
    if meta.get("duration"):
        lines.append(f"⏱ Длительность: {meta['duration']} сек.")

    return "\n".join(lines)


def format_file_size(size_bytes: int) -> str:
    """Форматирует размер файла в читаемый вид."""
    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} ТБ"


async def send_record_content(
    message_or_query: Union[Message, CallbackQuery],
    record: dict,
) -> None:
    """
    Отправляет контент записи пользователю по сохранённому file_id.
    Фото → как фото, видео → как видео и т.д.
    """
    # Определяем целевой чат
    if isinstance(message_or_query, CallbackQuery):
        target = message_or_query.message
    else:
        target = message_or_query

    rtype = record.get("type", "unknown")
    file_id = record.get("file_id")
    trusted_html = bool(record.get("_trusted_html"))
    text_value = (record.get("text") or "").strip()
    if trusted_html:
        safe_text = text_value
    else:
        safe_text = html.escape(text_value) if text_value else ""
    caption = safe_text[:1024] if safe_text else None

    try:
        if rtype == "text":
            await target.answer(safe_text or "Пустая запись")

        elif rtype == "photo" and file_id:
            await target.answer_photo(photo=file_id, caption=caption)

        elif rtype == "video" and file_id:
            await target.answer_video(video=file_id, caption=caption)

        elif rtype == "document" and file_id:
            await target.answer_document(document=file_id, caption=caption)

        elif rtype == "audio" and file_id:
            await target.answer_audio(audio=file_id, caption=caption)

        elif rtype == "voice" and file_id:
            await target.answer_voice(voice=file_id, caption=caption)

        elif rtype == "video_note" and file_id:
            await target.answer_video_note(video_note=file_id)

        elif rtype == "sticker" and file_id:
            await target.answer_sticker(sticker=file_id)

        else:
            await target.answer(f"⚠️ Неизвестный тип контента: <code>{rtype}</code>")

    except Exception as e:
        logger.error(f"Ошибка отправки контента: {e}")
        await target.answer(f"❌ Ошибка при отправке: {e}")


async def _edit_or_send(message: Message, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> Message:
    try:
        return await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest:
        return await message.answer(text, reply_markup=reply_markup)


async def show_records_page(
    message: Message,
    db_name: str,
    page: int = 1,
    page_size: int = 5,
) -> None:
    """Показывает страницу записей из базы данных (без спама файлами)."""
    db = load_db(db_name)
    records = db.get("records", [])

    if not records:
        await _edit_or_send(
            message,
            "📭 База данных пуста.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🏠 Главное меню", callback_data="go_home")],
            ]),
        )
        return

    total_pages = max(1, (len(records) + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))

    start_idx = (page - 1) * page_size
    page_records = records[start_idx : start_idx + page_size]

    header = (
        f"📂 <b>База:</b> <code>{html.escape(str(db.get('name', db_name)))}</code>\n"
        f"📊 Всего записей: <b>{len(records)}</b> | Страница <b>{page}/{total_pages}</b>\n"
        f"{'─' * 30}\n"
        "Выберите запись, чтобы посмотреть детали (файлы не отправляются автоматически)."
    )

    lines = [header]
    for record in page_records:
        lines.append("")
        lines.append(_record_line_preview(record))

    await _edit_or_send(
        message,
        "\n".join(lines),
        reply_markup=browse_list_keyboard(page_records, page, total_pages),
    )


# ─────────────────────────────────────────────────────────
# MIDDLEWARE (проверка доступа)
# ─────────────────────────────────────────────────────────

@router.message(lambda m: not is_allowed(m.from_user.id))
async def access_denied(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("⛔ У вас нет доступа к этому боту.")


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext) -> None:
    """Обработчик команды /start."""
    if not is_allowed(message.from_user.id):
        await message.answer("⛔ У вас нет доступа к этому боту.")
        return

    await state.clear()
    user_name = message.from_user.first_name or "Пользователь"
    text = (
        f"👋 Привет, <b>{user_name}</b>!\n\n"
        "🗄 Я — <b>умная база данных</b> для хранения контента.\n\n"
        "Я умею сохранять:\n"
        "• 📝 Текст и ссылки\n"
        "• 🖼 Фото и видео\n"
        "• 📄 Документы\n"
        "• 🎵 Аудио и голосовые\n"
        "• ⭕ Видео-заметки\n\n"
        "Используй меню ниже 👇"
    )
    await message.answer(text, reply_markup=main_menu_keyboard())


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    """Обработчик команды /help."""
    text = (
        "📖 <b>Справка по боту</b>\n\n"
        "➕ <b>Добавить запись</b> — сохранить контент в базу\n"
        "📂 <b>Открыть базу</b> — просмотр записей\n"
        "🔍 <b>Поиск</b> — поиск по тексту\n"
        "🧠 <b>/ask</b> — ИИ-помощник (поиск/файлы/удаление базы)\n"
        "🖼 <b>Получить запись</b> — получить запись по ID\n"
        "🗑 <b>Удалить запись</b> — удалить по ID\n"
        "🧨 <b>Удалить базу</b> — удалить JSON-файл базы целиком\n"
        "✏️ <b>Изменить запись</b> — редактировать текст\n"
        "📊 <b>Статистика</b> — информация о базе\n"
        "📁 <b>Список баз</b> — все базы данных\n\n"
        f"{BACKUP_ALL_BUTTON_TEXT} — скачать ZIP со всем содержимым баз\n\n"
        "💡 Базы хранятся локально (base/) или в Supabase.\n"
        "Файлы выгружаются из Telegram по file_id без потери качества."
    )
    await message.answer(text, reply_markup=main_menu_keyboard())


@router.message(Command("cancel"))
@router.message(F.text == "❌ Отмена")
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    """Универсальная отмена любого действия."""
    current_state = await state.get_state()
    if current_state:
        await state.clear()
        await message.answer("❌ Действие отменено.", reply_markup=main_menu_keyboard())
    else:
        await message.answer("🏠 Главное меню", reply_markup=main_menu_keyboard())


async def cmd_say(message: Message, state: FSMContext) -> None:
    """Умный поиск по всем базам через Groq."""
    if not is_allowed(message.from_user.id):
        return

    query = (message.text or "").split(maxsplit=1)
    query = query[1].strip() if len(query) > 1 else ""
    if not query:
        await state.clear()
        await state.set_state(SayAI.waiting_query)
        await message.answer(
            "🧠 <b>Умный поиск</b>\n\n"
            "Напиши, что нужно найти (можно примерно, без точных названий).\n"
            "Пример: <code>я не помню где аудио сообщение</code>",
            reply_markup=cancel_keyboard(),
        )
        return

    await state.clear()
    await _run_say_search(message, query)


@router.message(Command("ask"))
async def cmd_ask(message: Message, state: FSMContext) -> None:
    """Диалог с ИИ: ответ / поиск / файл / удаление базы (с подтверждением)."""
    if not is_allowed(message.from_user.id):
        return

    query_parts = (message.text or "").split(maxsplit=1)
    query = query_parts[1].strip() if len(query_parts) > 1 else ""

    if not query:
        await state.set_state(AskAI.waiting_message)
        await message.answer(
            "🧠 <b>ИИ-помощник</b>\n\n"
            "Напиши вопрос или что нужно сделать.\n"
            "Примеры:\n"
            "• <code>где аудио сообщение?</code>\n"
            "• <code>скинь файл базы Test_number_1</code>\n"
            "• <code>удали базу 1</code>",
            reply_markup=cancel_keyboard(),
        )
        return

    await state.set_state(AskAI.waiting_message)
    await _run_ask_assistant(message, state, query)


@router.message(Command("say"))
async def cmd_say_redirect(message: Message) -> None:
    """Совместимость: /say теперь перенаправляет на /ask."""
    await message.answer("ℹ️ Используйте команду /ask (теперь ИИ доступен только через неё).")


@router.message(StateFilter(SayAI.waiting_query))
async def say_waiting_query(message: Message, state: FSMContext) -> None:
    if message.text == "❌ Отмена":
        await cmd_cancel(message, state)
        return

    query = (message.text or "").strip()
    if not query:
        await message.answer("⚠️ Введите непустой запрос.", reply_markup=cancel_keyboard())
        return

    await state.clear()
    await _run_say_search(message, query)


@router.message(StateFilter(AskAI.waiting_message))
async def ask_waiting_message(message: Message, state: FSMContext) -> None:
    if message.text == "❌ Отмена":
        await cmd_cancel(message, state)
        return

    # Если пользователь нажал кнопку меню во время диалога с ИИ,
    # то выходим из состояния /ask и выполняем действие кнопки.
    menu_text = (message.text or "").strip()
    if menu_text in {
        "➕ Добавить запись",
        "📂 Открыть базу",
        "🔍 Поиск",
        "🖼 Получить запись",
        "🗑 Удалить запись",
        "🧨 Удалить базу",
        "✏️ Изменить запись",
        "📊 Статистика",
        "📁 Список баз",
        BACKUP_ALL_BUTTON_TEXT,
    }:
        await state.clear()
        # Выполняем соответствующий обработчик напрямую
        if menu_text == "➕ Добавить запись":
            await add_record_start(message, state)
        elif menu_text == "📂 Открыть базу":
            await open_base_start(message, state)
        elif menu_text == "🔍 Поиск":
            await search_start(message, state)
        elif menu_text == "🖼 Получить запись":
            await get_media_start(message, state)
        elif menu_text == "🗑 Удалить запись":
            await delete_record_start(message, state)
        elif menu_text == "🧨 Удалить базу":
            await delete_database_start(message, state)
        elif menu_text == "✏️ Изменить запись":
            await edit_record_start(message, state)
        elif menu_text == "📊 Статистика":
            await show_statistics(message, state)
        elif menu_text == "📁 Список баз":
            await list_databases(message)
        elif menu_text == BACKUP_ALL_BUTTON_TEXT:
            await backup_all_bases(message, state)
        return

    query = (message.text or "").strip()
    if not query:
        await message.answer("⚠️ Введите непустой запрос.", reply_markup=cancel_keyboard())
        return

    await _run_ask_assistant(message, state, query)


@router.callback_query(F.data.startswith("ask_dbdel_yes_"), StateFilter(AskAI.confirming))
async def ask_dbdel_yes(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    db_name = data.get("pending_db_name", "")

    ok = delete_db_file(db_name)
    if ok:
        await callback.message.edit_text(f"✅ База <code>{db_name}</code> удалена.")
    else:
        await callback.message.edit_text(f"❌ Не удалось удалить базу <code>{db_name}</code>.")

    await callback.answer()
    await state.clear()
    await callback.message.answer("🏠 Главное меню:", reply_markup=main_menu_keyboard())


@router.callback_query(F.data == "ask_dbdel_no")
async def ask_dbdel_no(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.message.edit_text("❌ Удаление базы отменено.")
    await callback.answer()
    await state.set_state(AskAI.waiting_message)


async def _run_say_search(message: Message, query: str) -> None:
    if not GROQ_API_KEY:
        await message.answer(
            "❌ Не задан <code>GROQ_API_KEY</code> в <code>.env</code>.\n"
            "Добавьте ключ, перезапустите бота и повторите /say.",
            reply_markup=main_menu_keyboard(),
        )
        return

    await message.answer("🧠 Ищу по всем базам…")
    try:
        results = await semantic_search_all_dbs_ai(query, limit=10)
    except Exception as e:
        logger.error(f"/say: ошибка умного поиска: {e}")
        details = html.escape(str(e))[:350]
        await message.answer(
            "❌ Ошибка умного поиска. Попробуйте ещё раз.\n"
            f"<code>{details}</code>",
            reply_markup=main_menu_keyboard(),
        )
        return

    if not results:
        await message.answer("🔎 Ничего подходящего не нашёл.", reply_markup=main_menu_keyboard())
        return

    await message.answer(
        f"🔎 Нашёл <b>{len(results)}</b> вариантов. Показываю топ:",
        reply_markup=main_menu_keyboard(),
    )

    for item in results[:10]:
        db_name = item.get("db_name", "—")
        record = item.get("record", {})
        text = f"📁 <b>{html.escape(str(db_name))}</b>\n\n{format_record_info(record)}"
        await message.answer(text)
        if record.get("type") != "text" and record.get("file_id"):
            await send_record_content(message, record)


async def _run_ask_assistant(message: Message, state: FSMContext, query: str) -> None:
    if not GROQ_API_KEY:
        await message.answer(
            "❌ Не задан <code>GROQ_API_KEY</code> в <code>.env</code>.",
            reply_markup=main_menu_keyboard(),
        )
        return

    databases = get_all_databases()
    decision = {}
    try:
        decision = await ai_assistant_decide(query, databases=databases)
    except Exception as e:
        logger.error(f"/ask: ошибка AI decide: {e}")
        decision = {"action": "search", "query": query}

    action = (decision.get("action") or "answer").lower()
    guessed_db = _guess_db_name_from_text(query, databases)

    # 1) Поиск по базам
    if action == "search":
        search_query = (decision.get("query") or query).strip()
        await message.answer("🧠 Ищу по всем базам…")
        try:
            results = await semantic_search_all_dbs_ai(search_query, limit=10)
        except Exception as e:
            logger.error(f"/ask: ошибка умного поиска: {e}")
            details = html.escape(str(e))[:350]
            await message.answer(f"❌ Ошибка умного поиска.\n<code>{details}</code>", reply_markup=main_menu_keyboard())
            return

        if not results:
            await message.answer("🔎 Ничего подходящего не нашёл.", reply_markup=main_menu_keyboard())
            return

        await message.answer(f"🔎 Нашёл <b>{len(results)}</b> вариантов. Показываю топ:", reply_markup=main_menu_keyboard())
        for item in results[:10]:
            db_name = item.get("db_name", "—")
            record = item.get("record", {})
            text = f"📁 <b>{html.escape(str(db_name))}</b>\n\n{format_record_info(record)}"
            await message.answer(text)
            if record.get("type") != "text" and record.get("file_id"):
                await send_record_content(message, record)
        return

    # 2) Скинуть файл базы
    if action == "send_db":
        db_name = (decision.get("db") or guessed_db or "").strip()
        if not db_name:
            await message.answer("⚠️ Укажите имя базы (например: <code>Test_number_1</code>).")
            return
        if not db_exists(db_name):
            await message.answer(f"❌ База <code>{db_name}</code> не найдена.")
            return
        stats = get_statistics(db_name)

        tmp_path: Optional[Path] = None
        try:
            if USE_SUPABASE:
                payload = json.dumps(load_db(db_name), ensure_ascii=False, indent=2)
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    suffix=f".{normalize_db_key(db_name)}.json",
                    delete=False,
                ) as tmp:
                    tmp.write(payload)
                    tmp.flush()
                    tmp_path = Path(tmp.name)
                path = tmp_path
            else:
                path = get_db_path(db_name)

            await send_local_file(
                message,
                path,
                caption=(
                    f"📁 База: <code>{db_name}</code>\n"
                    f"📊 Записей: {stats.get('total_records', 0)} | "
                    f"💾 {format_file_size(int(stats.get('file_size', 0)))}"
                ),
            )
        finally:
            if tmp_path:
                try:
                    tmp_path.unlink(missing_ok=True)
                except OSError:
                    pass
        return

    # 3) Скинуть исходник
    if action == "send_source":
        name = (decision.get("file") or "").strip()
        path = _get_source_file_path(name)
        if not path or not path.exists():
            await message.answer("❌ Можно отправить только <code>base.py</code> или <code>bot.py</code>.")
            return
        await send_local_file(message, path, caption=f"📄 Файл: <code>{html.escape(path.name)}</code>")
        return

    # 4) Удалить базу (только после подтверждения)
    if action == "delete_db":
        db_name = (decision.get("db") or guessed_db or "").strip()
        if not db_name:
            await message.answer("⚠️ Укажите имя базы для удаления (например: <code>1</code> или <code>Test_number_1</code>).")
            return
        if not db_exists(db_name):
            await message.answer(f"❌ База <code>{db_name}</code> не найдена.")
            return

        stats = get_statistics(db_name)
        await state.update_data(pending_db_name=db_name)
        await state.set_state(AskAI.confirming)
        await message.answer(
            "⚠️ <b>ИИ просит подтвердить удаление базы целиком</b>\n\n"
            f"📁 База: <code>{db_name}</code>\n"
            f"📊 Записей: <b>{stats.get('total_records', 0)}</b>\n"
            f"💾 Размер: <b>{format_file_size(int(stats.get('file_size', 0)))}</b>\n\n"
            "Подтвердить?",
            reply_markup=confirm_ai_delete_db_keyboard(db_name),
        )
        return

    # 5) Обычный ответ
    messages = [
        {"role": "system", "content": "Ты — помощник Telegram-бота. Отвечай кратко и по делу на русском."},
        {"role": "user", "content": query},
    ]
    try:
        resp = await _groq_chat_completion(messages, temperature=0.5, max_tokens=700)
        content = resp["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"/ask: ошибка ответа: {e}")
        await message.answer("❌ Не удалось получить ответ от модели.", reply_markup=main_menu_keyboard())
        return

    await message.answer(content or "…", reply_markup=main_menu_keyboard())


# ─────────────────────────────────────────────────────────
# ДОБАВЛЕНИЕ ЗАПИСИ
# ─────────────────────────────────────────────────────────

@router.message(F.text == "➕ Добавить запись")
async def add_record_start(message: Message, state: FSMContext) -> None:
    """Начало процесса добавления записи."""
    if not is_allowed(message.from_user.id):
        return

    databases = get_all_databases()
    text = "➕ <b>Добавление записи</b>\n\nВведите имя базы данных (или выберите из списка):"

    if databases:
        await message.answer(text, reply_markup=cancel_keyboard())
        await message.answer(
            "📁 Существующие базы:",
            reply_markup=databases_keyboard(databases, action="select"),
        )
    else:
        await message.answer(
            text + "\n\n<i>Введите новое имя для создания базы:</i>",
            reply_markup=cancel_keyboard(),
        )

    await state.set_state(AddRecord.waiting_db_name)


@router.callback_query(F.data.startswith("db_select_"), StateFilter(AddRecord.waiting_db_name))
async def add_record_db_selected(callback: CallbackQuery, state: FSMContext) -> None:
    """Выбор базы из списка при добавлении."""
    db_name = callback.data.replace("db_select_", "")
    await state.update_data(db_name=db_name, saved_count=0, saved_ids=[])
    await state.set_state(AddRecord.waiting_content)

    await callback.message.edit_text(
        f"✅ Выбрана база: <code>{db_name}</code>\n\n"
        "📨 Теперь отправьте контент:\n"
        "• Текст, фото, видео, документ\n"
        "• Аудио, голосовое, стикер\n"
        "• Можно прикрепить подпись к медиа"
    )
    await callback.message.answer(
        f"Отправляйте сколько нужно в базу <code>{db_name}</code>. "
        f"Когда закончите — нажмите «{FINISH_ADD_SESSION_TEXT}».",
        reply_markup=add_session_keyboard(),
    )
    await callback.answer()


@router.message(StateFilter(AddRecord.waiting_db_name))
async def add_record_db_name(message: Message, state: FSMContext) -> None:
    """Получение имени базы данных от пользователя."""
    if message.text == "❌ Отмена":
        await cmd_cancel(message, state)
        return

    db_name = message.text.strip()
    if not db_name or len(db_name) > 64:
        await message.answer("⚠️ Имя базы должно быть от 1 до 64 символов. Попробуйте снова:")
        return

    await state.update_data(db_name=db_name, saved_count=0, saved_ids=[])
    await state.set_state(AddRecord.waiting_content)

    created = "создана" if not db_exists(db_name) else "выбрана"
    await message.answer(
        f"✅ База <code>{db_name}</code> {created}.\n\n"
        "📨 Отправьте контент для сохранения:\n"
        "• Текст, фото, видео, документ\n"
        "• Аудио, голосовое, видео-заметка, стикер\n"
        "• Медиа можно отправить с подписью",
        reply_markup=add_session_keyboard(),
    )


@router.message(StateFilter(AddRecord.waiting_content), F.text == FINISH_ADD_SESSION_TEXT)
async def add_record_finish(message: Message, state: FSMContext) -> None:
    """Завершение сессии добавления."""
    data = await state.get_data()
    db_name = data.get("db_name", "default")
    saved_count = int(data.get("saved_count") or 0)
    saved_ids: list[int] = data.get("saved_ids") or []

    await state.clear()

    if saved_count <= 0:
        await message.answer(
            f"✅ Сессия завершена. В базу <code>{db_name}</code> ничего не добавлено.",
            reply_markup=main_menu_keyboard(),
        )
        return

    ids_preview = ", ".join(map(str, saved_ids[-10:]))
    suffix = f" (последние 10 из {saved_count})" if saved_count > 10 else ""
    await message.answer(
        f"✅ <b>Готово!</b>\n"
        f"📂 База: <code>{db_name}</code>\n"
        f"📦 Добавлено записей: <b>{saved_count}</b>\n"
        f"🆔 ID: {ids_preview}{suffix}",
        reply_markup=main_menu_keyboard(),
    )


@router.message(
    StateFilter(AddRecord.waiting_content),
    ~F.text.in_({"❌ Отмена"}),
)
async def add_record_content(message: Message, state: FSMContext) -> None:
    """Получение контента и его сохранение в базу (много сообщений за сессию)."""
    if not is_allowed(message.from_user.id):
        return

    data = await state.get_data()
    db_name = data.get("db_name", "default")

    # Извлекаем данные из сообщения
    record_data = extract_record_data(message)

    if record_data.get("type") == "unknown":
        await message.answer(
            "⚠️ Неподдерживаемый тип контента. Попробуйте другой формат."
        )
        return

    # Сохраняем в БД
    record_id = add_record_to_db(db_name, record_data)
    saved_count = int(data.get("saved_count") or 0) + 1
    saved_ids: list[int] = data.get("saved_ids") or []
    saved_ids.append(int(record_id))
    if len(saved_ids) > 200:
        saved_ids = saved_ids[-200:]
    await state.update_data(saved_count=saved_count, saved_ids=saved_ids)

    # Формируем ответ
    type_emoji = {
        "text": "📝", "photo": "🖼", "video": "🎬",
        "document": "📄", "audio": "🎵", "voice": "🎤",
        "video_note": "⭕", "sticker": "🎭",
    }
    emoji = type_emoji.get(record_data["type"], "📦")

    success_text = (
        f"✅ <b>Сохранено!</b>\n\n"
        f"{emoji} Тип: <code>{record_data['type']}</code>\n"
        f"🆔 ID записи: <b>{record_id}</b>\n"
        f"📂 База: <code>{db_name}</code>\n"
        f"🕒 Время: {record_data['timestamp']}"
    )
    if record_data.get("text") and record_data["type"] == "text":
        preview = record_data["text"][:80]
        if len(record_data["text"]) > 80:
            preview += "..."
        success_text += f"\n📄 Текст: {preview}"

    # Для медиа — эхо, чтобы сразу было видно, что сохранилось
    if record_data.get("type") != "text" and record_data.get("file_id"):
        echo_record = dict(record_data)
        echo_record["_trusted_html"] = True
        original_caption = (record_data.get("text") or "").strip()
        caption_prefix = f"✅ Сохранено как запись <b>#{record_id}</b>"
        if original_caption:
            original_caption = html.escape(original_caption[:900])
            echo_record["text"] = f"{caption_prefix}\n\n{original_caption}"
        else:
            echo_record["text"] = caption_prefix
        await send_record_content(message, echo_record)

    await message.answer(
        success_text + f"\n\nОтправляйте ещё или нажмите «{FINISH_ADD_SESSION_TEXT}».",
        reply_markup=add_session_keyboard(),
    )


# ─────────────────────────────────────────────────────────
# ПРОСМОТР БАЗЫ
# ─────────────────────────────────────────────────────────

@router.message(F.text == "📂 Открыть базу")
async def open_base_start(message: Message, state: FSMContext) -> None:
    """Начало просмотра базы данных."""
    if not is_allowed(message.from_user.id):
        return

    databases = get_all_databases()
    if not databases:
        await message.answer(
            "📭 Нет ни одной базы данных.\nСначала добавьте запись!",
            reply_markup=main_menu_keyboard(),
        )
        return

    await state.set_state(OpenBase.waiting_db_name)
    await message.answer(
        "📂 <b>Открыть базу</b>\n\nВыберите базу данных:",
        reply_markup=cancel_keyboard(),
    )
    await message.answer(
        "📁 Доступные базы:",
        reply_markup=databases_keyboard(databases, action="browse"),
    )


@router.callback_query(F.data.startswith("db_browse_"), StateFilter(OpenBase.waiting_db_name))
async def open_base_selected(callback: CallbackQuery, state: FSMContext) -> None:
    """Открытие выбранной базы данных."""
    db_name = callback.data.replace("db_browse_", "")
    await state.update_data(db_name=db_name, page=1)
    await state.set_state(OpenBase.browsing)

    await callback.message.edit_text(f"📂 Открываю базу <code>{db_name}</code>...")
    await callback.answer()
    await show_records_page(callback.message, db_name, page=1)


@router.callback_query(F.data.startswith("browse_page_"), StateFilter(OpenBase.browsing))
async def browse_page_callback(callback: CallbackQuery, state: FSMContext) -> None:
    """Переход по страницам (без отправки файлов)."""
    data = await state.get_data()
    db_name = data.get("db_name", "")
    try:
        page = int(callback.data.replace("browse_page_", ""))
    except ValueError:
        await callback.answer("⚠️ Некорректная страница", show_alert=False)
        return

    await state.update_data(page=page)
    await show_records_page(callback.message, db_name, page=page)
    await callback.answer()


@router.callback_query(F.data.startswith("browse_open_"), StateFilter(OpenBase.browsing))
async def browse_open_record(callback: CallbackQuery, state: FSMContext) -> None:
    """Открыть карточку записи (без автосенд медиа)."""
    data = await state.get_data()
    db_name = data.get("db_name", "")
    try:
        record_id = int(callback.data.replace("browse_open_", ""))
    except ValueError:
        await callback.answer("⚠️ Некорректный ID", show_alert=False)
        return

    record = get_record_by_id(db_name, record_id)
    if not record:
        await callback.answer("❌ Запись не найдена", show_alert=False)
        return

    info = format_record_info(record)
    if record.get("type") == "text":
        full_text = (record.get("text") or "").strip()
        if len(full_text) > 3500:
            full_text = full_text[:3500] + "…"
        info = info + "\n\n" + f"<b>Текст:</b>\n<code>{html.escape(full_text)}</code>"
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="browse_back")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="go_home")],
        ])
    else:
        keyboard = browse_record_keyboard(record_id)

    await _edit_or_send(callback.message, info, reply_markup=keyboard)
    await callback.answer()


@router.callback_query(F.data == "browse_back", StateFilter(OpenBase.browsing))
async def browse_back_to_list(callback: CallbackQuery, state: FSMContext) -> None:
    """Вернуться к списку записей."""
    data = await state.get_data()
    db_name = data.get("db_name", "")
    page = int(data.get("page") or 1)
    await show_records_page(callback.message, db_name, page=page)
    await callback.answer()


@router.callback_query(F.data.startswith("browse_send_"), StateFilter(OpenBase.browsing))
async def browse_send_record(callback: CallbackQuery, state: FSMContext) -> None:
    """Отправить контент записи по кнопке."""
    data = await state.get_data()
    db_name = data.get("db_name", "")
    try:
        record_id = int(callback.data.replace("browse_send_", ""))
    except ValueError:
        await callback.answer("⚠️ Некорректный ID", show_alert=False)
        return

    record = get_record_by_id(db_name, record_id)
    if not record:
        await callback.answer("❌ Запись не найдена", show_alert=False)
        return

    await send_record_content(callback, record)
    await callback.answer("✅ Отправлено", show_alert=False)


@router.message(StateFilter(OpenBase.waiting_db_name))
async def open_base_manual_name(message: Message, state: FSMContext) -> None:
    """Ручной ввод имени базы."""
    if message.text == "❌ Отмена":
        await cmd_cancel(message, state)
        return

    db_name = message.text.strip()
    if not db_exists(db_name):
        await message.answer(f"❌ База <code>{db_name}</code> не найдена. Попробуйте другое имя:")
        return

    await state.update_data(db_name=db_name, page=1)
    await state.set_state(OpenBase.browsing)
    await show_records_page(message, db_name, page=1)


# ─────────────────────────────────────────────────────────
# ПОИСК
# ─────────────────────────────────────────────────────────

@router.message(F.text == "🔍 Поиск")
async def search_start(message: Message, state: FSMContext) -> None:
    """Начало поиска."""
    if not is_allowed(message.from_user.id):
        return

    databases = get_all_databases()
    if not databases:
        await message.answer("📭 Нет баз для поиска.", reply_markup=main_menu_keyboard())
        return

    await state.set_state(SearchRecords.waiting_db_name)
    await message.answer(
        "🔍 <b>Поиск по базе</b>\n\nВыберите базу для поиска:",
        reply_markup=cancel_keyboard(),
    )
    await message.answer(
        "📁 Базы данных:",
        reply_markup=databases_keyboard(databases, action="search"),
    )


@router.callback_query(F.data.startswith("db_search_"), StateFilter(SearchRecords.waiting_db_name))
async def search_db_selected(callback: CallbackQuery, state: FSMContext) -> None:
    """Выбор базы для поиска."""
    db_name = callback.data.replace("db_search_", "")
    await state.update_data(db_name=db_name)
    await state.set_state(SearchRecords.waiting_query)

    await callback.message.edit_text(
        f"🔍 Поиск в базе <code>{db_name}</code>\n\n"
        "Введите поисковый запрос:"
    )
    await callback.answer()


@router.message(StateFilter(SearchRecords.waiting_db_name))
async def search_db_manual(message: Message, state: FSMContext) -> None:
    """Ручной ввод имени базы для поиска."""
    if message.text == "❌ Отмена":
        await cmd_cancel(message, state)
        return

    db_name = message.text.strip()
    if not db_exists(db_name):
        await message.answer(f"❌ База <code>{db_name}</code> не найдена.")
        return

    await state.update_data(db_name=db_name)
    await state.set_state(SearchRecords.waiting_query)
    await message.answer(
        f"🔍 Поиск в базе <code>{db_name}</code>\n\nВведите поисковый запрос:"
    )


@router.message(StateFilter(SearchRecords.waiting_query))
async def search_execute(message: Message, state: FSMContext) -> None:
    """Выполнение поиска."""
    if message.text == "❌ Отмена":
        await cmd_cancel(message, state)
        return

    data = await state.get_data()
    db_name = data.get("db_name", "")
    query = message.text.strip()

    if not query:
        await message.answer("⚠️ Введите непустой запрос.")
        return

    results = search_records(db_name, query)

    if not results:
        await message.answer(
            f"🔍 По запросу «<b>{query}</b>» ничего не найдено.",
            reply_markup=main_menu_keyboard(),
        )
        await state.clear()
        return

    await message.answer(
        f"🔍 Найдено <b>{len(results)}</b> записей по запросу «{query}»:",
        reply_markup=main_menu_keyboard(),
    )

    # Показываем найденные записи (максимум 10)
    for record in results[:10]:
        info_text = format_record_info(record)
        await message.answer(info_text)
        if record.get("type") != "text" and record.get("file_id"):
            await send_record_content(message, record)

    if len(results) > 10:
        await message.answer(f"... и ещё {len(results) - 10} записей. Уточните запрос.")

    await state.clear()


# ─────────────────────────────────────────────────────────
# ПОЛУЧЕНИЕ ЗАПИСИ ПО ID
# ─────────────────────────────────────────────────────────

@router.message(F.text == "🖼 Получить запись")
async def get_media_start(message: Message, state: FSMContext) -> None:
    """Начало получения записи по ID."""
    if not is_allowed(message.from_user.id):
        return

    databases = get_all_databases()
    if not databases:
        await message.answer("📭 Нет баз данных.", reply_markup=main_menu_keyboard())
        return

    await state.set_state(GetMedia.waiting_db_name)
    await message.answer(
        "🖼 <b>Получить запись</b>\n\nВыберите базу данных:",
        reply_markup=cancel_keyboard(),
    )
    await message.answer(
        "📁 Базы данных:",
        reply_markup=databases_keyboard(databases, action="get"),
    )


@router.callback_query(F.data.startswith("db_get_"), StateFilter(GetMedia.waiting_db_name))
async def get_media_db_selected(callback: CallbackQuery, state: FSMContext) -> None:
    """Выбор базы для получения записи."""
    db_name = callback.data.replace("db_get_", "")
    await state.update_data(db_name=db_name)
    await state.set_state(GetMedia.waiting_record_id)

    await callback.message.edit_text(
        f"🖼 База: <code>{db_name}</code>\n\n"
        "Введите ID записи (число):"
    )
    await callback.answer()


@router.message(StateFilter(GetMedia.waiting_db_name))
async def get_media_db_manual(message: Message, state: FSMContext) -> None:
    """Ручной ввод базы."""
    if message.text == "❌ Отмена":
        await cmd_cancel(message, state)
        return

    db_name = message.text.strip()
    if not db_exists(db_name):
        await message.answer(f"❌ База <code>{db_name}</code> не найдена.")
        return

    await state.update_data(db_name=db_name)
    await state.set_state(GetMedia.waiting_record_id)
    await message.answer(
        f"🖼 База: <code>{db_name}</code>\n\nВведите ID записи:"
    )


@router.message(StateFilter(GetMedia.waiting_record_id))
async def get_media_execute(message: Message, state: FSMContext) -> None:
    """Получение и отправка записи."""
    if message.text == "❌ Отмена":
        await cmd_cancel(message, state)
        return

    data = await state.get_data()
    db_name = data.get("db_name", "")

    try:
        record_id = int(message.text.strip())
    except ValueError:
        await message.answer("⚠️ Введите числовой ID записи.")
        return

    record = get_record_by_id(db_name, record_id)
    if not record:
        await message.answer(
            f"❌ Запись #{record_id} не найдена в базе <code>{db_name}</code>."
        )
        return

    # Отправляем информацию и контент
    await message.answer(format_record_info(record), reply_markup=main_menu_keyboard())
    await send_record_content(message, record)
    await state.clear()


# ─────────────────────────────────────────────────────────
# УДАЛЕНИЕ ЗАПИСИ
# ─────────────────────────────────────────────────────────

@router.message(F.text == "🗑 Удалить запись")
async def delete_record_start(message: Message, state: FSMContext) -> None:
    """Начало удаления записи."""
    if not is_allowed(message.from_user.id):
        return

    databases = get_all_databases()
    if not databases:
        await message.answer("📭 Нет баз данных.", reply_markup=main_menu_keyboard())
        return

    await state.set_state(DeleteRecord.waiting_db_name)
    await message.answer(
        "🗑 <b>Удалить запись</b>\n\nВыберите базу данных:",
        reply_markup=cancel_keyboard(),
    )
    await message.answer(
        "📁 Базы данных:",
        reply_markup=databases_keyboard(databases, action="del"),
    )


@router.callback_query(F.data.startswith("db_del_"), StateFilter(DeleteRecord.waiting_db_name))
async def delete_db_selected(callback: CallbackQuery, state: FSMContext) -> None:
    """Выбор базы для удаления."""
    db_name = callback.data.replace("db_del_", "")
    await state.update_data(db_name=db_name)
    await state.set_state(DeleteRecord.waiting_record_id)

    await callback.message.edit_text(
        f"🗑 База: <code>{db_name}</code>\n\n"
        "Введите ID записи для удаления:"
    )
    await callback.answer()


@router.message(StateFilter(DeleteRecord.waiting_db_name))
async def delete_db_manual(message: Message, state: FSMContext) -> None:
    """Ручной ввод базы для удаления."""
    if message.text == "❌ Отмена":
        await cmd_cancel(message, state)
        return

    db_name = message.text.strip()
    if not db_exists(db_name):
        await message.answer(f"❌ База <code>{db_name}</code> не найдена.")
        return

    await state.update_data(db_name=db_name)
    await state.set_state(DeleteRecord.waiting_record_id)
    await message.answer(
        f"🗑 База: <code>{db_name}</code>\n\nВведите ID записи для удаления:"
    )


@router.message(StateFilter(DeleteRecord.waiting_record_id))
async def delete_record_id_received(message: Message, state: FSMContext) -> None:
    """Получение ID записи и запрос подтверждения."""
    if message.text == "❌ Отмена":
        await cmd_cancel(message, state)
        return

    data = await state.get_data()
    db_name = data.get("db_name", "")

    try:
        record_id = int(message.text.strip())
    except ValueError:
        await message.answer("⚠️ Введите числовой ID записи.")
        return

    record = get_record_by_id(db_name, record_id)
    if not record:
        await message.answer(
            f"❌ Запись #{record_id} не найдена в базе <code>{db_name}</code>."
        )
        return

    await state.update_data(record_id=record_id)
    await state.set_state(DeleteRecord.confirming)

    info = format_record_info(record)
    await message.answer(
        f"⚠️ <b>Подтвердите удаление:</b>\n\n{info}",
        reply_markup=confirm_delete_keyboard(record_id),
    )


@router.callback_query(F.data.startswith("delete_yes_"), StateFilter(DeleteRecord.confirming))
async def delete_confirm_yes(callback: CallbackQuery, state: FSMContext) -> None:
    """Подтверждение удаления записи."""
    data = await state.get_data()
    db_name = data.get("db_name", "")
    record_id = data.get("record_id")

    success = delete_record_by_id(db_name, record_id)

    if success:
        await callback.message.edit_text(
            f"✅ Запись #{record_id} удалена из базы <code>{db_name}</code>."
        )
    else:
        await callback.message.edit_text(
            f"❌ Не удалось удалить запись #{record_id}."
        )

    await callback.answer()
    await state.clear()
    await callback.message.answer("🏠 Главное меню:", reply_markup=main_menu_keyboard())


@router.callback_query(F.data == "delete_no")
async def delete_confirm_no(callback: CallbackQuery, state: FSMContext) -> None:
    """Отмена удаления."""
    await callback.message.edit_text("❌ Удаление отменено.")
    await callback.answer()
    await state.clear()
    await callback.message.answer("🏠 Главное меню:", reply_markup=main_menu_keyboard())


# ─────────────────────────────────────────────────────────
# УДАЛЕНИЕ БАЗЫ (ФАЙЛА ЦЕЛИКОМ)
# ─────────────────────────────────────────────────────────

@router.message(F.text == "🧨 Удалить базу")
async def delete_database_start(message: Message, state: FSMContext) -> None:
    if not is_allowed(message.from_user.id):
        return

    databases = get_all_databases()
    if not databases:
        await message.answer("📭 Нет баз данных.", reply_markup=main_menu_keyboard())
        return

    await state.set_state(DeleteDatabase.waiting_db_name)
    await message.answer(
        "🧨 <b>Удалить базу целиком</b>\n\nВыберите базу данных:",
        reply_markup=cancel_keyboard(),
    )
    await message.answer(
        "📁 Базы данных:",
        reply_markup=databases_keyboard(databases, action="drop"),
    )


@router.callback_query(F.data.startswith("db_drop_"), StateFilter(DeleteDatabase.waiting_db_name))
async def delete_database_selected(callback: CallbackQuery, state: FSMContext) -> None:
    db_name = callback.data.replace("db_drop_", "")
    if not db_exists(db_name):
        await callback.answer("База не найдена", show_alert=True)
        return

    stats = get_statistics(db_name)
    await state.update_data(db_name=db_name)
    await state.set_state(DeleteDatabase.confirming)

    await callback.message.edit_text(
        "⚠️ <b>Подтвердите удаление базы целиком</b>\n\n"
        f"📁 База: <code>{db_name}</code>\n"
        f"📊 Записей: <b>{stats.get('total_records', 0)}</b>\n"
        f"💾 Размер: <b>{format_file_size(int(stats.get('file_size', 0)))}</b>\n\n"
        "Это действие удалит JSON-файл базы и все записи в нём.",
        reply_markup=confirm_delete_db_keyboard(db_name),
    )
    await callback.answer()


@router.message(StateFilter(DeleteDatabase.waiting_db_name))
async def delete_database_manual(message: Message, state: FSMContext) -> None:
    if message.text == "❌ Отмена":
        await cmd_cancel(message, state)
        return

    db_name = (message.text or "").strip()
    if not db_exists(db_name):
        await message.answer(f"❌ База <code>{db_name}</code> не найдена.")
        return

    stats = get_statistics(db_name)
    await state.update_data(db_name=db_name)
    await state.set_state(DeleteDatabase.confirming)
    await message.answer(
        "⚠️ <b>Подтвердите удаление базы целиком</b>\n\n"
        f"📁 База: <code>{db_name}</code>\n"
        f"📊 Записей: <b>{stats.get('total_records', 0)}</b>\n"
        f"💾 Размер: <b>{format_file_size(int(stats.get('file_size', 0)))}</b>\n\n"
        "Это действие удалит JSON-файл базы и все записи в нём.",
        reply_markup=confirm_delete_db_keyboard(db_name),
    )


@router.callback_query(F.data.startswith("dbdrop_yes_"), StateFilter(DeleteDatabase.confirming))
async def delete_database_confirm_yes(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    db_name = data.get("db_name", "")

    ok = delete_db_file(db_name)
    if ok:
        await callback.message.edit_text(f"✅ База <code>{db_name}</code> удалена.")
    else:
        await callback.message.edit_text(f"❌ Не удалось удалить базу <code>{db_name}</code>.")

    await callback.answer()
    await state.clear()
    await callback.message.answer("🏠 Главное меню:", reply_markup=main_menu_keyboard())


@router.callback_query(F.data == "dbdrop_no")
async def delete_database_confirm_no(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.message.edit_text("❌ Удаление базы отменено.")
    await callback.answer()
    await state.clear()
    await callback.message.answer("🏠 Главное меню:", reply_markup=main_menu_keyboard())


# ─────────────────────────────────────────────────────────
# ИЗМЕНЕНИЕ ЗАПИСИ
# ─────────────────────────────────────────────────────────

@router.message(F.text == "✏️ Изменить запись")
async def edit_record_start(message: Message, state: FSMContext) -> None:
    """Начало редактирования записи."""
    if not is_allowed(message.from_user.id):
        return

    databases = get_all_databases()
    if not databases:
        await message.answer("📭 Нет баз данных.", reply_markup=main_menu_keyboard())
        return

    await state.set_state(EditRecord.waiting_db_name)
    await message.answer(
        "✏️ <b>Изменить запись</b>\n\nВыберите базу данных:",
        reply_markup=cancel_keyboard(),
    )
    await message.answer(
        "📁 Базы данных:",
        reply_markup=databases_keyboard(databases, action="edit"),
    )


@router.callback_query(F.data.startswith("db_edit_"), StateFilter(EditRecord.waiting_db_name))
async def edit_db_selected(callback: CallbackQuery, state: FSMContext) -> None:
    """Выбор базы для редактирования."""
    db_name = callback.data.replace("db_edit_", "")
    await state.update_data(db_name=db_name)
    await state.set_state(EditRecord.waiting_record_id)

    await callback.message.edit_text(
        f"✏️ База: <code>{db_name}</code>\n\nВведите ID записи для изменения:"
    )
    await callback.answer()


@router.message(StateFilter(EditRecord.waiting_db_name))
async def edit_db_manual(message: Message, state: FSMContext) -> None:
    """Ручной ввод базы для редактирования."""
    if message.text == "❌ Отмена":
        await cmd_cancel(message, state)
        return

    db_name = message.text.strip()
    if not db_exists(db_name):
        await message.answer(f"❌ База <code>{db_name}</code> не найдена.")
        return

    await state.update_data(db_name=db_name)
    await state.set_state(EditRecord.waiting_record_id)
    await message.answer(
        f"✏️ База: <code>{db_name}</code>\n\nВведите ID записи для изменения:"
    )


@router.message(StateFilter(EditRecord.waiting_record_id))
async def edit_record_id_received(message: Message, state: FSMContext) -> None:
    """Получение ID записи для редактирования."""
    if message.text == "❌ Отмена":
        await cmd_cancel(message, state)
        return

    data = await state.get_data()
    db_name = data.get("db_name", "")

    try:
        record_id = int(message.text.strip())
    except ValueError:
        await message.answer("⚠️ Введите числовой ID записи.")
        return

    record = get_record_by_id(db_name, record_id)
    if not record:
        await message.answer(
            f"❌ Запись #{record_id} не найдена в базе <code>{db_name}</code>."
        )
        return

    await state.update_data(record_id=record_id)
    await state.set_state(EditRecord.waiting_new_text)

    current_text = record.get("text", "")
    await message.answer(
        f"✏️ Запись #{record_id}\n"
        f"Текущий текст: <i>{current_text or '(пусто)'}</i>\n\n"
        "Введите новый текст (или подпись для медиа):"
    )


@router.message(StateFilter(EditRecord.waiting_new_text))
async def edit_record_save(message: Message, state: FSMContext) -> None:
    """Сохранение нового текста записи."""
    if message.text == "❌ Отмена":
        await cmd_cancel(message, state)
        return

    data = await state.get_data()
    db_name = data.get("db_name", "")
    record_id = data.get("record_id")
    new_text = message.text.strip()

    success = update_record_text(db_name, record_id, new_text)

    if success:
        await message.answer(
            f"✅ Запись #{record_id} обновлена!\n"
            f"Новый текст: <i>{new_text}</i>",
            reply_markup=main_menu_keyboard(),
        )
    else:
        await message.answer(
            "❌ Не удалось обновить запись.",
            reply_markup=main_menu_keyboard(),
        )

    await state.clear()


# ─────────────────────────────────────────────────────────
# СТАТИСТИКА
# ─────────────────────────────────────────────────────────

@router.message(F.text == "📊 Статистика")
async def show_statistics(message: Message, state: FSMContext) -> None:
    """Показывает статистику по всем базам."""
    if not is_allowed(message.from_user.id):
        return

    databases = get_all_databases()
    if not databases:
        await message.answer("📭 Нет баз данных.", reply_markup=main_menu_keyboard())
        return

    total_records = sum(db["records_count"] for db in databases)
    total_size = sum(db["size"] for db in databases)

    lines = [
        "📊 <b>Общая статистика</b>\n",
        f"📁 Баз данных: <b>{len(databases)}</b>",
        f"📝 Всего записей: <b>{total_records}</b>",
        f"💾 Общий размер: <b>{format_file_size(total_size)}</b>\n",
        "─" * 25,
    ]

    for db in databases:
        # Загружаем детальную статистику
        stats = get_statistics(db["name"])
        type_str = ", ".join(
            f"{t}: {c}" for t, c in stats["type_counts"].items()
        ) or "нет записей"

        lines.extend([
            f"\n📄 <b>{db['name']}</b>",
            f"  📊 Записей: {db['records_count']}",
            f"  🗂 Типы: {type_str}",
            f"  💾 Размер: {format_file_size(db['size'])}",
            f"  🕒 Создана: {stats['created_at']}",
        ])

    await message.answer("\n".join(lines), reply_markup=main_menu_keyboard())


# ─────────────────────────────────────────────────────────
# СПИСОК БАЗ ДАННЫХ
# ─────────────────────────────────────────────────────────

@router.message(F.text == "📁 Список баз")
async def list_databases(message: Message) -> None:
    """Показывает список всех баз данных."""
    if not is_allowed(message.from_user.id):
        return

    databases = get_all_databases()
    if not databases:
        await message.answer(
            "📭 <b>Нет ни одной базы данных.</b>\n\n"
            "Используйте ➕ Добавить запись, чтобы создать первую базу.",
            reply_markup=main_menu_keyboard(),
        )
        return

    lines = [f"📁 <b>Базы данных ({len(databases)})</b>\n"]
    for i, db in enumerate(databases, 1):
        lines.append(
            f"{i}. 📄 <b>{db['name']}</b>\n"
            f"   📊 {db['records_count']} записей | "
            f"💾 {format_file_size(db['size'])}"
        )

    await message.answer("\n".join(lines), reply_markup=main_menu_keyboard())


# ─────────────────────────────────────────────────────────
# РЕЗЕРВНАЯ КОПИЯ ВСЕХ БАЗ (ZIP)
# ─────────────────────────────────────────────────────────

@router.message(F.text == BACKUP_ALL_BUTTON_TEXT)
async def backup_all_bases(message: Message, state: FSMContext) -> None:
    if not is_allowed(message.from_user.id):
        return

    await state.clear()
    await message.answer("📦 Собираю архив со всеми файлами из баз…")

    zip_path: Optional[Path] = None
    try:
        zip_path = await _build_bases_files_zip(bot)
        await message.answer_document(
            FSInputFile(str(zip_path)),
            caption="📦 Архив содержимого баз (backup)",
        )
    finally:
        if zip_path:
            try:
                zip_path.unlink(missing_ok=True)
            except OSError:
                pass


# ─────────────────────────────────────────────────────────
# CALLBACK ОБРАБОТЧИКИ (общие)
# ─────────────────────────────────────────────────────────

@router.callback_query(F.data == "noop")
async def noop_callback(callback: CallbackQuery) -> None:
    """Пустой обработчик для неактивных кнопок."""
    await callback.answer()


@router.callback_query(F.data == "go_home")
async def go_home_callback(callback: CallbackQuery, state: FSMContext) -> None:
    """Возврат в главное меню."""
    await state.clear()
    await callback.message.answer("🏠 Главное меню:", reply_markup=main_menu_keyboard())
    await callback.answer()


# ─────────────────────────────────────────────────────────
# ОБРАБОТЧИК НЕИЗВЕСТНЫХ СООБЩЕНИЙ
# ─────────────────────────────────────────────────────────

@router.message()
async def unknown_message(message: Message, state: FSMContext) -> None:
    """Обрабатывает сообщения вне контекста FSM."""
    current_state = await state.get_state()
    if current_state:
        # Если мы в каком-то состоянии — игнорируем неожиданные сообщения
        return

    await message.answer(
        "❓ Не понимаю команду.\n"
        "Используй кнопки меню или /help для справки.",
        reply_markup=main_menu_keyboard(),
    )


# ─────────────────────────────────────────────────────────
# ЗАПУСК БОТА
# ─────────────────────────────────────────────────────────

async def _render_http_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """
    Минимальный HTTP-ответ, чтобы Render (Web Service) видел открытый порт.
    Боту webhook не нужен — это только "keep-alive" endpoint.
    """
    try:
        try:
            await reader.read(1024)
        except Exception:
            pass

        body = b"OK"
        headers = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain; charset=utf-8\r\n"
            + f"Content-Length: {len(body)}\r\n".encode("ascii")
            + b"Connection: close\r\n"
            + b"\r\n"
        )
        writer.write(headers + body)
        await writer.drain()
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def start_render_server() -> asyncio.AbstractServer:
    port_s = os.environ.get("PORT", str(WEBAPP_PORT))
    try:
        port = int(port_s)
    except ValueError:
        port = WEBAPP_PORT
    return await asyncio.start_server(_render_http_handler, host=WEBAPP_HOST, port=port)


async def on_startup() -> None:
    """Действия при запуске бота."""
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    if USE_SUPABASE:
        _supabase_ensure_schema()
        ok = _supabase_healthcheck()
        logger.info(f"Supabase: {'OK' if ok else 'DOWN'} | table={SUPABASE_TABLE}")

    # Работаем только через polling — на всякий случай убираем webhook.
    await bot.delete_webhook(drop_pending_updates=True)
    bot_info = await bot.get_me()
    logger.info(f"Бот запущен: @{bot_info.username} (ID: {bot_info.id})")
    logger.info(f"Папка для баз данных: {BASE_DIR.resolve()}")


async def on_shutdown() -> None:
    """Действия при остановке бота."""
    logger.info("Бот остановлен.")
    try:
        await bot.delete_webhook()
    except Exception:
        pass
    await bot.session.close()


async def main() -> None:
    """Главная функция запуска."""
    # Регистрируем обработчики жизненного цикла
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    server = await start_render_server()
    sockets = server.sockets or []
    if sockets:
        try:
            port = sockets[0].getsockname()[1]
            logger.info(f"HTTP server listening on {WEBAPP_HOST}:{port}")
        except Exception:
            logger.info("HTTP server listening")

    try:
        logger.info("Запуск поллинга...")
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            drop_pending_updates=True,  # Игнорируем накопившиеся обновления при старте
        )
    finally:
        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
