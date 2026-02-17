import os
import math
from typing import Any
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

# ----------- HELPERS -----------
def _parse_datetime(value) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.replace(tzinfo=None, microsecond=0)

    s = str(value).strip()

    if "." in s:
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            pass

    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def _resolve_part_id(serial: str | None) -> str | None:
    if not serial:
        return None
    for prefix, guid in PART_ID_BY_PREFIX.items():
        if serial.startswith(prefix):
            return guid
    return None


def _clean_nan(row: dict[str, Any]) -> dict[str, Any]:
    for k, v in row.items():
        if isinstance(v, float) and math.isnan(v):
            row[k] = None
    return row

# ----------- ENVIRONMENT VARIABLES -----------

def _require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise RuntimeError(f"Missing required env var: {key}")
    return value

# REQUIRED ENV 
SOURCE_DB = _require_env("SOURCE_DB")
TARGET_DB = _require_env("TARGET_DB")
USER_GUID = _require_env("USER_GUID")

# OPTIONAL (ensure that no limitation based on mssql and your RAM for insert)
BATCH_SIZE = min(int(os.getenv("BATCH_SIZE", "10000")), 50_000)

# Engines (MySQL-source and MSSQL-target)
mysql_engine = create_engine(
    SOURCE_DB,
    pool_pre_ping=True,
    pool_recycle=3600,
)
mssql_engine = create_engine(
    TARGET_DB,
    pool_pre_ping=True,
    fast_executemany=True,
    pool_size=5,
    max_overflow=5,
    pool_timeout=30,
    pool_recycle=3600,
    isolation_level="READ COMMITTED",
)


PART_ID_BY_PREFIX = {
    "00": "A3925DD2-F7C3-4E27-B487-E547F8F980E2",
    "05": "B159B8DA-AD61-4C25-97C8-C82CF7955D06",
}

