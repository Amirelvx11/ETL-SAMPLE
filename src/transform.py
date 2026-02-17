import uuid
import math
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime, timezone
from backend_toolkit.logger import get_logger
from src.config import USER_GUID, PART_ID_BY_PREFIX

logger = get_logger("transform")


# -------------- HELPERS --------------
def _parse_datetime(value) -> datetime | None:
    if value is None:
        return None

    # MySQL zero-date guards
    if isinstance(value, str):
        s = value.strip()
        if not s or s.startswith(("0000-00-00", "2000-00-00")):
            return None

    try:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.to_pydatetime().replace(tzinfo=None, microsecond=0)
    except Exception:
        return None


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


# -------------- TRANSFORM LAYER --------------
def transform_tamper_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    rows: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    
    for r in df.itertuples(index=False):
        try:
            row = {
                "Id": str(uuid.uuid4()).upper(),
                "IsActive": 1,
                "CreatedBy": str(USER_GUID),
                "CreatedOn": now,
                "ModifiedBy": str(USER_GUID),
                "ModifiedOn": now,
                "OwnerId": str(USER_GUID),
                "TamperLogId": int(r.TamperLogId),
                "Tusn": str(r.Tusn).strip(),
                "SerialNumber": str(r.SerialNumber).strip(),
                "TamperType": int(r.TamperType),
                "DisconnectTime": _parse_datetime(r.DisconnectTime),
                "ReconnectTime": _parse_datetime(r.ReconnectTime),
                "PartId": _resolve_part_id(str(r.SerialNumber).strip()),
            }

            rows.append(_clean_nan(row))

        except Exception as exc:
            logger.error(
                "tamper row transform failed",
                extra={
                    "tamper_log_id": getattr(r, "TamperLogId", None),
                    "error": str(exc),
                },
                exc_info=True,
            )
            continue

    return pd.DataFrame(rows)
