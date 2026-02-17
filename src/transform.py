import uuid
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime, timezone
from backend_toolkit.logger import get_logger
from src.config import (
    _parse_datetime, _resolve_part_id,
    _clean_nan, USER_GUID
)

logger = get_logger("transform")


def transform_tamper_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    rows: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    
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
                    "row": r._asdict(),
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise

    return pd.DataFrame(rows).convert_dtypes()
