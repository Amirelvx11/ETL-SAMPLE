import pandas as pd
from sqlalchemy import text
from backend_toolkit.logger import get_logger
from src.config import mssql_engine, mysql_engine, BATCH_SIZE

logger = get_logger("fetch")

def get_last_inserted_tamper_id() -> int:
    try:
        with mssql_engine.connect() as conn:
            return int(
                conn.execute(
                    text("""
                        SELECT ISNULL(MAX(TamperLogId), 0)
                        FROM Hamon.mfu.DeviceTamperLog
                    """)
                ).scalar_one()
            )
    except Exception as exc:
        logger.error(
            "failed to load last tamper_log_id",
            extra={"error": str(exc)},
            exc_info=True,
        )
        raise


def fetch_new_tamper_logs(last_id: int) -> pd.DataFrame:
    sql = text(f"""
        SELECT
            id        AS TamperLogId,
            tusn      AS Tusn,
            sn        AS SerialNumber,
            type      AS TamperType,
            dis_time  AS DisconnectTime,
            ok_time   AS ReconnectTime
        FROM en_tms.szaf_dismounting_log
        WHERE id > :last_id
        ORDER BY id
        LIMIT {int(BATCH_SIZE)}
    """)

    try:
        with mysql_engine.connect() as conn:
            return pd.read_sql(sql, conn, params={"last_id": last_id})
    except Exception as exc:
        logger.error(
            "failed to fetch tamper logs",
            extra={"error": str(exc)},
            exc_info=True,
        )
        raise
