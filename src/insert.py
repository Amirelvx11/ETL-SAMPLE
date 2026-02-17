import pandas as pd
from backend_toolkit.logger import get_logger
from src.config import mssql_engine

logger = get_logger("insert")


def insert_tamper_logs(df: pd.DataFrame, run_id: str | None) -> int:
    if df.empty:
        return 0

    try:
        with mssql_engine.begin() as conn:
            df.to_sql(
                name="DeviceTamperLog",
                schema="mfu",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=500,
            )
        return len(df)
    except Exception as exc:
        logger.error(
            "tamper log insert failed",
            extra={"run_id": run_id, "row_count": len(df), "error": str(exc)},
            exc_info=True,
        )
        raise
