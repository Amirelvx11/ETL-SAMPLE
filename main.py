import time
from backend_toolkit.logger import get_logger
from src.fetch import get_last_inserted_tamper_id, fetch_new_tamper_logs
from src.transform import transform_tamper_rows
from src.insert import insert_tamper_logs
from src.config import BATCH_SIZE

logger = get_logger("main-etl")


def run_etl():
    try:
        start_ts = time.monotonic()
        start_id = get_last_inserted_tamper_id()
        total_inserted = 0
        batches = 0

        while True:
            df = fetch_new_tamper_logs(start_id)
            if df.empty:
                break

            batches += 1
            df = transform_tamper_rows(df)
            total_inserted += insert_tamper_logs(df)
            duration = round(time.monotonic() - start_ts, 3)
            
            if len(df) < BATCH_SIZE:
                break

        logger.info(
            "tamper-log etl finished",
            extra={
                "inserted": total_inserted,
                "batches": batches,
                "start_tamper_log_id": start_id,
                "last_tamper_log_id": int(df["TamperLogId"].max()),
                "duration_sec": duration,
            },
        )
    except Exception as e:
        logger.critical("main etl cycle error", extra={"error":e})
        raise


if __name__ == "__main__":
    run_etl()
