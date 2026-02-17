from backend_toolkit.monitoring.run_tracker import RunTracker
from backend_toolkit.logger import get_logger
from src.config import BATCH_SIZE
from src.fetch import get_last_inserted_tamper_id, fetch_new_tamper_logs
from src.insert import transform_tamper_rows, insert_tamper_logs

logger = get_logger("main-etl")


def tamper_logs_etl() -> int:
    with RunTracker("device-log-etl") as run:
        run_id = run.run_id

        last_id = get_last_inserted_tamper_id(run_id)
        total_inserted = 0
        batches = 0

        while True:
            df = fetch_new_tamper_logs(last_id, run_id)

            if df.empty:
                break

            batches += 1
            last_id = int(df["TamperLogId"].max())

            df = transform_tamper_rows(df)
            total_inserted += insert_tamper_logs(df, run_id)

            if len(df) < BATCH_SIZE:
                break

        logger.info(
            "tamper-log etl finished",
            extra={
                "run_id": run_id,
                "batches": batches,
                "rows_inserted": total_inserted,
                "final_last_tamper_log_id": last_id,
            },
        )

        return total_inserted


if __name__ == "__main__":
    tamper_logs_etl()
