import time
from backend_toolkit.logger import get_logger
from src.fetch import get_last_inserted_tamper_id, fetch_new_tamper_logs
from src.transform import transform_tamper_rows
from src.insert import insert_tamper_logs
from src.config import BATCH_SIZE

logger = get_logger("main-etl")


def run_etl():
    start_ts = time.monotonic()
    start_id = get_last_inserted_tamper_id()

    total_inserted = 0
    batches = 0
    last_processed_id = start_id

    try:
        while True:
            source_df = fetch_new_tamper_logs(last_processed_id)
            if source_df.empty:
                break

            batches += 1
            last_processed_id = int(source_df["TamperLogId"].max())

            transformed_df = transform_tamper_rows(source_df)
            total_inserted += insert_tamper_logs(transformed_df)

            if len(source_df) < BATCH_SIZE:
                break

        duration = round(time.monotonic() - start_ts, 3)
	if total_inserted > 0:
        	logger.info(
            		"tamper-log etl finished",
            		extra={
                		"batches": batches,
                		"inserted": total_inserted,
                		"start_tamper_log_id": start_id,
                		"last_tamper_log_id": last_processed_id,
                		"duration_sec": duration,
            		},
        	)
    except Exception as e:
        logger.critical(
            "main etl cycle error",
            extra={"error": str(e)},
        )
        raise


if __name__ == "__main__":
    run_etl()
