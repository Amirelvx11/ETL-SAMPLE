import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from backend_toolkit.logger import get_logger
from main import run_etl

logger = get_logger("scheduler")

IRAN = ZoneInfo("Asia/Tehran")

CHECK_INTERVAL_SECONDS = 10
ALLOWED_START_HOUR = 8
ALLOWED_END_HOUR = 22

def is_inside_window(now: datetime) -> bool:
    return ALLOWED_START_HOUR <= now.hour <= ALLOWED_END_HOUR

def validate_env() -> None:
    required_vars = [
        "SOURCE_DB",
        "TARGET_DB",
        "BT_MONGO_URI",
        "BT_MONGO_DB",
        "BT_MONGO_COLLECTION",
    ]

    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        logger.critical(
            "missing required environment variables",
            extra={"missing_vars": missing},
        )
        raise RuntimeError(f"Missing env vars: {missing}")
    
def main() -> None:
    
    validate_env()
    logger.info(
        "scheduler started",
        extra={
            "interval_seconds": CHECK_INTERVAL_SECONDS,
            "timezone": "Asia/Tehran",
        },
    )

    while True:
        start = time.time()
        now = datetime.now(IRAN)
        
        try:
            if is_inside_window(now):
                run_etl()
        except Exception:
            logger.exception("ETL execution failed")

        elapsed = time.time() - start
        time.sleep(max(0, CHECK_INTERVAL_SECONDS - elapsed))


if __name__ == "__main__":
    main()
