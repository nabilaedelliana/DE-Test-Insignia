import json
import logging
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from q1_pipeline.config import PIPELINE_NAME, SPARK_MASTER


def create_spark_session() -> SparkSession:
    # Create and configure the Spark session used by the pipeline.

    return (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(PIPELINE_NAME)
        .getOrCreate()
    )


def configure_logging() -> None:
    # Configure application logging.

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )



def save_watermark(
    state_file: Path,
    pipeline_name: str,
    watermark,
) -> None:
    """
    Persist the last successful watermark.

    The watermark should only be saved after the pipeline
    has completed successfully.
    """

    state_file.parent.mkdir(parents=True, exist_ok=True)

    state = {}

    if state_file.exists():
        with state_file.open("r", encoding="utf-8") as file:
            state = json.load(file)

    state[pipeline_name] = {
        "last_successful_watermark": watermark.isoformat(),
    }

    with state_file.open("w", encoding="utf-8") as file:
        json.dump(state, file, indent=2)

    logging.getLogger(__name__).info(
        "Watermark saved: pipeline=%s watermark=%s",
        pipeline_name,
        watermark,
    )




def load_watermark(
    state_file: Path,
    pipeline_name: str,
):
    """
    Load the last successfully processed watermark.

    Returns None when no previous successful watermark exists.
    """

    if not state_file.exists():
        logging.getLogger(__name__).info(
            "No watermark state found for pipeline=%s",
            pipeline_name,
        )
        return None

    with state_file.open("r", encoding="utf-8") as file:
        state = json.load(file)

    pipeline_state = state.get(pipeline_name)

    if not pipeline_state:
        logging.getLogger(__name__).info(
            "No watermark found for pipeline=%s",
            pipeline_name,
        )
        return None

    watermark_value = pipeline_state.get("last_successful_watermark")

    if not watermark_value:
        return None

    watermark = datetime.fromisoformat(watermark_value)

    logging.getLogger(__name__).info(
        "Loaded watermark: pipeline=%s watermark=%s",
        pipeline_name,
        watermark,
    )

    return watermark
