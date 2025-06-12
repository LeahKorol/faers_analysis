import os
import logging
import sys

from src import (
    download_faers_data,
    deduplicate_faers_data,
    mark_data,
    get_demographic_data,
    summarize_demographic_data,
    report,
)

logger = logging.getLogger("FAERS")
logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"simple": {"format": "%(levelname)s: %(message)s"}},
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "stream": "ext://sys.stdout",
        }
    },
    "loggers": {"root": {"level": "DEBUG", "handlers": ["stdout"]}},
}


def main(
    *,
    year_q_from: str = "2020q1",
    year_q_to: str = "2022q3",
):
    """
    Run the pipeline.

    Args:
        year_q_from: The first quarter to process.
        year_q_to: The last quarter to process.
    """
    logging.basicConfig(level=logging.INFO)
    dir_data = "data"
    dir_external = os.path.join(dir_data, "external")
    dir_interim = os.path.join(dir_data, "interim")
    dir_processed = os.path.join(dir_data, "processed")
    config_dir = "config"

    # 1. Download
    logging.info("[Pipeline] Downloading data...")
    download_dir = os.path.join(dir_external, "faers")
    os.makedirs(download_dir, exist_ok=True)
    download_faers_data.main(
        year_q_from=year_q_from,
        year_q_to=year_q_to,
        dir_out=download_dir,
        threads=4,
    )
    # Optionally verify files here

    # 2. Deduplicate
    logging.info("[Pipeline] Deduplicating data...")
    dedup_dir = os.path.join(dir_interim, "faers_deduplicated")
    os.makedirs(dedup_dir, exist_ok=True)
    deduplicate_faers_data.main(
        dir_in=download_dir,
        dir_out=dedup_dir,
        threads=4,
    )

    # 3. Mark the data
    logging.info("[Pipeline] Marking data...")
    marked_dir = os.path.join(dir_interim, "marked_data_v2")
    os.makedirs(marked_dir, exist_ok=True)
    mark_data.main(
        year_q_from=year_q_from,
        year_q_to=year_q_to,
        dir_in=dedup_dir,
        config_dir=config_dir,
        dir_out=marked_dir,
        threads=7,
        clean_on_failure=True,
    )

    # 4. Demographic data
    logging.info("[Pipeline] Getting demographic data...")
    demography_dir = os.path.join(dir_interim, "demographic_analysis_v2")
    os.makedirs(demography_dir, exist_ok=True)
    try:
        get_demographic_data.main(
            year_q_from=year_q_from,
            year_q_to=year_q_to,
            dir_raw_data=dedup_dir,
            dir_marked_data=marked_dir,
            dir_config=config_dir,
            dir_out=demography_dir,
            threads=1,
            clean_on_failure=True,
        )
    except FileNotFoundError as e:
        logging.warning(f"[Pipeline] Demographic data step failed: {e}")

    # 5. Demographic summary
    logging.info("[Pipeline] Summarizing demographic data...")
    summary_dir = os.path.join(dir_interim, "demographic_summary_v2")
    os.makedirs(summary_dir, exist_ok=True)
    try:
        summarize_demographic_data.main(
            dir_demography_data=demography_dir,
            dir_config=config_dir,
            dir_out=summary_dir,
            clean_on_failure=True,
        )
    except Exception as e:
        logging.warning(f"[Pipeline] Demographic summary step failed: {e}")

    # 6. Report
    logging.info("[Pipeline] Generating report...")
    reports_dir = os.path.join(dir_processed, "reports")
    os.makedirs(reports_dir, exist_ok=True)
    try:
        report.main(
            dir_marked_data=marked_dir,
            dir_raw_data=dedup_dir,
            config_dir=config_dir,
            dir_reports=reports_dir,
            output_raw_exposure_data=True,
        )
    except Exception as e:
        logging.warning(f"[Pipeline] Report step failed: {e}")


if __name__ == "__main__":
    import defopt

    defopt.run(main)
