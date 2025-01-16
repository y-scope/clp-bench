import argparse
import logging
import traceback

from .executor import BenchmarkingMode, BenchmarkingSystemMetric, ClpBenchExecutor
from .version import VERSION

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging_console_handler = logging.StreamHandler()
logging_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
logging_console_handler.setFormatter(logging_formatter)
logger.addHandler(logging_console_handler)


def load_benchmarking_assets(assets_path: str) -> ClpBenchExecutor:
    """This function loads the benchmarking assets of the tool to benchmark, including container
    configurations and benchmarking scripts

    :param assets_path: A string of the path to the assets
    :return: An executor instance
    """

    logger.info(f"Loading benchmarking assets from: {assets_path}")
    return ClpBenchExecutor(assets_path)


def ingest(clp_bench_executor: ClpBenchExecutor):
    """This function is invoked when work mode is "ingest"

    :param clp_bench_executor: The executor instance
    """

    logger.info("Ingesting data")
    try:
        clp_bench_executor.start_polling_system_metric(
            BenchmarkingSystemMetric.MEMORY, BenchmarkingMode.INGEST_MODE
        )
        clp_bench_executor.launch(BenchmarkingMode.INGEST_MODE)
        clp_bench_executor.ingest(BenchmarkingMode.INGEST_MODE)
    except Exception as e:
        logger.error(f"Failed to ingest in {BenchmarkingMode.INGEST_MODE.value} mode : {e}")
    finally:
        clp_bench_executor.stop_polling_system_metric(
            BenchmarkingSystemMetric.MEMORY, BenchmarkingMode.INGEST_MODE
        )
        try:
            clp_bench_executor.terminate()
        except Exception as e:
            logger.error(
                f"Failed to finish benchmark in {BenchmarkingMode.INGEST_MODE.value} mode: {e}"
            )


def run_query_benchmark(
    clp_bench_executor: ClpBenchExecutor, mode: BenchmarkingMode = BenchmarkingMode.HOT_RUN_MODE
):
    """This functions runs the query benchmark, where the queries are from the yaml file in assets

    :param clp_bench_executor: The executor instance
    :param mode: The work mode (could be one of "cold" or "hot")
    """

    logger.info(f"Running benchmarking in {mode.value} mode")
    try:
        clp_bench_executor.start_polling_system_metric(BenchmarkingSystemMetric.MEMORY, mode)
        # Note that no deployment here, it assumes finished ingestion.
        clp_bench_executor.launch(mode)
        clp_bench_executor.run_query_benchmark(mode)
    except Exception as e:
        logger.error(f"Failed to run benchmark in {mode.value} mode: {e}")
    finally:
        clp_bench_executor.stop_polling_system_metric(BenchmarkingSystemMetric.MEMORY, mode)
        try:
            clp_bench_executor.terminate()
        except Exception as e:
            logger.error(f"Failed to finish benchmark in {mode.value} mode: {e}")


def main():
    description = "CLP Bench--An out-of-the-box benchmarking framework."
    # Command line arguments parsing
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-a", "--asset", type=str, required=True, help="The benchmarking asset location"
    )
    parser.add_argument(
        "-m",
        "--mode",
        type=str,
        choices=["cold", "hot", "ingest"],
        required=True,
        help="The benchmarking mode",
    )
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument("-v", "--version", action="version", version=f"{VERSION}")
    args = parser.parse_args()
    logger.info(f"The benchmarking asset location: {args.asset}")
    logger.info(f"The benchmarking mode: {args.mode}")

    if args.debug:
        logger.info("Enable DEBUG mode")
        logger.setLevel(logging.DEBUG)

    # Load corresponding implementation for executor's SPI
    try:
        clp_bench_executor = load_benchmarking_assets(args.asset)
    except Exception as e:
        traceback.print_exc()
        logger.error(e)
        return

    # Cold run mode with cold cache benchmarking
    if "cold" == args.mode:
        run_query_benchmark(clp_bench_executor, BenchmarkingMode.COLD_RUN_MODE)

    # Hot run mode with warm cache benchmarking
    if "hot" == args.mode:
        run_query_benchmark(clp_bench_executor, BenchmarkingMode.HOT_RUN_MODE)

    # Only ingest the data
    if "ingest" == args.mode:
        ingest(clp_bench_executor)

    clp_bench_executor.visualize()
