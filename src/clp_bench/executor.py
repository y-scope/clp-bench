from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List
import logging
import subprocess
import yaml
import threading
import statistics
import time


# Retrive logger
logger = logging.getLogger(__name__)


class BenchmarkingMode(Enum):
    """
    Benchmarking mode marcos.
    """

    HOT_RUN_MODE = "hot run"
    COLD_RUN_MODE = "cold run"
    QUERY_ONLY_RUN_MODE = "query only run"


class BenchmarkingStage(Enum):
    """
    Benchmarking stage marcos
    """

    INGEST = "ingest"
    RUN_QUERY_BENCHMARK = "run_query_benchmark"


class BenchmarkingSystemMetric(Enum):
    """
    Benchmarking system metric marcos
    """

    MEMORY = ("memory", "KB")


class BenchmarkingResult:
    """
    Benchmarking result data structure, for visualization.
    """

    def __init__(
        self, mode: str, compressed_size="", decompressed_size="", ratio="", ingest_e2e_latency=""
    ):
        self.mode: str = mode
        self.compressed_size: str = compressed_size
        self.decompressed_size: str = decompressed_size
        self.ratio: str = ratio
        self.ingest_e2e_latency: str = ingest_e2e_latency
        self.query_e2e_latencies = []

        class SystemMetricResult:
            def __init__(self, metric: BenchmarkingSystemMetric):
                self.metric = metric
                self.result_baseline = (
                    0  # The OS has used how much memory etc. 0: need baseline, -1: no baseline
                )
                self.stage_results: Dict[BenchmarkingStage, List] = {}
                for stage in BenchmarkingStage:
                    self.stage_results[stage] = []

        self.system_metric_results: Dict[BenchmarkingSystemMetric, SystemMetricResult] = {}
        for metric in BenchmarkingSystemMetric:
            self.system_metric_results[metric] = SystemMetricResult(metric)


class CPTExecutorBase(ABC):
    """
    Namespace for all essential CPT workflow steps. A base class.

    Different tools that to be benchmarked might need to implement
    their own executor based on this base class, which works in a
    SPI manner.
    """

    def __init__(self, config_path: str) -> None:
        super().__init__()
        self.config = None
        with open(config_path, "r") as config_file:
            self.config = yaml.safe_load(config_file)
            if self.config is None:
                raise Exception("Unable to parse " + config_path)
        # Results for different modes
        self.benchmarking_reseults: Dict[BenchmarkingMode, BenchmarkingResult] = {}
        for mode in BenchmarkingMode:
            self.benchmarking_reseults[mode] = BenchmarkingResult(mode)

        self.__overall_threading_event = threading.Event()

        class SystemMetricPoller:
            def __init__(self, metric: BenchmarkingSystemMetric):
                self.metric = metric
                self.thread: threading.Thread = None
                self.stage_alteration_notifier = threading.Event()
                self.stage_polling_intervals: Dict[BenchmarkingStage, int] = {}
                self.stage_events: Dict[BenchmarkingStage, threading.Event] = {}
                for stage in BenchmarkingStage:
                    self.stage_polling_intervals[stage] = 10
                    self.stage_events[stage] = threading.Event()

        self.__system_metric_pollers: Dict[BenchmarkingSystemMetric, SystemMetricPoller] = {}
        for metric in BenchmarkingSystemMetric:
            self.__system_metric_pollers[metric] = SystemMetricPoller(metric)

    # The following are some utils
    def _check_file_in_docker(self, container_id: str, file_path: str) -> None:
        try:
            subprocess.run(["docker", "exec", container_id, "test", "-f", file_path], check=True)
            logger.info(f"{file_path} exists in container {container_id}")
        except subprocess.CalledProcessError:
            raise Exception(f"{file_path} does not exist in container {container_id}")

    def _check_directory_in_docker(
        self, container_id: str, directory_path: str, need_to_create=True, need_to_clear=False
    ) -> None:
        try:
            subprocess.run(
                ["docker", "exec", container_id, "test", "-d", directory_path], check=True
            )
            logger.info(f"{directory_path} exists in {container_id}")
            if need_to_clear:
                logger.info(
                    f"Clearing existing stuff in {directory_path} in container {container_id}"
                )
                try:
                    # Note to myself: when running the command manually in a shell, the wildcard (*) is expanded
                    # by the shell to match files in the directory. However, when you run it through
                    # `subprocess.run`, there is no shell involved by default, so the wildcard (*) isn’t expanded
                    # and remains a literal *, which won’t work as expected. So the solution is to use `bash -c`
                    # to enable wildcard expansion.
                    subprocess.run(
                        [
                            "docker",
                            "exec",
                            container_id,
                            "bash",
                            "-c",
                            f"rm -rf {directory_path}/*",
                        ],
                        check=True,
                    )
                    logger.info(
                        f"All contents within {directory_path} cleared successfully in container {container_id}"
                    )
                except subprocess.CalledProcessError as e:
                    raise Exception(
                        f"Failed to clear {directory_path} contents in {container_id}: {e}"
                    )
        except subprocess.CalledProcessError as e1:
            if need_to_create:
                logger.info(f"{directory_path} does not exist in {container_id}, try to create one")
                try:
                    subprocess.run(
                        ["docker", "exec", container_id, "mkdir", "-p", directory_path], check=True
                    )
                    logger.info(
                        f"{directory_path} created successfully in container {container_id}"
                    )
                except subprocess.CalledProcessError as e2:
                    raise Exception(
                        f"{directory_path} failed to create in container {container_id}: {e2}"
                    )
            else:
                raise Exception(f"{directory_path} does not exist in {container_id}: {e1}")

    def _get_mem_usage_from_docker_stats(self, line: str) -> float:
        mem_usage = line.strip().split()[3]
        if "GiB" in mem_usage:
            return float(mem_usage.split("GiB")[0]) * 1024 * 1024
        elif "MiB" in mem_usage:
            return float(mem_usage.split("MiB")[0]) * 1024
        elif "KB" in mem_usage:
            return float(mem_usage.split("KB")[0])
        else:
            return float(mem_usage.split("B")[0]) / 1024

    def _execute_query(self, mode: BenchmarkingMode, command: str):
        wc_command = f"{command} | wc -l"
        logger.info(f"Executing command: {wc_command}")
        start_ts = time.perf_counter_ns()
        result = subprocess.run(
            wc_command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, shell=True, check=True
        )
        end_ts = time.perf_counter_ns()
        elapsed_time = (end_ts - start_ts) / 1e9
        nr_matched_log_lines = int(result.stdout.decode("utf-8").strip())
        logger.info(f"Number of matched log lines: {nr_matched_log_lines}")
        self.benchmarking_reseults[mode].query_e2e_latencies.append(f"{elapsed_time:.9f}s")

    def __set_thread_event_for_stage(self, stage: BenchmarkingStage):
        for it_stage in BenchmarkingStage:
            for it_metric in BenchmarkingSystemMetric:
                if stage != it_stage:
                    self.__system_metric_pollers[it_metric].stage_events[it_stage].clear()
                else:
                    self.__system_metric_pollers[it_metric].stage_events[it_stage].set()
                    self.__system_metric_pollers[it_metric].stage_alteration_notifier.set()
                    self.__system_metric_pollers[it_metric].stage_alteration_notifier.clear()

    def __unset_thread_event_after_stage(self, stage: BenchmarkingStage):
        for it_stage in BenchmarkingStage:
            for it_metric in BenchmarkingSystemMetric:
                if stage != it_stage:
                    self.__system_metric_pollers[it_metric].stage_events[it_stage].clear()
                else:
                    self.__system_metric_pollers[it_metric].stage_events[it_stage].clear()
                    self.__system_metric_pollers[it_metric].stage_alteration_notifier.set()
                    self.__system_metric_pollers[it_metric].stage_alteration_notifier.clear()

    # The following are the main SPI
    @abstractmethod
    def deploy(self, mode: BenchmarkingMode):
        pass

    @abstractmethod
    def ingest(self, mode: BenchmarkingMode):
        self.__set_thread_event_for_stage(BenchmarkingStage.INGEST)
        pass

    @abstractmethod
    def run_query_benchmark(self, mode: BenchmarkingMode):
        self.__set_thread_event_for_stage(BenchmarkingStage.RUN_QUERY_BENCHMARK)
        pass

    @abstractmethod
    def launch(self, mode: BenchmarkingMode):
        pass

    @abstractmethod
    def mid_terminate(self, mode: BenchmarkingMode):
        self.__unset_thread_event_after_stage(BenchmarkingStage.INGEST)
        pass

    @abstractmethod
    def terminate(self, mode: BenchmarkingMode):
        pass

    def visualize(self):
        for mode, result in self.benchmarking_reseults.items():
            if result.decompressed_size:
                logger.info(
                    f"{mode.value.capitalize()} mode: decompressed size {result.decompressed_size}"
                )
            if result.compressed_size:
                logger.info(
                    f"{mode.value.capitalize()} mode: compressed size {result.compressed_size}"
                )
            if result.ratio:
                logger.info(f"{mode.value.capitalize()} mode: compression ratio {result.ratio}")
            if result.ingest_e2e_latency:
                logger.info(
                    f"{mode.value.capitalize()} mode: ingest e2e latency {result.ingest_e2e_latency}"
                )
            for i in range(len(result.query_e2e_latencies)):
                logger.info(
                    f"{mode.value.capitalize()} mode: No.{i} query e2e latency {result.query_e2e_latencies[i]}"
                )

            if self.config.get("system_metric", {}).get("enable", False):
                for metric in BenchmarkingSystemMetric:
                    for stage in BenchmarkingStage:
                        if not result.system_metric_results[metric].stage_results[stage]:
                            average_metric_result = 0
                        else:
                            result.system_metric_results[metric].stage_results[stage] = [
                                result
                                for result in result.system_metric_results[metric].stage_results[
                                    stage
                                ]
                                if 0 < result
                            ]
                            if -1 != result.system_metric_results[metric].result_baseline:
                                average_metric_result = int(
                                    statistics.mean(
                                        result.system_metric_results[metric].stage_results[stage]
                                    )
                                    - result.system_metric_results[metric].result_baseline
                                )
                            else:
                                average_metric_result = int(
                                    statistics.mean(
                                        result.system_metric_results[metric].stage_results[stage]
                                    )
                                )
                            logger.info(
                                f"{mode.value.capitalize()} mode: average {metric.value[0]} usage at {stage.value} stage: {average_metric_result}{metric.value[1]}"
                            )

    def __load_system_metric_polling_config(self, metric: BenchmarkingSystemMetric):
        for stage in BenchmarkingStage:
            interval = (
                self.config.get("system_metric", {})
                .get(metric.value[0], {})
                .get(f"{stage.value}_polling_interval", 10)
            )
            self.__system_metric_pollers[metric].stage_polling_intervals[stage] = interval
            logger.info(
                f"{metric.value[0].capitalize()} usage polling interval for {stage.value}: {interval} seconds"
            )

    def __record_system_metric_polling_sample(
        self, metric: BenchmarkingSystemMetric, mode: BenchmarkingMode
    ):
        for stage in BenchmarkingStage:
            if self.__system_metric_pollers[metric].stage_events[stage].is_set():
                metric_sample = self._acquire_system_metric_sample(metric)
                self.benchmarking_reseults[mode].system_metric_results[metric].stage_results[
                    stage
                ].append(metric_sample)
                logger.info(
                    f"Current {metric.value[0]} usage at {stage.value} stage: {metric_sample}{metric.value[1]}"
                )
                self.__system_metric_pollers[metric].stage_alteration_notifier.wait(
                    self.__system_metric_pollers[metric].stage_polling_intervals[stage]
                )
                break  # Only one stage's event should be set at any time

    def _acquire_system_metric_sample(self, metric: BenchmarkingSystemMetric) -> int:
        if BenchmarkingSystemMetric.MEMORY == metric:
            with open("/proc/meminfo", "r") as f:
                mem_total = 0
                mem_free = 0
                for line in f.readlines():
                    if "MemTotal" in line:
                        mem_total = int(line.strip().split()[1])
                    elif "MemFree" in line:
                        mem_free = int(line.strip().split()[1])
                    if 0 != mem_total and 0 != mem_free:
                        break
            metric_sample = mem_total - mem_free
        elif BenchmarkingSystemMetric.CPU == metric:
            metric_sample = 0
            # TODO
        else:
            raise Exception(f"Unknow metric: {metric.value[0]}")
        return metric_sample

    def __poll_system_metrics(self, metric: BenchmarkingSystemMetric, mode: BenchmarkingMode):
        self.__load_system_metric_polling_config(metric)

        while self.__overall_threading_event.is_set():
            self.__record_system_metric_polling_sample(metric, mode)

    def start_polling_system_metric(self, metric: BenchmarkingSystemMetric, mode: BenchmarkingMode):
        if not self.config.get("system_metric", {}).get("enable", False):
            return
        if not self.__overall_threading_event.is_set():
            logger.info(f"Start polling {metric.value[0]} usage for mode {mode.value}")
            if 0 == self.benchmarking_reseults[mode].system_metric_results[metric].result_baseline:
                metric_sample = self._acquire_system_metric_sample(metric)
                self.benchmarking_reseults[mode].system_metric_results[
                    metric
                ].result_baseline = metric_sample
                logger.info(f"Initial {metric.value[0]} usage: {metric_sample}{metric.value[1]}")
            self.__overall_threading_event.set()
            self.__system_metric_pollers[metric].thread = threading.Thread(
                target=self.__poll_system_metrics,
                args=(
                    metric,
                    mode,
                ),
                daemon=True,
            )
            self.__system_metric_pollers[metric].thread.start()
        else:
            logger.error(f"Already being polling {metric.value[0]} usage for mode {mode.value}")

    def stop_polling_system_metric(self, metric: BenchmarkingSystemMetric, mode: BenchmarkingMode):
        if not self.config.get("system_metric", {}).get("enable", False):
            return
        if self.__overall_threading_event.is_set():
            logger.info(f"Stop polling {metric.value[0]} usage for mode {mode.value}")
            self.__overall_threading_event.clear()
        else:
            logger.error(f"Already stopped polling {metric.value[0]} usage for mode {mode.value}")
