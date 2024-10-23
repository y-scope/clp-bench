import logging
import subprocess
import time

from .executor import BenchmarkingMode, BenchmarkingSystemMetric, CPTExecutorBase

# Retrieve logger
logger = logging.getLogger(__name__)


class CPTExecutorCLPS(CPTExecutorBase):
    """
    A service provider for clp-s, which is a binary.
    """

    def __init__(self, config_path: str) -> None:
        super().__init__(config_path)
        # We read memory info directly from elasticsearch's API, there is no need to use baseline
        for mode in BenchmarkingMode:
            self.benchmarking_results[mode].system_metric_results[
                BenchmarkingSystemMetric.MEMORY
            ].result_baseline = -1

    def deploy(self, mode: BenchmarkingMode):
        logger.info("Deploying CLP-S")
        container_id = self.config["clp_s"]["container_id"]
        logger.info(f"clp-s docker container ID: {container_id}")
        binary_path = self.config["clp_s"]["binary_path"]
        logger.info(f"clp-s binary location: {binary_path}")
        data_path = self.config["clp_s"]["data_path"]
        logger.info(f"clp-s data location: {data_path}")
        dataset_path = self.config["clp_s"]["dataset_path"]
        logger.info(f"clp-s dataset location: {dataset_path}")

        self._check_file_in_docker(container_id, binary_path)
        self._check_directory_in_docker(
            container_id, data_path, need_to_create=False, need_to_clear=True
        )
        self._check_directory_in_docker(container_id, data_path, need_to_create=False)

    def ingest(self, mode: BenchmarkingMode):
        super().ingest(mode)
        logger.info("Ingesting data for clp-s")
        container_id = self.config["clp_s"]["container_id"]
        binary_path = self.config["clp_s"]["binary_path"]
        data_path = self.config["clp_s"]["data_path"]
        dataset_path = self.config["clp_s"]["dataset_path"]
        try:
            result = subprocess.run(
                ["du", dataset_path, "-c", "-b"], stdout=subprocess.PIPE, check=True
            )
            decompressed_size_mb = (
                int(result.stdout.decode("utf-8").split("\n")[-2].split()[0].strip()) / 1024 / 1024
            )
            self.benchmarking_results[mode].decompressed_size = f"{decompressed_size_mb:.2f}MB"
            start_ts = time.perf_counter_ns()
            subprocess.run(
                f"docker exec {container_id} {binary_path} c --timestamp-key 't.$date' "
                f"--target-encoded-size 268435456 {data_path} {dataset_path}",
                shell=True,
                check=True,
            )
            end_ts = time.perf_counter_ns()
            elapsed_time = (end_ts - start_ts) / 1e9
            self.benchmarking_results[mode].ingest_e2e_latency = f"{elapsed_time:.9f}s"
            result = subprocess.run(
                ["du", data_path, "-c", "-b"], stdout=subprocess.PIPE, check=True
            )
            compressed_size_mb = (
                int(result.stdout.decode("utf-8").split("\n")[-2].split()[0].strip()) / 1024 / 1024
            )
            self.benchmarking_results[mode].compressed_size = f"{compressed_size_mb:.2f}MB"
            self.benchmarking_results[mode].ratio = f"{decompressed_size_mb / compressed_size_mb}x"
        except subprocess.CalledProcessError as e:
            raise Exception(f"clp-s failed to compress data: {e}")

    def run_query_benchmark(self, mode: BenchmarkingMode):
        super().run_query_benchmark(mode)
        logger.info("Running query benchmark for clp-s")
        container_id = self.config["clp_s"]["container_id"]
        binary_path = self.config["clp_s"]["binary_path"]
        data_path = self.config["clp_s"]["data_path"]
        queries = self.config["clp_s"]["queries"]
        for query in queries:
            command = f"docker exec {container_id} {binary_path} s {data_path} '{query}'"
            self._execute_query(mode, command)

    def mid_terminate(self, mode: BenchmarkingMode):
        super().mid_terminate(mode)
        self.terminate(mode)

    def launch(self, mode: BenchmarkingMode):
        logger.info("Launching clp-s")
        pass

    def terminate(self, mode: BenchmarkingMode):
        logger.info("Terminating clp-s")
        pass

    def _acquire_system_metric_sample(self, metric: BenchmarkingSystemMetric) -> int:
        binary_path = self.config["clp_s"]["binary_path"]
        data_path = self.config["clp_s"]["data_path"]
        container_id = self.config["clp_s"]["container_id"]
        try:
            command = f"docker exec {container_id} ps aux"
            result = subprocess.run(command, stdout=subprocess.PIPE, shell=True, check=True)
            output = result.stdout.decode("utf-8").strip().split("\n")
            for line in output:
                if binary_path in line and data_path in line:
                    return int(line.strip().split()[5])
            return 0
        except subprocess.CalledProcessError:
            raise Exception("clp-s failed to get mem usage info")
