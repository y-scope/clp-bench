import logging
import re
import subprocess
import time

from .executor import BenchmarkingMode, BenchmarkingSystemMetric, CPTExecutorBase

# Retrive logger
logger = logging.getLogger(__name__)


class CPTExecutorCLPJson(CPTExecutorBase):
    """
    A service provider for CLP.
    """

    def __init__(self, config_path: str) -> None:
        super().__init__(config_path)
        # We read memory info directly from elasticsearch's API, there is no need to use baseline
        for mode in BenchmarkingMode:
            self.benchmarking_reseults[mode].system_metric_results[
                BenchmarkingSystemMetric.MEMORY
            ].result_baseline = -1

    def deploy(self, mode: BenchmarkingMode):
        logger.info("Deploying CLP")
        container_id = self.config["clp_json"]["container_id"]
        logger.info(f"clp-json docker container ID: {container_id}")
        launch_script_path = self.config["clp_json"]["launch_script_path"]
        logger.info(f"clp-json launch script location: {launch_script_path}")
        compress_script_path = self.config["clp_json"]["compress_script_path"]
        logger.info(f"clp-json compress script location: {compress_script_path}")
        serach_script_path = self.config["clp_json"]["search_script_path"]
        logger.info(f"clp-json search script location: {serach_script_path}")
        terminate_script_path = self.config["clp_json"]["terminate_script_path"]
        logger.info(f"clp-json terminate script location: {terminate_script_path}")
        data_path = self.config["clp_json"]["data_path"]
        logger.info(f"clp-json data location: {data_path}")
        log_path = self.config["clp_json"]["log_path"]
        logger.info(f"clp-json log location: {log_path}")
        dataset_path = self.config["clp_json"]["dataset_path"]
        logger.info(f"clp-json dataset location: {dataset_path}")

        self._check_file_in_docker(container_id, launch_script_path)
        self._check_file_in_docker(container_id, compress_script_path)
        self._check_file_in_docker(container_id, serach_script_path)
        self._check_file_in_docker(container_id, terminate_script_path)
        self._check_directory_in_docker(
            container_id, data_path, need_to_create=False, need_to_clear=True
        )
        self._check_directory_in_docker(
            container_id, log_path, need_to_create=False, need_to_clear=True
        )
        self._check_directory_in_docker(container_id, data_path, need_to_create=False)

    def ingest(self, mode: BenchmarkingMode):
        super().ingest(mode)
        logger.info("Ingesting data for CLP")
        container_id = self.config["clp_json"]["container_id"]
        compress_script_path = self.config["clp_json"]["compress_script_path"]
        dataset_path = self.config["clp_json"]["dataset_path"]
        try:
            start_ts = time.perf_counter_ns()
            command = f"docker exec {container_id} {compress_script_path} --timestamp-key 't.$date' {dataset_path}"
            result = subprocess.run(
                command, stderr=subprocess.PIPE, shell=True, check=True, text=True
            )
            end_ts = time.perf_counter_ns()
            elapsed_time = (end_ts - start_ts) / 1e9
            logger.info(
                f"clp-json compressed data in {dataset_path} successfully in {elapsed_time:.9f} seconds"
            )
            self.benchmarking_reseults[mode].ingest_e2e_latency = f"{elapsed_time:.9f}s"
            output = result.stderr
            match = re.search(r"Compressed (\S+).*?into (\S+).*?\((\d+\.\d+x)\)", output)
            if match:
                self.benchmarking_reseults[mode].decompressed_size = match.group(1)
                self.benchmarking_reseults[mode].compressed_size = match.group(2)
                self.benchmarking_reseults[mode].ratio = match.group(3)
                logger.info("Ingest metrics collected")
            else:
                logger.error("Cannot get ingest metrics")
        except subprocess.CalledProcessError as e:
            raise Exception(f"clp-json failed to compress data: {e}")

    def run_query_benchmark(self, mode: BenchmarkingMode):
        super().run_query_benchmark(mode)
        logger.info("Running query benchmark for CLP")
        container_id = self.config["clp_json"]["container_id"]
        search_script_path = self.config["clp_json"]["search_script_path"]
        queries = self.config["clp_json"]["queries"]
        for query in queries:
            command = f"docker exec {container_id} {search_script_path} '{query}'"
            self._execute_query(mode, command)

    def launch(self, mode: BenchmarkingMode):
        logger.info("Launching CLP")
        try:
            container_id = self.config["clp_json"]["container_id"]
            launch_script_path = self.config["clp_json"]["launch_script_path"]
            subprocess.run(
                ["docker", "exec", container_id, "bash", "-c", f"{launch_script_path}"], check=True
            )
            logger.info(f"clp-json launched successfully in container {container_id}")
        except subprocess.CalledProcessError as e:
            raise Exception(f"clp-json failed to launch: {e}")

    def mid_terminate(self, mode: BenchmarkingMode):
        super().mid_terminate(mode)
        self.terminate(mode)

    def terminate(self, mode: BenchmarkingMode):
        logger.info("Terminating CLP")
        container_id = self.config["clp_json"]["container_id"]
        terminate_script_path = self.config["clp_json"]["terminate_script_path"]
        try:
            subprocess.run(
                ["docker", "exec", container_id, "bash", "-c", f"{terminate_script_path}"],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"clp-json failed to terminate: {e}")

    def _acquire_system_metric_sample(self, metric: BenchmarkingSystemMetric) -> int:
        container_id = self.config["clp_json"]["container_id"]
        while True:
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    container_id,
                    "bash",
                    "-c",
                    'docker stats $(docker ps --format "{{.Names}}" | grep "^clp-") --no-stream',
                ],
                stdout=subprocess.PIPE,
            )
            output = result.stdout.decode("utf-8").strip().split("\n")
            if len(output) > 1:
                break
            else:
                logger.info("Cannot get output of docker stats, try again")
        metric_sample = 0
        for line in output[1:]:
            metric_sample += self._get_mem_usage_from_docker_stats(line)
        return metric_sample
