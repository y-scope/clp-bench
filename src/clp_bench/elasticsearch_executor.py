import logging
import re
import subprocess

from .executor import BenchmarkingMode, BenchmarkingSystemMetric, CPTExecutorBase

# Retrive logger
logger = logging.getLogger(__name__)


class CPTExecutorElasticsearch(CPTExecutorBase):
    """
    A service provider for Elasticsearch.
    """

    def __init__(self, config_path: str) -> None:
        super().__init__(config_path)
        # We read memory info directly from elasticsearch's API, there is no need to use baseline
        for mode in BenchmarkingMode:
            self.benchmarking_results[mode].system_metric_results[
                BenchmarkingSystemMetric.MEMORY
            ].result_baseline = -1

    def deploy(self, mode: BenchmarkingMode):
        logger.info("Deploying Elasticsearch")
        container_id = self.config["elasticsearch"]["container_id"]
        logger.info(f"Elasticsearch docker container ID: {container_id}")
        launch_script_path = self.config["elasticsearch"]["launch_script_path"]
        logger.info(f"Elasticsearch launch script location: {launch_script_path}")
        compress_script_path = self.config["elasticsearch"]["compress_script_path"]
        logger.info(f"Elasticsearch compress script location: {compress_script_path}")
        serach_script_path = self.config["elasticsearch"]["search_script_path"]
        logger.info(f"Elasticsearch search script location: {serach_script_path}")
        terminate_script_path = self.config["elasticsearch"]["terminate_script_path"]
        logger.info(f"Elasticsearch terminate script location: {terminate_script_path}")
        data_path = self.config["elasticsearch"]["data_path"]
        logger.info(f"Elasticsearch data location: {data_path}")
        log_path = self.config["elasticsearch"]["log_path"]
        logger.info(f"Elasticsearch log location: {log_path}")
        dataset_path = self.config["elasticsearch"]["dataset_path"]
        logger.info(f"Elasticsearch dataset location: {dataset_path}")

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
        logger.info("Ingesting data for Elasticsearch")
        container_id = self.config["elasticsearch"]["container_id"]
        compress_script_path = self.config["elasticsearch"]["compress_script_path"]
        dataset_path = self.config["elasticsearch"]["dataset_path"]
        try:
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    container_id,
                    "bash",
                    "-c",
                    f"python3 {compress_script_path} {dataset_path}",
                ],
                stderr=subprocess.PIPE,
                check=True,
                text=True,
            )
            output = result.stderr
            decompressed_size_match = re.search(r"Original size for \S+ is (\d+)", output)
            compressed_size_match = re.search(r"Compressed size for \S+ is (\d+)", output)
            ratio_match = re.search(r"Compression ratio for \S+ is (\d+\.\d+)", output)
            ingest_e2e_match = re.search(r"Ingestion time for \S+ is (\d+\.\d+) s", output)
            if decompressed_size_match:
                self.benchmarking_results[mode].decompressed_size = (
                    f"{int(decompressed_size_match.group(1)) / 1024 / 1024}MB"
                )
                logger.info(
                    "File size before compression: "
                    f'{self.benchmarking_results[mode].decompressed_size}'
                )
            else:
                logger.error("Cannot get decompressed metric")
            if compressed_size_match:
                self.benchmarking_results[mode].compressed_size = (
                    f"{int(compressed_size_match.group(1)) / 1024 / 1024}MB"
                )
                logger.info(
                    "File size after compression: "
                    f'{self.benchmarking_results[mode].compressed_size}'
                )
            else:
                logger.error("Cannot get compressed metric")
            if ratio_match:
                self.benchmarking_results[mode].ratio = f"{ratio_match.group(1)}x"
                logger.info(f"Compression ratio: {self.benchmarking_results[mode].ratio}")
            else:
                logger.error("Cannot get compression ratio metric")
            if ingest_e2e_match:
                self.benchmarking_results[mode].ingest_e2e_latency = (
                    f"{ingest_e2e_match.group(1)}s"
                )
                logger.info(
                    f"Elasticsearch compressed data in {dataset_path} "
                    f"successfully in {ingest_e2e_match.group(1)} seconds"
                )
            else:
                logger.error("Cannot get ingest end-to-end latency metric")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Elasticsearch failed to compress data: {e}")

    def run_query_benchmark(self, mode: BenchmarkingMode):
        super().run_query_benchmark(mode)
        logger.info("Running query benchmark for Elasticsearch")
        container_id = self.config["elasticsearch"]["container_id"]
        search_script_path = self.config["elasticsearch"]["search_script_path"]
        queries = self.config["elasticsearch"]["queries"]
        for query in queries:
            command = f"docker exec {container_id} python3 {search_script_path} '{query}'"
            self._execute_query(mode, command)

    def launch(self, mode: BenchmarkingMode):
        logger.info("Launching Elasticsearch")
        try:
            container_id = self.config["elasticsearch"]["container_id"]
            launch_script_path = self.config["elasticsearch"]["launch_script_path"]
            subprocess.run(
                ["docker", "exec", container_id, "bash", "-c", f"bash {launch_script_path}"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
                check=True,
            )
            logger.info(f"Elasticsearch launched successfully in container {container_id}")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Elasticsearch failed to launch: {e}")

    def mid_terminate(self, mode: BenchmarkingMode):
        super().mid_terminate(mode)
        self.terminate(mode)

    def terminate(self, mode: BenchmarkingMode):
        logger.info("Terminating Elasticsearch")
        try:
            container_id = self.config["elasticsearch"]["container_id"]
            terminate_script_path = self.config["elasticsearch"]["terminate_script_path"]
            subprocess.run(
                ["docker", "exec", container_id, "bash", "-c", f"bash {terminate_script_path}"],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"Elasticsearch failed to terminate: {e}")


    def _acquire_system_metric_sample(self, metric: BenchmarkingSystemMetric) -> int:
        container_id = self.config["elasticsearch"]["container_id"]
        try:
            command = f"docker exec {container_id} ps aux"
            result = subprocess.run(command, stdout=subprocess.PIPE, shell=True, check=True)
            output = result.stdout.decode("utf-8").strip().split("\n")
            metric = 0
            for line in output:
                if "/usr/share/elasticsearch" in line:
                    metric += int(line.strip().split()[5])
            return metric
        except subprocess.CalledProcessError:
            raise Exception("Elasticsearch failed to get mem usage info")
