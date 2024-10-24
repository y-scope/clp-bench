import logging
import subprocess
import time

from .executor import BenchmarkingMode, BenchmarkingSystemMetric, CPTExecutorBase, BenchmarkingResult

# Retrieve logger
logger = logging.getLogger(__name__)


class CPTExecutorCLPG(CPTExecutorBase):
    """
    A service provider for clp, which is a binary; clg is used for searching.
    """

    def deploy(self, mode: BenchmarkingMode):
        logger.info("Deploying CLP and CLG")
        container_id = self.config["clpg"]["container_id"]
        logger.info(f"clp docker container ID: {container_id}")
        clp_binary_path = self.config["clpg"]["clp_binary_path"]
        logger.info(f"clp binary location: {clp_binary_path}")
        clg_binary_path = self.config["clpg"]["clg_binary_path"]
        logger.info(f"clg binary location: {clg_binary_path}")
        data_path = self.config["clpg"]["data_path"]
        logger.info(f"clp data location: {data_path}")
        dataset_path = self.config["clpg"]["dataset_path"]
        logger.info(f"clp dataset location: {dataset_path}")

        self._check_file_in_docker(container_id, clp_binary_path)
        self._check_directory_in_docker(
            container_id, data_path, need_to_create=False, need_to_clear=True
        )
        self._check_directory_in_docker(container_id, data_path, need_to_create=False)

    def ingest(self, mode: BenchmarkingMode):
        super().ingest(mode)
        logger.info("Ingesting data for clp")
        container_id = self.config["clpg"]["container_id"]
        clp_binary_path = self.config["clpg"]["clp_binary_path"]
        data_path = self.config["clpg"]["data_path"]
        dataset_path = self.config["clpg"]["dataset_path"]
        try:
            result = subprocess.run(
                ["du", dataset_path, "-c", "-b"], stdout=subprocess.PIPE, check=True
            )
            decompressed_size_mb = (
                int(result.stdout.decode("utf-8").split("\n")[-2].split()[0].strip()) / 1024 / 1024
            )
            self.benchmarking_results[mode].decompressed_size = f"{decompressed_size_mb:.{BenchmarkingResult.SIZE_PRECISION}f}MB"
            start_ts = time.perf_counter_ns()
            subprocess.run(
                [
                    "docker",
                    "exec",
                    container_id,
                    "bash",
                    "-c",
                    f"{clp_binary_path} c {data_path} {dataset_path}",
                ],
                check=True,
            )
            end_ts = time.perf_counter_ns()
            elapsed_time = (end_ts - start_ts) / 1e9
            self.benchmarking_results[mode].ingest_e2e_latency = f"{elapsed_time:.{BenchmarkingResult.TIME_PRECISION}f}s"
            # FIXME: this is inconsistent with clp-s generated archives permission
            subprocess.run(
                ["sudo", "find", data_path, "-exec", "chmod", "o+r+x", "{}", ";"], check=True
            )
            result = subprocess.run(
                ["du", data_path, "-c", "-b"], stdout=subprocess.PIPE, check=True
            )
            compressed_size_mb = (
                int(result.stdout.decode("utf-8").split("\n")[-2].split()[0].strip()) / 1024 / 1024
            )
            self.benchmarking_results[mode].compressed_size = f"{compressed_size_mb:.{BenchmarkingResult.SIZE_PRECISION}f}MB"
            self.benchmarking_results[mode].ratio = f"{decompressed_size_mb / compressed_size_mb}x"
        except subprocess.CalledProcessError as e:
            raise Exception(f"clp failed to compress data: {e}")

    def run_query_benchmark(self, mode: BenchmarkingMode):
        super().run_query_benchmark(mode)
        logger.info("Running query benchmark for clp")
        container_id = self.config["clpg"]["container_id"]
        clg_binary_path = self.config["clpg"]["clg_binary_path"]
        data_path = self.config["clpg"]["data_path"]
        queries = self.config["clpg"]["queries"]
        try:
            for query in queries:
                command = f"docker exec {container_id} {clg_binary_path} {data_path} {query}"
                self._execute_query(mode, command)
        except subprocess.CalledProcessError as e:
            raise Exception(f"clp failed to finish the query benchmarking: {e}")
        pass

    def mid_terminate(self, mode: BenchmarkingMode):
        super().mid_terminate(mode)
        self.terminate(mode)

    def launch(self, mode: BenchmarkingMode):
        logger.info("Launching clp")
        pass

    def terminate(self, mode: BenchmarkingMode):
        logger.info("Terminating clp")
        pass

    def _acquire_system_metric_sample(self, metric: BenchmarkingSystemMetric) -> int:
        container_id = self.config["clpg"]["container_id"]
        try:
            result = subprocess.run(
                ["docker", "stats", container_id, "--no-stream"], stdout=subprocess.PIPE, check=True
            )
            output = result.stdout.decode("utf-8").strip().split("\n")
            for line in output:
                if container_id in line:
                    return self._get_mem_usage_from_docker_stats(line)
        except subprocess.CalledProcessError:
            raise Exception("clp failed to get mem usage info")
