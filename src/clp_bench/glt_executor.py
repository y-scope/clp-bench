from .executor import BenchmarkingSystemMetric, BenchmarkingMode, CPTExecutorBase
import logging
import subprocess
import time


# Retrive logger
logger = logging.getLogger(__name__)


class CPTExecutorGLT(CPTExecutorBase):
    """
    A service provider for glt, which is a binary.
    """

    def deploy(self, mode: BenchmarkingMode):
        logger.info("Deploying GLT")
        container_id = self.config["glt"]["container_id"]
        logger.info(f"glt docker container ID: {container_id}")
        binary_path = self.config["glt"]["binary_path"]
        logger.info(f"glt binary location: {binary_path}")
        data_path = self.config["glt"]["data_path"]
        logger.info(f"glt data location: {data_path}")
        dataset_path = self.config["glt"]["dataset_path"]
        logger.info(f"glt dataset location: {dataset_path}")

        self._check_file_in_docker(container_id, binary_path)
        self._check_directory_in_docker(
            container_id, data_path, need_to_create=False, need_to_clear=True
        )
        self._check_directory_in_docker(container_id, data_path, need_to_create=False)

    def ingest(self, mode: BenchmarkingMode):
        super().ingest(mode)
        logger.info("Ingesting data for glt")
        container_id = self.config["glt"]["container_id"]
        binary_path = self.config["glt"]["binary_path"]
        data_path = self.config["glt"]["data_path"]
        dataset_path = self.config["glt"]["dataset_path"]
        try:
            result = subprocess.run(
                ["du", dataset_path, "-c", "-b"], stdout=subprocess.PIPE, check=True
            )
            decompressed_size_mb = (
                int(result.stdout.decode("utf-8").split("\n")[-2].split()[0].strip()) / 1024 / 1024
            )
            self.benchmarking_reseults[mode].decompressed_size = f"{decompressed_size_mb:.2f}MB"
            start_ts = time.perf_counter_ns()
            subprocess.run(
                [
                    "docker",
                    "exec",
                    container_id,
                    "bash",
                    "-c",
                    f"{binary_path} c {data_path} {dataset_path}",
                ],
                check=True,
            )
            end_ts = time.perf_counter_ns()
            elapsed_time = (end_ts - start_ts) / 1e9
            self.benchmarking_reseults[mode].ingest_e2e_latency = f"{elapsed_time:.9f}s"
            # FIXME: this is inconsistent with clp-s genereated archives permission
            subprocess.run(
                ["sudo", "find", data_path, "-exec", "chmod", "o+r+x", "{}", ";"], check=True
            )
            result = subprocess.run(
                ["du", data_path, "-c", "-b"], stdout=subprocess.PIPE, check=True
            )
            compressed_size_mb = (
                int(result.stdout.decode("utf-8").split("\n")[-2].split()[0].strip()) / 1024 / 1024
            )
            self.benchmarking_reseults[mode].compressed_size = f"{compressed_size_mb:.2f}MB"
            self.benchmarking_reseults[mode].ratio = f"{decompressed_size_mb / compressed_size_mb}x"
        except subprocess.CalledProcessError as e:
            raise Exception(f"glt failed to compress data: {e}")
        pass

    def run_query_benchmark(self, mode: BenchmarkingMode):
        super().run_query_benchmark(mode)
        logger.info("Running query benchmark for glt")
        container_id = self.config["glt"]["container_id"]
        binary_path = self.config["glt"]["binary_path"]
        data_path = self.config["glt"]["data_path"]
        queries = self.config["glt"]["queries"]
        try:
            for query in queries:
                command = f"docker exec {container_id} {binary_path} s {data_path} {query}"
                self._execute_query(mode, command)
        except subprocess.CalledProcessError as e:
            raise Exception(f"glt failed to finish the query benchmarking: {e}")

    def mid_terminate(self, mode: BenchmarkingMode):
        super().mid_terminate(mode)
        self.terminate(mode)

    def launch(self, mode: BenchmarkingMode):
        logger.info("Launching glt")
        pass

    def terminate(self, mode: BenchmarkingMode):
        logger.info("Terminating glt")
        pass

    # def _acquire_system_metric_sample(self, metric: BenchmarkingSystemMetric) -> int:
    #     container_id = self.config['glt']['container_id']
    #     try:
    #         result = subprocess.run(
    #             ['docker', 'stats', container_id, '--no-stream'],
    #             stdout=subprocess.PIPE,
    #             check=True
    #         )
    #         output = result.stdout.decode('utf-8').strip().split('\n')
    #         for line in output:
    #             if container_id in line:
    #                 return self._get_mem_usage_from_docker_stats(line)
    #     except subprocess.CalledProcessError as e:
    #         raise Exception(f"glt failed to get mem usage info")

    def _acquire_system_metric_sample(self, metric: BenchmarkingSystemMetric) -> int:
        binary_path = self.config["glt"]["binary_path"]
        data_path = self.config["glt"]["data_path"]
        container_id = self.config["glt"]["container_id"]
        try:
            command = f"docker exec {container_id} ps aux"
            result = subprocess.run(command, stdout=subprocess.PIPE, shell=True, check=True)
            output = result.stdout.decode("utf-8").strip().split("\n")
            for line in output:
                if binary_path in line and data_path in line:
                    return int(line.strip().split()[5])
            return 0
        except subprocess.CalledProcessError as e:
            raise Exception(f"clp-s failed to get mem usage info")
