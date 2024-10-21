from .executor import BenchmarkingMode, CPTExecutorBase
from dateutil import parser
from datetime import timedelta
import logging
import time
import subprocess


# Retrive logger
logger = logging.getLogger(__name__)
  
  
class CPTExecutorGrafanaLoki(CPTExecutorBase):
    """
    A service provider for Grafana Loki.
    """
    
    def deploy(self, mode: BenchmarkingMode):
        logger.info("Deploying Grafana Loki")
        pass
    
    
    def ingest(self, mode: BenchmarkingMode):
        super().ingest(mode)
        logger.info("Ingesting data for Grafana Loki")
        # When launched the loki and promtail containers, the ingestion is automatically started
        # You could query the current ingested bytes by:
        # curl -G http://localhost:3100/metrics | grep 'loki_distributor_bytes_received_total'
        # You could query the current compression size by:
        # curl -G http://localhost:3100/metrics | grep 'loki_chunk_store_stored_chunk_bytes_total'
        # You could query the ingestion time by:
        # curl -G http://localhost:3100/metrics | grep 'loki_request_duration_seconds_sum{method="POST",route="loki_api_v1_push"'
        pass
    
    
    def run_query_benchmark(self, mode: BenchmarkingMode):
        super().run_query_benchmark(mode)
        logger.info("Running query benchmark for Grafana Loki")
        logcli_binary_path = self.config['loki']['logcli_binary_path']
        job = self.config['loki']['job']
        limit = self.config['loki']['limit']
        batch = self.config['loki']['batch']
        from_ts = self.config['loki']['from']
        to_ts = self.config['loki']['to']
        queries = self.config['loki']['queries']
        
        start_time = parser.isoparse(from_ts)
        end_time = parser.isoparse(to_ts)
        interval = timedelta(minutes=self.config.get('loki', {}).get('interval', 10))
        for query in queries:
            current_time = start_time
            total_query_latency = 0
            total_nr_matched_log_lines = 0
            while current_time <= end_time - interval:
                command = f'{logcli_binary_path} query ' + "'{ job=" + f'"{job}"' + '} |~ ' + query + f"' --limit={limit} --batch={batch} " + f'--from="{current_time.isoformat()}" --to="{(current_time + interval).isoformat()}" | wc -l'
                logger.info(f"Executing command: {command}")
                start_ts = time.perf_counter_ns()
                result = subprocess.run(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    shell=True,
                    check=True
                )
                end_ts = time.perf_counter_ns()
                total_query_latency += (end_ts - start_ts) / 1e9
                total_nr_matched_log_lines += int(result.stdout.decode('utf-8').strip())
                current_time += interval
            logger.info(f'Number of matched log lines: {total_nr_matched_log_lines}')
            self.benchmarking_reseults[mode].query_e2e_latencies.append(f'{total_query_latency:.9f}s')
    
    
    def launch(self, mode: BenchmarkingMode):
        logger.info("Launching Grafana Loki")
        pass
    
    
    def mid_terminate(self, mode: BenchmarkingMode):
        super().mid_terminate(mode)
        self.terminate(mode)
    
    
    def terminate(self, mode: BenchmarkingMode):
        logger.info("Terminating Grafana Loki")
        pass
    
    