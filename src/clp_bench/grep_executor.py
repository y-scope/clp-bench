from .executor import BenchmarkingSystemMetric, BenchmarkingMode, CPTExecutorBase
import logging


# Retrive logger
logger = logging.getLogger(__name__)
  
  
class CPTExecutorGrep(CPTExecutorBase):
    """
    A service provider for grep, which is a binary.
    """
    
    def deploy(self, mode: BenchmarkingMode):
        logger.info("Deploying Grep")
        dataset_path = self.config['grep']['dataset_path']
        logger.info(f"grep data location: {dataset_path}")
    
    
    def ingest(self, mode: BenchmarkingMode):
        super().ingest(mode)
        pass
    
    
    def run_query_benchmark(self, mode: BenchmarkingMode):
        super().run_query_benchmark(mode)
        logger.info("Running query benchmark for grep")
        dataset_path = self.config['grep']['dataset_path']
        queries = self.config['grep']['queries']
        for query in queries:
            command = f'grep -r {query} {dataset_path}'
            self._execute_query(mode, command)
    
    
    def mid_terminate(self, mode: BenchmarkingMode):
        super().mid_terminate(mode)
        self.terminate(mode)
    
    
    def launch(self, mode: BenchmarkingMode):
        logger.info("Launching grep")
        pass
    
    
    def terminate(self, mode: BenchmarkingMode):
        logger.info("Terminating grep")
        pass