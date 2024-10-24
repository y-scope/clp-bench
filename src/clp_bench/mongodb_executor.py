import logging

from .executor import BenchmarkingMode, BenchmarkingSystemMetric, CPTExecutorBase

logger = logging.getLogger(__name__)


class CPTExecutorMongoDB(CPTExecutorBase):
    """
    A service provider for clp, which is a binary; clg is used for searching.
    """

    def deploy(self, mode: BenchmarkingMode):
        logger.info("Deploying MongoDB")
        

    def ingest(self, mode: BenchmarkingMode):
        super().ingest(mode)
        logger.info("Ingesting data for MongoDB")
        

    def run_query_benchmark(self, mode: BenchmarkingMode):
        super().run_query_benchmark(mode)
        logger.info("Running query benchmark for MongoDB")
        

    def mid_terminate(self, mode: BenchmarkingMode):
        super().mid_terminate(mode)
        self.terminate(mode)
        

    def launch(self, mode: BenchmarkingMode):
        logger.info("Launching MongoDB")


    def terminate(self, mode: BenchmarkingMode):
        logger.info("Terminating MongoDB")

    
