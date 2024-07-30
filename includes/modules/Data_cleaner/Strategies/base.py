from pyspark.sql import DataFrame
from modules.logger.logger import Logger 

class CleaningStrategy:
    def __init__(self, columns: list):
        self.columns = columns
        self.errors = []
        self.logger = Logger()

    def clean(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError

    def get_report(self) -> str:
        return self.logger.get_logs()
