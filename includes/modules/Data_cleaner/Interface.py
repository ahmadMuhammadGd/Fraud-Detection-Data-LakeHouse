from modules.Data_cleaner.Strategies.base import CleaningStrategy
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import row_number,lit
from pyspark.sql.window import Window


class CleaningPipeline:
    def __init__(self):
        self.strategies = []
        self.df = None

    def add_strategy(self, strategy: CleaningStrategy):
        self.strategies.append(strategy)

    def set_dataframe(self, df: DataFrame):
        self.df = df.withColumn('__index', row_number().over(
                Window().orderBy(lit('A'))
                ))

    def run(self) -> DataFrame:
        if self.df is None:
            raise ValueError("DataFrame has not been set.")
        
        for strategy in self.strategies:
            self.df = strategy.clean(self.df)
            print(strategy.get_report())
        
        return self.df
