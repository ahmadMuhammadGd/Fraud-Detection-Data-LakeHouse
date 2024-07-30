from modules.Data_cleaner.Strategies.base import CleaningStrategy, DataFrame
from pyspark.sql.functions import col

class FilterNegativeValuesStrategy(CleaningStrategy):
    def clean(self, df: DataFrame) -> DataFrame:
        for column in self.columns:
            invalid_values = df.filter(col(column) <= 0)
            for row in invalid_values.collect():
                self.logger.log_error(row['__index'], column, 'negative_value')
            df = df.filter(col(column) > 0)
        return df