from modules.Data_cleaner.Strategies.base import CleaningStrategy, DataFrame 
from pyspark.sql.functions import col, expr

class ValidateDatesStrategy(CleaningStrategy):
    def __init__(self, columns: list, date_format: str):
        super().__init__(columns)
        self.date_format = date_format

    def clean(self, df: DataFrame) -> DataFrame:
        for column in self.columns:
            try:
                df = df.withColumn(column, expr(f"to_date({column}, '{self.date_format}')"))
                invalid_dates = df.filter(col(column).isNull())
                for row in invalid_dates.collect():
                    self.logger.log_error(row['__index'], column, 'invalid_date')
            except Exception as e:
                self.logger.log_error(None, column, str(e))
        
        return df