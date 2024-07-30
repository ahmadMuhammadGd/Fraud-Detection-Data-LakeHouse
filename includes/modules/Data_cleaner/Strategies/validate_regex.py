from modules.Data_cleaner.Strategies.base import CleaningStrategy, DataFrame 
from pyspark.sql.functions import col


class ValidateRegexStrategy(CleaningStrategy):
    def __init__(self, columns: list, patterns: dict, error_msg: str='regex_mismatch'):
        super().__init__(columns)
        self.patterns = patterns
        self.error_msg = error_msg

    def clean(self, df: DataFrame) -> DataFrame:
        for column, pattern in self.patterns.items():
            invalid_records = df.filter(~col(column).rlike(pattern))
            for row in invalid_records.collect():
                self.logger.log_error(row['__index'], column, self.error_msg)
            df = df.filter(col(column).rlike(pattern))
        return df