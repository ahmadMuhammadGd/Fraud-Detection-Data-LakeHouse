from modules.Data_cleaner.Strategies.base import CleaningStrategy, DataFrame 
class ValidateColumnTypesStrategy(CleaningStrategy):
    def __init__(self, columns: list, expected_types: dict):
        super().__init__(columns)
        self.expected_types = expected_types

    def clean(self, df: DataFrame) -> DataFrame:
        for column, expected_type in self.expected_types.items():
            if column in self.columns:
                actual_type = str(df.schema[column].dataType)
                if actual_type != expected_type:
                    self.logger.log_error(None, column, f"Type mismatch: expected {expected_type}, got {actual_type}")
        return df
