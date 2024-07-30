from modules.Data_cleaner.Strategies.base import CleaningStrategy, DataFrame

class DropDuplicatesStrategy(CleaningStrategy):
    def clean(self, df: DataFrame) -> DataFrame:
        columns_to_select = [col for col in df.columns if col != "__index"]
        df = df.select(*columns_to_select)
        initial_count = df.count()
        df_cleaned = df.dropDuplicates()
        final_count = df_cleaned.count()
        
        if initial_count > final_count:
            self.logger.log_error(None, None, f"Dropped {initial_count - final_count} duplicate rows.")
        
        return df_cleaned
