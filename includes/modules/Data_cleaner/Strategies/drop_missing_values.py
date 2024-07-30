from modules.Data_cleaner.Strategies.base import CleaningStrategy, DataFrame

class DropMissingValuesStrategy(CleaningStrategy):
    def clean(self, df: DataFrame) -> DataFrame:
        initial_count = df.count()
        df_cleaned = df.dropna(subset=self.columns)
        final_count = df_cleaned.count()
        
        if initial_count > final_count:
            for column in self.columns:
                missing = df.filter(df[column].isNull())
                for row in missing.collect():
                    self.logger.log_error(row['__index'], column, 'missing_value')
        
        return df_cleaned
    
    