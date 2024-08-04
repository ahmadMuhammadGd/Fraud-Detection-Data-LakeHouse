import sys
sys.path.insert(1, '/')
import glob

from includes.modules.SparkIcebergNessieMinIO.spark_setup import init_spark_session

def read_sql_file(file_path:str)->str:
    with open(file_path, 'r') as file:
        return file.read()
    
spark = init_spark_session(app_name="DLH tables init")

# SQL FILES WILL BE EXCUTED BY ORDER
# every file contains ONLY one sql query
sql_files_dir = [ 
    '/includes/SQL-scripts/raw_bronz/init-*.sql',
    '/includes/SQL-scripts/lookup-tables/init-*.sql',
    '/includes/SQL-scripts/silver/init-*.sql',
]

sql_files_paths= [file for path in sql_files_dir for file in glob.glob(path)]
sql_files_content = [read_sql_file(path) for path in sql_files_paths]

for file in sql_files_content:
    for query in file:
        spark.sql(query)