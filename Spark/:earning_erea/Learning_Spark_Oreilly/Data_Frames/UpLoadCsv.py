# =====================================================================
# Create my first view
# =====================================================================

# Import libraries:
from pyspark.sql import SparkSession

# Declare variables:
csv_path = '/home/niv/Downloads/GitHubrepos/LearningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv'

# create SparkSession:
spark = SparkSession.builder\
        .appName('my_firs_view')\
        .getOrCreate()

# read data from csv file and create a temporary table in memory:
# Infer schema by sparkSession (for larg table a schema must be provided as infering will take lots of resorcess)

df = spark.read.format('csv')\
        .option('inferSchema', 'true')\
        .option("header", "True")\
        .load(csv_path)
df.createOrReplaceTempView('us_delay_flights_tbl')

df1 = spark.read.table('us_delay_flights_tbl')

# Global view:
q1 = spark.sql('''select * from us_delay_flights_tbl where origin = "SFO" ''')
q1.createOrReplaceGlobalTempView("global_v1")
spark.sql('select origin, count(*) as num_of_rows from global_temp.global_v1 group by origin').show(10)

# Temporary view:
q1 = spark.sql('''select * from us_delay_flights_tbl where origin = "ABE" ''')
q1.createOrReplaceTempView("tmp_v1")
spark.sql('select origin, count(*) as num_of_rows from tmp_v1 group by origin').show(10)


[print(f'DB--> {i}') for i in spark.catalog.listDatabases()]
[print(f'Tbl--> {i}') for i in spark.catalog.listTables()]
tmp_view_cos = spark.table('tmp_v1').columns
[print(f'cols--> {i}') for i in tmp_view_cos]