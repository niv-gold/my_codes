# =====================================================================
# Create my first view
# =====================================================================

# Import libraries:
from pyspark.sql import SparkSession
from pyspark import SparkConf

# Declare variables:
csv_path = '/home/niv/Downloads/GitHubrepos/LearningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv'

# create SparkSession:
spark = SparkSession.builder\
        .appName('managed_unmanagedTables')\
        .getOrCreate()


conf = SparkConf().setAppName("managed_unmanagedTables").set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "True")


# create DB and set it to default:
# spark.sql('CREATE DATABASE learn_spark_db')
# spark.sql('USE learn_spark_db')

# read data from csv file and create a temporary table in memory:
# Infer schema by sparkSession (for larg table a schema must be provided as infering will take lots of resorcess)

df = spark.read.format('csv')\
        .option('inferSchema', 'True')\
        .option('haeder','True')\
        .load(csv_path)
df.createOrReplaceTempView('us_delay_flights_tbl')

# Managed table saved into learn_spark_db database:
delay_flights_df = spark.read.table('us_delay_flights_tbl')

delay_flights_df.write.saveAsTable('us_delay_flights',mode='overwrite')

# Unmanaged table saved into external location, out of the database scop:
delay_flights_df.write.option('path','/tmp/data_warehous/us_fights_tbl').saveAsTable('us_fights_unManagedTbl')

# read both tables:
spark.read.table('us_delay_flights').show(20)

lst = spark.catalog.listTables()

[print(f'--> {i}') for i in lst] 

spark.stop()