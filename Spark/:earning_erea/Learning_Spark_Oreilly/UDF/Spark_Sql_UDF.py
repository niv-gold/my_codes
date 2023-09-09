
# Import libraries:
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.functions import pow

# create SparkSession:
spark = SparkSession.builder\
        .appName('creating _udf')\
        .getOrCreate()

# creat cube function
def cube(s: int):
    return s*s*s

# Register UDF
spark.udf.register('cubed',cube,LongType())

# Generate temporary view
spark.range(1,9).createOrReplaceTempView('udf_test')

spark.sql('select id, cubed(id) as id_cubed from udf_test').show()
