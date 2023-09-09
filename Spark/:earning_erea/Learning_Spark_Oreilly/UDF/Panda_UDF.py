
# Import libraries:
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.functions import pandas_udf, col
import pandas as pd

# create SparkSession:
spark = SparkSession.builder\
        .appName('creating _udf')\
        .getOrCreate()

# declare the cube function
def cubed(a: pd.Series) -> pd.Series:
    return a*a*a

# Create pandas UDF for the cube function
cube_udf = pandas_udf(cubed, returnType=LongType())

# Create pandas series
x = pd.Series([1,2,3])

# the function for a pandas_udf executed with loacal pandas data
print(cubed(x))

# Create spark DataFrame, 'spark' is an existing spark session
df = spark.range(1,4)

# Execute function as spark vectoriezed UDF
df.select('id', cube_udf(col('id'))).show()