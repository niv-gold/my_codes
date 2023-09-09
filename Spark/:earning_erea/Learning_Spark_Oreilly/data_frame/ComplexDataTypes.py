from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('HighOrder').getOrCreate()

# df1 = spark.sql("SELECT array_distinct(array(1,2,2,3,3,4,4,4,4,null))");
# df1.show()

array1 = [[[1,2,3,4,4,4,4,5,6,7,8]],[[1,2,3,4,20]],[[1,1,1,1,1,1,1]]]

scm = "array1 ARRAY<INT>"
df1 = spark.createDataFrame(array1,scm)
df1.show()

cols_lst = [F.array_distinct(F.col('array1')).alias('array_distinct'),
            F.array_max(F.col('array1')).alias('array_max')]

df_arry_distinct = df1.select(cols_lst)
df_arry_distinct.show(truncate=False)
