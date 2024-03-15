#----------------------------------------------------------
# import libraries
#----------------------------------------------------------
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window as win

#----------------------------------------------------------
# variables
#----------------------------------------------------------


#----------------------------------------------------------
# functions
#----------------------------------------------------------


#----------------------------------------------------------
# main
#----------------------------------------------------------

#creaete spark session
spark = SparkSession.\
        builder.\
        appName('Perion_exam').\
        getOrCreate()

#read csv file
csv_path = '/home/niv/Downloads/GitHubrepos/codeSpace/my_codes/Spark/:earning_erea/Learning_Spark_Oreilly/Exams/covid_data.csv'
df_csv = (spark.read.csv(path=csv_path, header=True, inferSchema=True))                        

df = df_csv.printSchema()
df_csv.show(3)

column_list = df_csv.columns
print(column_list,end='\n')
df_field_list = df_csv.schema.fields
print(df_field_list)

#list comprehension
date_cols = [field.name for field in df_csv.schema.fields if isinstance(field.dataType, T.LongType)]
print(f'--> LongType: {date_cols}')


df_agg = df_csv.groupBy(F.col('Direction'),F.col('Year'),F.col('Country')).agg(
                                                                    F.count('*').alias('country_row_count'),
                                                                    F.sum('Value').alias('sum_values'),
                                                                    F.first('Cumulative').alias('first_Cumulative')
                                                                    )
df_agg.show(10)

# window function

windowSpec = win.partitionBy(F.col('Country')).orderBy(F.col('first_Cumulative')).rowsBetween(win.unboundedPreceding,win.unboundedFollowing)

df_WF = df_agg.withColumn('sum_WF',F.sum(F.col('sum_values')).over(windowSpec))
df_WF.show(10)

# Collect data frame results into a list

df_sample = df_csv.limit(10)

colected_list = df_sample.rdd.flatMap(lambda row: (row['Country'],row['Year'])).collect()
print(colected_list)

# use udf with lambda

left2 = F.udf(lambda country: country[0:2], T.StringType())

df_mod = df_WF.withColumn('udf_l2', left2(F.col('Country')))
df_mod.show()


# Stop the SparkSession
spark.stop()

