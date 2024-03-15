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

'creaete spark session'
spark = SparkSession.\
        builder.\
        appName('Perion_exam').\
        getOrCreate()

'read csv file'
csv_path = '/home/niv/Downloads/GitHubrepos/codeSpace/my_codes/Spark/:earning_erea/Learning_Spark_Oreilly/Exams/covid_data.csv'
df_csv = (spark.read.csv(path=csv_path, header=True, inferSchema=True))                        

df = df_csv.printSchema()
df_csv.show(3)

column_list = df_csv.columns
print(column_list,end='\n')





