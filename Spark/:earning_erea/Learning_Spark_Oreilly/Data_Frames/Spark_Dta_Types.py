import pyspark 
from datetime import datetime
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DoubleType, TimestampType, DecimalType, DateType, LongType
# -----------------------------------------------------------------
# schema configurations
# -----------------------------------------------------------------

schema_q = StructType([
        StructField("customer_id", DecimalType()),
        StructField("customer_id_code", DecimalType()),
        StructField("treatment_enter_date", TimestampType()),
        StructField("treatment_cpt_fk", StringType()),
        StructField("treatment_quantity", DecimalType()),
        StructField("ind_current", DecimalType()),
        StructField("treatment_fk", DecimalType()),
        StructField("customer_patient_distinct", LongType())
        ])

# ------------------------------------------------ case 1 --------------------------------------------------
'''pytest first try'''

accounting_data1 = [{'customer_id': 123456789 ,'customer_id_code': 0,'treatment_enter_date': datetime.strptime('2022-06-24 10:00:00', '%Y-%m-%d %H:%M:%S'),
                    'treatment_cpt_fk': '907740112', 'treatment_quantity':5, 'ind_current':1, 'treatment_fk':'4595', 
                    'customer_patient_distinct': '1840128'}]