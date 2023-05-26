import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

spark=SparkSession.builder.appName('Task1').master('local[1]').getOrCreate()

#df.printSchema()



df=spark.read.option('multiline','true').option('inferschema','true').option('header','true').json('AAD.json')

'''df = spark.read.format("json") \
      .option("header", True) \
      .schema(schema) \
      .load('AAD.json')'''


df.show()
schema=StructType([
    StructField("Date",StringType(),True), \
    StructField("Close",FloatType(),True), \
    StructField("High",FloatType(),True), \
    StructField("Low", FloatType(), True), \
    StructField("Open", FloatType(), True), \
    StructField("Volume", IntegerType(), True), \
    StructField('Adjclose',FloatType(),True),\
    StructField('Adjhigh',FloatType(),True),\
    StructField('Adjlow',FloatType(),True),
    StructField('Adjopen',FloatType(),True),
    StructField('Adjvolume',IntegerType(),True),
    StructField('Divcash',FloatType(),True),
    StructField('Splitfactor',FloatType(),True)])

df1=df.schema(schema)
df1.show()
#df1=df.write.mode('overwrite').option('header','true').csv('AAD.csv')
