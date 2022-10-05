from pyspark.sql.functions import udf, col, trim, ltrim, rtrim
from pyspark.sql.types import StringType
from hashlib import md5
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext, SQLContext


conf = SparkConf().setMaster('local').setAppName('column_encryption3')
sc = SparkContext(conf=conf)
sqlcontext = SQLContext(sc)

# encriptar en modo MD5
upperUDF = udf(lambda name: md5( name.encode("utf-8") ).hexdigest())

# lectura del archivo
# rawDF = spark.read.format("csv").option("header", "true").load("gs://project-bridge/python-test/Questions.csv")
rawDF = sqlcontext.read.csv('gs://project-bridge/python-test/Questions.csv', header=True)
rawDF.printSchema()

## limpiando espacios
rawDF = rawDF.withColumn('Title',trim(rawDF['Title']))
rawDF = rawDF.withColumn('Title',ltrim(rawDF['Title']))
rawDF = rawDF.withColumn('Title',rtrim(rawDF['Title']))
rawDF.show()

## limpiando nulos
rawDF = rawDF.where(rawDF['Title'].isNotNull())
rawDF.show()
# encriptar columna
rawDF = rawDF.withColumn("Title", upperUDF(rawDF['Title']))
rawDF.show(50)

rawDF.write.format("bigquery").option("table","result.encrypted_table").option("temporaryGcsBucket","pyspark_temp_table") .save()