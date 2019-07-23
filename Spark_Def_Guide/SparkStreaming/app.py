from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.\
    builder.\
    appName("Spark Streaming Learning").\
    master('local[*]').\
    getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 5)

static = spark.read.json('D:/Spark-The-Definitive-Guide-master/Spark-The-Definitive-Guide-master/data/activity-data')

dataSchema = static.schema

streaming = spark.\
    readStream.\
    schema(dataSchema).\
    option("maxFilesPerTrigger", 1).\
    json('D:/Spark-The-Definitive-Guide-master/Spark-The-Definitive-Guide-master/data/activity-data')

activityCounts = streaming.groupBy("gt").count()

activityQuery = activityCounts.writeStream.queryName("activity_count").\
    format('memory').outputMode('complete').start()



from time import sleep
for x in range(10):
    spark.sql('select * from activity_count').show()
    sleep(1)

simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'")).\
    where("stairs").\
    where('gt is not null').select("gt", "model", "arrival_time", "creation_time").\
    writeStream.queryName('simple_transformation').format('memory').outputMode('append').start()

from time import sleep
for x in range(2):
    spark.sql('select * from simple_transformation').show()
    sleep(1)


#https://stackoverflow.com/questions/37975227/what-is-the-difference-between-cube-rollup-and-groupby-operators

deviceModelStats = streaming.cube("gt", "model").avg()\
    .drop("avg(Arrival_time)")\
    .drop("avg(Creation_Time)")\
    .drop("avg(Index)")\
    .writeStream.queryName("device_counts").format("memory")\
    .outputMode("complete")\
    .start()

from time import sleep
for x in range(10):
    spark.sql('select * from device_counts').show()
    sleep(1)

activityQuery.awaitTermination()




