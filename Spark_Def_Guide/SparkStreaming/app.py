from pyspark.sql import SparkSession

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

activityQuery.awaitTermination()




