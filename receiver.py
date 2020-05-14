from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.context import SQLContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row
import json
import numpy as np
import datetime

sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sc.setLogLevel("WARN")
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 10)
kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'test':1})

def parseJson(v):
    ds = json.loads(v[1])
    return ((ds['shape'], ds['color']), [ds['size']])

db_prop={'user': 'myuser', 'password': 'mypass'}

def writeToDB(v):
    if not v.isEmpty():
        try:
            v.toDF().write.jdbc(url='jdbc:postgresql://localhost:5432/mydb', table='logs', mode='append', properties=db_prop)
        except Exception as e:
            print(e)
            print("bad, bad")

parsed = kafkaStream.map(parseJson)
parsed = parsed.reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], (x[0][1], len(x[1]), np.percentile(x[1], 10))))
parsed.reduceByKey(lambda x, y: x if x[1] > y[1] else y).map(lambda v: Row(currentTime=datetime.datetime.now(), shape=v[0], mostPopularColor=v[1][0], percentile=float(v[1][2]))).foreachRDD(writeToDB)

ssc.start()
ssc.awaitTermination()
