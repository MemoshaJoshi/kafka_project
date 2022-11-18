import requests
import json
from json import dumps, loads
from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('kafkaConsumer').config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1").getOrCreate()

url = "https://everyearthquake.p.rapidapi.com/earthquakes"

querystring = {"start":"1","count":"100","type":"earthquake","latitude":"33.962523","longitude":"-118.3706975","radius":"1000","units":"miles","magnitude":"3","intensity":"1"}

headers = {
	"X-RapidAPI-Key": "81672069a7msh0b3d711859966e6p1d86e5jsn84253a0e36b9",
	"X-RapidAPI-Host": "everyearthquake.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)
print(response.url)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))

producer.send('Earthquake_API',value=response.json())
producer.flush()

product_rdd = spark.sparkContext.parallelize([json.dumps(response.json())])
schema_df = spark.read.json(product_rdd)
schema_product = schema_df.schema
with open('schema_products.json', 'w') as f:
    json.dump(schema_product.jsonValue(), f, indent=4)

with open('schema_products.json', 'r') as f:
    schema_earthquake1 = F.StructType.fromJson(json.load(f)) 

print(schema_earthquake1)

