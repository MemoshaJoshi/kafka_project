import requests
import json
from json import dumps, loads
from kafka import KafkaConsumer, KafkaProducer

# url = "https://everyearthquake.p.rapidapi.com/earthquakes"

# querystring = {"start":"1","count":"100","type":"earthquake","latitude":"33.962523","longitude":"-118.3706975","radius":"1000","units":"miles","magnitude":"3","intensity":"1"}

# headers = {
# 	"X-RapidAPI-Key": "81672069a7msh0b3d711859966e6p1d86e5jsn84253a0e36b9",
# 	"X-RapidAPI-Host": "everyearthquake.p.rapidapi.com"
# }

# response = requests.request("GET", url, headers=headers, params=querystring)

consumer = KafkaConsumer('Earthquake_API',bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                              enable_auto_commit=True, value_deserializer=lambda x: loads(x.decode('utf-8')))

for msg in consumer:
    print(msg)
    with open('data.json', 'w') as f:
        json.dump(msg.value, f)
