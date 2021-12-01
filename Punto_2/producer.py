from kafka import KafkaProducer
import csv
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer = lambda v: json.dumps(v).encode('utf-8'))

with open('SPY_TICK_TRADE.csv','r') as file:
        reader = csv.DictReader(file, delimiter = "\t")
        for row in reader:
                for datos in row.values():
                        TIME, PRICE, SIZE, EXCHANGE, SALE_CONDITION, SUSPICIOUS = datos.split(",")
                        print(PRICE)

                        producer.send('parcial3', PRICE)
                        producer.flush()

