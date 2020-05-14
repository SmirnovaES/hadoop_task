from pykafka import KafkaClient
import numpy as np
import random
import json
import time

colors = ['red', 'blue', 'green', 'yellow']
shapes = ['triangle', 'circle', 'rectangle']

client = KafkaClient(hosts="localhost:9092")
topic = client.topics["test"]

with topic.get_sync_producer() as producer:
#    start = time.time()
#    for i in range(100):
    while(True):
        size = np.random.lognormal()
        message = {'color': random.choice(colors), 'shape': random.choice(shapes), 'size': np.random.lognormal()}
        msg = json.dumps(message).encode('utf-8')
        producer.produce(msg)
#    end = time.time()
#    print(end - start)
