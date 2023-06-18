import os
import uuid 
import numpy as np
from typing import List
import logging
from dotenv import load_dotenv
from fastapi import FastAPI
from faker import Faker
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from commands import CreatePeopleCommand,CreateDataCommand
from entities import Person
from entities import Data

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger()

load_dotenv(verbose=True)

app = FastAPI()

@app.on_event('startup')
async def startup_event():
    client = KafkaAdminClient(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'])
    try:
        topic = NewTopic(name=os.environ['TOPICS_DATA'],
                     num_partitions=int(os.environ['TOPICS_DATA_PARTITIONS']),
                     replication_factor=int(os.environ['TOPICS_DATA_REPLICAS']))
        client.create_topics([topic])
    except TopicAlreadyExistsError as e:
        logger.warning("Topic already exists")
    finally:
        client.close()


def make_producer():
    producer = KafkaProducer(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
                             linger_ms=int(os.environ['TOPICS_DATA_LINGER_MS']),
                             retries=int(os.environ['TOPICS_DATA_RETRIES']),
                             max_in_flight_requests_per_connection=int(os.environ['TOPICS_DATA_INFLIGHT_REQS']),
                             acks=os.environ['TOPICS_DATA_ACK'])
    return producer


class SuccessHandler:
    def __init__(self, data):
        self.data = data

    def __call__(self, rec_metadata):
        logger.info(f"""
                    Successfully produced
                    data {self.data}
                    to topic {rec_metadata.topic}
                    and partition {rec_metadata.partition}
                    at offset {rec_metadata.offset}
                    """)
        

class ErrorHandler:
    def __init__(self,data):
        self.data = data
    
    def __call__(self,ex):
        logger.error(f"Failed producing data {self.data}",exc_info=ex)




@app.post('/api/data', status_code=201, response_model=List[Data])
async def create_Data(cmd: CreateDataCommand):
    datas: List[Data] = []

    # faker = Faker()
    producer = make_producer()

    for num in range(cmd.count):
        data = Data(sample=str(num), data_values=str(np.random.randint(256,size=100)), cols=str(cmd.cols))
        datas.append(data)
        producer.send(topic=os.environ['TOPICS_DATA'],
                    #   key=person.title.lower().replace(r's+','-').encode('utf-8'),
                      key='row_0'.encode('utf-8'),
                      value=data.json().encode('utf-8'))\
                      .add_callback(SuccessHandler(data))\
                      .add_errback(ErrorHandler(data))
        
    producer.flush()

    return datas

@app.get('/hello-world')
async def hello_world():
    return {"message":"Hello World!"}