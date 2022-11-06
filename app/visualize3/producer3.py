from confluent_kafka import Producer
from faker import Faker
import json
import time
import logging
import random
from datagenerator import generate_message_frequented_tram

fake=Faker()

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer3.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


p=Producer({'bootstrap.servers':'localhost:9092'})
print('Kafka Producer has been initiated...')

def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)
        

def main():
    for i in range(10):
        data=generate_message_frequented_tram()
        m=json.dumps(data) #transformer en format json
        p.poll(1)
        p.produce('frequented_station', m.encode('utf-8'),callback=receipt)
        p.flush()
        time.sleep(3)
        
if __name__ == '__main__':
    main()