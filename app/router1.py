from asyncio import sleep
from fastapi import APIRouter
from schema import Message
from config import loop, KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP, KAFKA_TOPIC
from kafka import KafkaConsumer, KafkaProducer
import json
from json import dumps
from datagenerator import generate_message

route = APIRouter()

#créer des messages
"""async def send():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda x: dumps(x).encode('utf-8'))
    await producer.start()
    for e in range(100):
        data = generate_message()
        producer.send('messages', value=data)
        sleep(5)
    #await producer.start()
    #try:
       # print(f'Le message envoyé est: {message}')
      #  value_json = json.dumps(message.__dict__).encode('utf-8')
     #   await producer.send_and_wait(topic=KAFKA_TOPIC, value=value_json)
    #finally:
        await producer.stop()"""

#envoyer les messages à l'aide de kafka
#la méthode view permet de les visualiser (non, on peut visualiser les données à l'aide de l'interface de kafdrop)
#ici on crée notre endpoint /view
#au niveau de ce endpoint, on visualise pour une donnée envoyée pendant un jour, quel est l'abonnement le plus utilisé?
#objectif: proposer des réductions exclusives pour les abonnements les plus utilisées ou pour les autres
#new_subs is the topic of the new subscribers messages passing
@route.get('/view_new_subs')
async def consume_new_subs():
    consumer = KafkaConsumer('new_subs', bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, auto_offset_reset='earliest')
    #await consumer.start()
    #try:
        #async
    for msg in consumer:
        print(json.loads(msg.value))
        if ( msg.new_subscribers.normal_user> msg.new_subscribers.monthly) and (msg.new_subscribers.normal_user > msg.new_subscribers.year):
            largest_num = msg.new_subscribers.normal_user
            abonnement = "normal_user"
        elif (msg.new_subscribers.monthly > msg.new_subscribers.normal_user) and (msg.new_subscribers.monthly > msg.new_subscribers.year):
            largest_num = msg.new_subscribers.monthly
            abonnement = "monthly"
        else:
            largest_num = msg.new_subscribers.year
            abonnement = "year"
        print(f"L'abonnement {abonnement} a eu le grand nombre d'adhérant le {msg.day} avec un nombre de {largest_num}.")
    #finally:
    #    await consumer.stop()

#pour visualiser la ligne de tram la plus fréquentée
@route.get('/view_frequented_tram')
async def consume_frequented_tram():
    consumer = KafkaConsumer('frequented_tram', bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, auto_offset_reset='earliest')
    for msg in consumer:
        print(json.loads(msg.value))
        if ( msg.tram_A.users> msg.tram_B.users) and (msg.tram_A.users > msg.tram_C.users):
            largest_num = msg.tram_A.users
            ligne = "tram_A"
        elif (msg.tram_B.users > msg.tram_A.users) and (msg.tram_B.users > msg.tram_C.users):
            largest_num = msg.tram_B.users
            ligne = "tram_B"
        else:
            largest_num = msg.tram_C.users
            ligne = "tram_C"
        print(f"La ligne la plus fréquentée le {msg.day} est {ligne} avec un nombre d'utilisateurs de {largest_num}.")
    
#on peut aussi visualiser la station de tram la plus fréquentée (inspirée de la fonction précédente)
