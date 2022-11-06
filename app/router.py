from asyncio import sleep
from fastapi import APIRouter
from json import dumps
from datagenerator import generate_message_new_subs, generate_message_frequented_tram
from confluent_kafka import Producer
from faker import Faker
from confluent_kafka import Consumer
from receipt import receipt, receipt2
import json
import logging
import time
import ast

route = APIRouter()

#créer des messages
#@route.post('/create_message')
async def send():
    fake=Faker()
    p=Producer({'bootstrap.servers':'localhost:9092'})
    print('---------------------------')
    print('Producer est initialisé...')
    print('---------------------------')
    #les nouveaux adhérents
    for i in range(3):
        data=generate_message_new_subs()
        m=json.dumps(data) #transformer en format json
        p.poll(1)
        p.produce('new_subs', m.encode('utf-8'),callback=receipt)
        p.flush()
        time.sleep(3)
    #la ligne de tram la plus utilisée
    for i in range(3):
        data=generate_message_frequented_tram()
        m=json.dumps(data) #transformer en format json
        p.poll(1)
        p.produce('frequented_tram', m.encode('utf-8'),callback=receipt2)
        p.flush()
        time.sleep(3)
    #la station de tram la plus fréquentée
    for i in range(3):
        data=generate_message_frequented_tram()
        m=json.dumps(data) #transformer en format json
        p.poll(1)
        p.produce('frequented_station', m.encode('utf-8'),callback=receipt)
        p.flush()
        time.sleep(3)

#envoyer les messages à l'aide de kafka
#la méthode view permet de les visualiser (non, on peut visualiser les données à l'aide de l'interface de kafdrop)
#ici on crée notre endpoint /view
#au niveau de ce endpoint, on visualise pour une donnée envoyée pendant un jour, quel est l'abonnement le plus utilisé?
#objectif: proposer des réductions exclusives pour les abonnements les plus utilisées ou pour les autres
#new_subs is the topic of the new subscribers messages passing
@route.get('/view')
async def consume_new_subs():

    #les nouveaux adhérents
    c=Consumer({'bootstrap.servers':'localhost:9092','group.id':'python-consumer','auto.offset.reset':'earliest'})
    print('Les nouveaux adhérents--------------')
    #c.subscribe(['new_subs'])
    running = True
    try:
        while running:
            c.subscribe(['new_subs'])
            msg=c.poll(1.0) #timeout
            if msg is None:
                continue
            if msg.error():
                print('Error: {}'.format(msg.error()))
                continue
            else:
                data=msg.value().decode('utf-8')
                mydata = ast.literal_eval(data) #transformer en dictionnaire
                if ( mydata['new_subscribers'][0]['normal_user']> mydata['new_subscribers'][0]['monthly']) and (mydata['new_subscribers'][0]['normal_user'] > mydata['new_subscribers'][0]['year']):
                    largest_num = mydata['new_subscribers'][0]['normal_user']
                    abonnement = "normal_user"
                elif (mydata['new_subscribers'][0]['monthly'] > mydata['new_subscribers'][0]['normal_user']) and (mydata['new_subscribers'][0]['monthly'] > mydata['new_subscribers'][0]['year']):
                    largest_num = mydata['new_subscribers'][0]['monthly']
                    abonnement = "monthly"
                else:
                    largest_num = mydata['new_subscribers'][0]['year']
                    abonnement = "year"
                print(data)
                print("--------------------------------------------------------------------------------------------------------------")
                print(f"L'abonnement {abonnement} a eu le grand nombre d'adhérant avec un nombre de {largest_num}.")
                print("--------------------------------------------------------------------------------------------------------------")
    finally:
        # Close down consumer to commit final offsets.
        c.close()

    """for i in range(3):
        msg=c.poll(1.0) #timeout
        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        data=msg.value().decode('utf-8')
        mydata = ast.literal_eval(data) #transformer en dictionnaire
        if ( mydata['new_subscribers'][0]['normal_user']> mydata['new_subscribers'][0]['monthly']) and (mydata['new_subscribers'][0]['normal_user'] > mydata['new_subscribers'][0]['year']):
            largest_num = mydata['new_subscribers'][0]['normal_user']
            abonnement = "normal_user"
        elif (mydata['new_subscribers'][0]['monthly'] > mydata['new_subscribers'][0]['normal_user']) and (mydata['new_subscribers'][0]['monthly'] > mydata['new_subscribers'][0]['year']):
            largest_num = mydata['new_subscribers'][0]['monthly']
            abonnement = "monthly"
        else:
            largest_num = mydata['new_subscribers'][0]['year']
            abonnement = "year"
        print(data)
        print("--------------------------------------------------------------------------------------------------------------")
        print(f"L'abonnement {abonnement} a eu le grand nombre d'adhérant avec un nombre de {largest_num}.")
        print("--------------------------------------------------------------------------------------------------------------")
    c.close()"""

    c=Consumer({'bootstrap.servers':'localhost:9092','group.id':'python-consumer','auto.offset.reset':'earliest'})
    print('Les lignes les plus fréquentées--------------')
    running = True
    try:
        while running:
            c.subscribe(['frequented_tram'])
            msg=c.poll(1.0) #timeout
            if msg is None:
                continue
            if msg.error():
                print('Error: {}'.format(msg.error()))
                continue
            else:
                data=msg.value().decode('utf-8')
                mydata = ast.literal_eval(data)
                if ( mydata['tram_A'][0]['users']> mydata['tram_B'][0]['users']) and (mydata['tram_A'][0]['users'] > mydata['tram_C'][0]['users']):
                    largest_num = mydata['tram_A'][0]['users']
                    ligne = "tram_A"
                    date_time = mydata['day']
                elif (mydata['tram_B'][0]['users'] > mydata['tram_A'][0]['users']) and (mydata['tram_B'][0]['users'] > mydata['tram_C'][0]['users']):
                    largest_num = mydata['tram_B'][0]['users']
                    ligne = "tram_B"
                    date_time = mydata['day']
                else:
                    largest_num = mydata['tram_C'][0]['users']
                    ligne = "tram_C"
                    date_time = mydata['day']
                print(data)
                print("--------------------------------------------------------------------------------------------------------------")
                print(f"La ligne la plus fréquentée est {ligne} avec un nombre d'utilisateurs de {largest_num}.",date_time)
                print("--------------------------------------------------------------------------------------------------------------")
    finally:
        # Close down consumer to commit final offsets.
        c.close()
    
    #la ligne de tran la plus fréquentée
    """c=Consumer({'bootstrap.servers':'localhost:9092','group.id':'python-consumer','auto.offset.reset':'earliest'})
    print('Les lignes les plus fréquentées--------------')
    c.subscribe(['frequented_tram'])
    for i in range(3):
        msg=c.poll(1.0) #timeout
        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        data=msg.value().decode('utf-8')
        mydata = ast.literal_eval(data)
        if ( mydata['tram_A'][0]['users']> mydata['tram_B'][0]['users']) and (mydata['tram_A'][0]['users'] > mydata['tram_C'][0]['users']):
            largest_num = mydata['tram_A'][0]['users']
            ligne = "tram_A"
            date_time = mydata['day']
        elif (mydata['tram_B'][0]['users'] > mydata['tram_A'][0]['users']) and (mydata['tram_B'][0]['users'] > mydata['tram_C'][0]['users']):
            largest_num = mydata['tram_B'][0]['users']
            ligne = "tram_B"
            date_time = mydata['day']
        else:
            largest_num = mydata['tram_C'][0]['users']
            ligne = "tram_C"
            date_time = mydata['day']
        print(data)
        print("--------------------------------------------------------------------------------------------------------------")
        print(f"La ligne la plus fréquentée est {ligne} avec un nombre d'utilisateurs de {largest_num}.",date_time)
        print("--------------------------------------------------------------------------------------------------------------")
    c.close()"""

    c=Consumer({'bootstrap.servers':'localhost:9092','group.id':'python-consumer','auto.offset.reset':'earliest'})
    print('Les stations les plus fréquentées pour chaque ligne--------------')
    running = True
    try:
        while running:
            c.subscribe(['frequented_station'])
            msg=c.poll(1.0) #timeout
            if msg is None:
                continue
            if msg.error():
                print('Error: {}'.format(msg.error()))
                continue
            else:
                data=msg.value().decode('utf-8')
                mydata = ast.literal_eval(data)

                #pour le tramA
                if ( mydata['tram_A'][0]['station1']> mydata['tram_A'][0]['station2']) and (mydata['tram_A'][0]['station1'] > mydata['tram_A'][0]['station3']):
                    largest_num = mydata['tram_A'][0]['station1']
                    station = "station1"
                    date_time = mydata['day']
                elif (mydata['tram_A'][0]['station2'] > mydata['tram_A'][0]['station1']) and (mydata['tram_A'][0]['station2'] > mydata['tram_A'][0]['station3']):
                    largest_num = mydata['tram_A'][0]['station2']
                    station = "station2"
                    date_time = mydata['day']
                else:
                    largest_num = mydata['tram_A'][0]['station3']
                    station = "station3"
                    date_time = mydata['day']
                print(data)
                print("--------------------------------------------------------------------------------------------------------------")
                print(f"La station la plus fréquentée pour la ligne A est {station} avec un nombre d'utilisateurs de {largest_num}.",date_time)
                print("--------------------------------------------------------------------------------------------------------------")

                #pour le tramB
                if ( mydata['tram_B'][0]['station1']> mydata['tram_B'][0]['station2']) and (mydata['tram_B'][0]['station1'] > mydata['tram_B'][0]['station3']):
                    largest_num = mydata['tram_B'][0]['station1']
                    station = "station1"
                    date_time = mydata['day']
                elif (mydata['tram_B'][0]['station2'] > mydata['tram_B'][0]['station1']) and (mydata['tram_B'][0]['station2'] > mydata['tram_B'][0]['station3']):
                    largest_num = mydata['tram_B'][0]['station2']
                    station = "station2"
                    date_time = mydata['day']
                else:
                    largest_num = mydata['tram_B'][0]['station3']
                    station = "station3"
                    date_time = mydata['day']
                print(f"La station la plus fréquentée pour la ligne B est {station} avec un nombre d'utilisateurs de {largest_num}.",date_time)
                print("--------------------------------------------------------------------------------------------------------------")

                #pour le tramC
                if ( mydata['tram_C'][0]['station1']> mydata['tram_C'][0]['station2']) and (mydata['tram_C'][0]['station1'] > mydata['tram_C'][0]['station3']):
                    largest_num = mydata['tram_C'][0]['station1']
                    station = "station1"
                    date_time = mydata['day']
                elif (mydata['tram_C'][0]['station2'] > mydata['tram_C'][0]['station1']) and (mydata['tram_C'][0]['station2'] > mydata['tram_C'][0]['station3']):
                    largest_num = mydata['tram_C'][0]['station2']
                    station = "station2"
                    date_time = mydata['day']
                else:
                    largest_num = mydata['tram_C'][0]['station3']
                    station = "station3"
                    date_time = mydata['day']
                print(f"La station la plus fréquentée pour la ligne C est {station} avec un nombre d'utilisateurs de {largest_num}.",date_time)
                print("--------------------------------------------------------------------------------------------------------------")
    finally:
        # Close down consumer to commit final offsets.
        c.close()

    #la station de tram la plus fréquentée
    """c=Consumer({'bootstrap.servers':'localhost:9092','group.id':'python-consumer','auto.offset.reset':'earliest'})
    print('Les stations les plus fréquentées pour chaque ligne--------------')
    c.subscribe(['frequented_station'])
    for i in range(3):
        msg=c.poll(1.0) #timeout
        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        data=msg.value().decode('utf-8')
        mydata = ast.literal_eval(data)

        #pour le tramA
        if ( mydata['tram_A'][0]['station1']> mydata['tram_A'][0]['station2']) and (mydata['tram_A'][0]['station1'] > mydata['tram_A'][0]['station3']):
            largest_num = mydata['tram_A'][0]['station1']
            station = "station1"
            date_time = mydata['day']
        elif (mydata['tram_A'][0]['station2'] > mydata['tram_A'][0]['station1']) and (mydata['tram_A'][0]['station2'] > mydata['tram_A'][0]['station3']):
            largest_num = mydata['tram_A'][0]['station2']
            station = "station2"
            date_time = mydata['day']
        else:
            largest_num = mydata['tram_A'][0]['station3']
            station = "station3"
            date_time = mydata['day']
        print(data)
        print("--------------------------------------------------------------------------------------------------------------")
        print(f"La station la plus fréquentée pour la ligne A est {station} avec un nombre d'utilisateurs de {largest_num}.",date_time)
        print("--------------------------------------------------------------------------------------------------------------")

        #pour le tramB
        if ( mydata['tram_B'][0]['station1']> mydata['tram_B'][0]['station2']) and (mydata['tram_B'][0]['station1'] > mydata['tram_B'][0]['station3']):
            largest_num = mydata['tram_B'][0]['station1']
            station = "station1"
            date_time = mydata['day']
        elif (mydata['tram_B'][0]['station2'] > mydata['tram_B'][0]['station1']) and (mydata['tram_B'][0]['station2'] > mydata['tram_B'][0]['station3']):
            largest_num = mydata['tram_B'][0]['station2']
            station = "station2"
            date_time = mydata['day']
        else:
            largest_num = mydata['tram_B'][0]['station3']
            station = "station3"
            date_time = mydata['day']
        print(f"La station la plus fréquentée pour la ligne B est {station} avec un nombre d'utilisateurs de {largest_num}.",date_time)
        print("--------------------------------------------------------------------------------------------------------------")

        #pour le tramC
        if ( mydata['tram_C'][0]['station1']> mydata['tram_C'][0]['station2']) and (mydata['tram_C'][0]['station1'] > mydata['tram_C'][0]['station3']):
            largest_num = mydata['tram_C'][0]['station1']
            station = "station1"
            date_time = mydata['day']
        elif (mydata['tram_C'][0]['station2'] > mydata['tram_C'][0]['station1']) and (mydata['tram_C'][0]['station2'] > mydata['tram_C'][0]['station3']):
            largest_num = mydata['tram_C'][0]['station2']
            station = "station2"
            date_time = mydata['day']
        else:
            largest_num = mydata['tram_C'][0]['station3']
            station = "station3"
            date_time = mydata['day']
        print(f"La station la plus fréquentée pour la ligne C est {station} avec un nombre d'utilisateurs de {largest_num}.",date_time)
        print("--------------------------------------------------------------------------------------------------------------")
    c.close()"""



    
#on peut aussi visualiser la station de tram la plus fréquentée (inspirée de la fonction précédente)
"""@route.get('/view_frequented_tram')
async def consume_frequented_tram():
    c=Consumer({'bootstrap.servers':'localhost:9092','group.id':'python-consumer','auto.offset.reset':'earliest'})
    print('Kafka Consumer has been initiated...')
    print('Available topics to consume: ', c.list_topics().topics)
    c.subscribe(['frequented_tram'])
    while True:
        msg=c.poll(1.0) #timeout
        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue
        data=msg.value().decode('utf-8')
        mydata = ast.literal_eval(data)
        if ( mydata['tram_A'][0]['users']> mydata['tram_B'][0]['users']) and (mydata['tram_A'][0]['users'] > mydata['tram_C'][0]['users']):
            largest_num = mydata['tram_A'][0]['users']
            ligne = "tram_A"
            date_time = mydata['day']
        elif (mydata['tram_B'][0]['users'] > mydata['tram_A'][0]['users']) and (mydata['tram_B'][0]['users'] > mydata['tram_C'][0]['users']):
            largest_num = mydata['tram_B'][0]['users']
            ligne = "tram_B"
            date_time = mydata['day']
        else:
            largest_num = mydata['tram_C'][0]['users']
            ligne = "tram_C"
            date_time = mydata['day']
        print(data)
        print(f"La ligne la plus fréquentée est {ligne} avec un nombre d'utilisateurs de {largest_num}.",date_time)
        print("-------------------------------------------------------")
    c.close()
    """