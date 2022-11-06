from confluent_kafka import Consumer
import ast

c=Consumer({'bootstrap.servers':'localhost:9092','group.id':'python-consumer','auto.offset.reset':'earliest'})
print('Kafka Consumer has been initiated...')

print('Available topics to consume: ', c.list_topics().topics)
c.subscribe(['frequented_tram'])

def main():
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
        
if __name__ == '__main__':
    main()