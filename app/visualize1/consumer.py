from confluent_kafka import Consumer
import ast

c=Consumer({'bootstrap.servers':'localhost:9092','group.id':'python-consumer','auto.offset.reset':'earliest'})
print('Kafka Consumer has been initiated...')

print('Available topics to consume: ', c.list_topics().topics)
c.subscribe(['new_subs'])

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
        print(f"L'abonnement {abonnement} a eu le grand nombre d'adhérant avec un nombre de {largest_num}.")
        print("-------------------------------------------------------")
    c.close()
        
if __name__ == '__main__':
    main()