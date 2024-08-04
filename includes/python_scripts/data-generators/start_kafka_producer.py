from confluent_kafka import Producer
import json, random, threading, time
from bank_app_simulation import BankAppSimulation

bas = BankAppSimulation(number_of_branches=3, number_of_customers=3)

conf = {
    'bootstrap.servers': 'localhost:29092',
    'client.id': 'bank-app-simulation-producer'
}

producer = Producer(conf)

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def run_simulation_and_kafka_producer(sending_msgs_num:int=None)->None:
    '''
    set sending_msgs_num to None to send unlimited msgs, it's none by default
    '''
    global bas
    c = 0
    while True:
        
        new_transactions = bas.simulate()

        for transaction in new_transactions:
            producer.produce(
                'bank-transactions',
                key=str(transaction['sender_id']),
                value=json.dumps(transaction),
                callback=delivery_report
            )
        
        producer.flush()
        
        if c == 100:
            break
        
        if sending_msgs_num:
            c += 1
            

def load_to_mongodb():
    global bas
    fraud_table = bas.fraud_transactions_data
    customers_table = bas.customers_data

    time.sleep(120) # every two mins


if __name__ == '__main__':
    thread = threading.Thread(target=load_to_mongodb)
    thread.start()
    run_simulation_and_kafka_producer(100)