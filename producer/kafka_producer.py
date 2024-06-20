from confluent_kafka import Producer
from dotenv import dotenv_values

config = dotenv_values(".env")
producer = None


def delivery_callback(err, msg):
    if err:
        print('Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


def init_kafka_producer():
    global producer
    configKafka = {
        'bootstrap.servers': config["KAFKA_BOOTSTRAP_SERVERS"],
        'sasl.username':     config["CLUSTER_API_KEY"],
        'sasl.password':     config["CLUSTER_API_SECRET"],

        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':   'PLAIN',
        'acks':              'all'
    }
    producer = Producer(configKafka)


def send_to_kafka(topic: str, message):
    serialized_message = message.SerializeToString()
    producer.produce(topic, serialized_message, callback=delivery_callback)
    producer.flush()
