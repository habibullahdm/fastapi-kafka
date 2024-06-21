import threading

from confluent_kafka import Consumer
from dotenv import dotenv_values

from protobuf import transaction_pb2 as transaction_pb

config = dotenv_values(".env")


def init_kafka_consumer():
    global consumer
    configKafka = {
        'bootstrap.servers': config["KAFKA_BOOTSTRAP_SERVERS"],
        'sasl.username': config["CLUSTER_API_KEY"],
        'sasl.password': config["CLUSTER_API_SECRET"],

        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'group.id': config["KAFKA_GROUP_ID"],
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(configKafka)
    consumer.subscribe([config["KAFKA_TOPIC"]])


def consume():
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                transaction_proto = transaction_pb.Transaction()
                transaction_proto.ParseFromString(msg.value())
                print("Consumed event from topic {topic}:\n{proto}".format(
                    topic=msg.topic(), proto=transaction_proto))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def run_consumer():
    consumer_thread = threading.Thread(target=consume)
    consumer_thread.start()
