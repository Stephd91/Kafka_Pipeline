import time
import json
from kafka import KafkaConsumer
from pymongo import MongoClient


# Kafka consumer configuration
def create_kafka_consumer():
    """
    Creates a Kafka consumer object
    """
    # Kafka configuration
    kafka_broker_host = "localhost"
    kafka_broker_port = 9092
    kafka_broker = f"{kafka_broker_host}:{kafka_broker_port}"
    kafka_config = {
        "bootstrap_servers": kafka_broker,  # Replace with your Kafka broker address
        "value_deserializer": lambda v: json.loads(
            v.decode("utf-8")
        ),  # deserialize data from JSON to Python object to str
        "auto_offset_reset": "earliest",  # Start reading from the beginning
        "group_id": "my_consumer_group",  # With a Consumer Group, the consumer will regularly commit (save) its position to Kafka
        "enable_auto_commit": False,  # Disable automatic commits
    }
    # Create a Kafka consumer object
    return KafkaConsumer(**kafka_config)


# 🔎 NEED TO SERIALIZE DATA SENT TO KAFKA BROKER :
# message value must be convert to type "bytes" before sending to kafka :
# 1st method : in the kafka config dict with the value_serializer argument
#    "value_serializer": lambda v: json.dumps(v).encode("utf-8")
# 2nd method : inside the producer function with the bytearray function
#   bytearray(message.encode("utf-8"))


# ************* Consumer *************
def read_batch_message(topic: str):
    """
    Read data form a kafka topic in batch.
    The data is displayed only if the producer is closed.
    """
    # Create a Kafka consumer instance
    consumer = create_kafka_consumer()
    consumer.subscribe([topic])
    print("Connected to Kafka")
    print(f"Reading messages from the topic {topic}")
    messages = []
    # https://stackoverflow.com/questions/66101466/python-kafka-keep-polling-topic-infinitely
    # https://stackoverflow.com/questions/73909580/how-to-poll-listen-to-a-kafka-topic-continuously
    try:
        while True:
            records = consumer.poll(timeout_ms=100, update_offsets=True)
            print("**** the dict records is :", records, "****")
            if not records:
                print("there are no more messages")
                break  # no more messages, exit the loop
            print(f"Received message: {records.values()}")
            for record in records.values():
                for consumer_record in record:
                    print("consumer_record :", consumer_record.value)
                    messages.append(consumer_record.value)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("consumer has been closed")
    return messages


# Debugging stuff 🚧
# a = read_batch_message("somedata")
# print(a)


def get_mongo_database(dbname: str):
    # mongodb configuration
    # URI = 'mongodb://root:root@mongo:27017/'
    mongo_host = "localhost"
    mongo_port = 27017

    # Create a connection to MongoDB Daemon Service using MongoClient
    mongo_client = MongoClient(host=mongo_host, port=mongo_port)
    # Access your MongoDB database
    db = mongo_client[dbname]
    return db


def consumer_monogdb(dbname: str, topic: str):
    """
    Send data to a MongoDB database that will be sent in a collection named as
    the kafka topic name being consumed.
    """
    # Connect to db
    db = get_mongo_database(dbname)
    collection = db[topic]
    # Read messages
    documents_to_send = read_batch_message(topic)
    print(documents_to_send)
    if topic not in db.list_collection_names():
        collection = db.create_collection(topic)
    collection.insert_many(documents_to_send)
    return print(
        f"Data successfully sent to collection {collection} into MongoDB{dbname}"
    )


# Debugging stuff 🚧
# b = consumer_monogdb("mydb", "somedata")
# print(b)

if __name__ == "__main__":
    consumer_monogdb("mydb", "footdata")
    # consumer_monogdb("mydb", "somedata")
