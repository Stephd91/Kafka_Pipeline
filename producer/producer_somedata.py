from random import choice, randint
import time
import json
import requests
from kafka import KafkaProducer


def create_kafka_producer():
    """
    Creates the Kafka producer object
    """
    # Kafka configuration
    kafka_broker_host = "localhost"
    kafka_broker_port = 9092
    kafka_broker = f"{kafka_broker_host}:{kafka_broker_port}"
    kafka_config = {
        "bootstrap_servers": kafka_broker,
        "value_serializer": lambda v: json.dumps(v).encode(
            "utf-8"
        ),  # Serialize obj to a JSON formatted str
    }
    # Create a Kafka producer instance
    return KafkaProducer(**kafka_config)


# 🔎 NEED TO SERIALIZE DATA SENT TO KAFKA BROKER :
# message value must be convert to type "bytes" before sending to kafka :
# 1st method : in the kafka config dict with the value_serializer argument
#    "value_serializer": lambda v: json.dumps(v).encode("utf-8")
# 2nd method : inside the producer function with the bytearray function
#   bytearray(message.encode("utf-8"))


# ************* SOMEDATA producer *************
def generate_somedata_path(url: str) -> str:
    """
    Generate random data from a list to pass in the somedata API path
    """
    vehicle = ("car", "truck", "van", "motorbike", "bike")
    message = choice(vehicle)
    number = randint(1, 4)
    return f"{url}/{message}?number={number}"


def get_somedata_data(url: str) -> dict:
    """
    Get JSON results from API call to /somedata path
    """
    somedata_url = generate_somedata_path(url)
    headers = {"Accept": "application/json"}
    response = requests.get(somedata_url, headers=headers, timeout=60)
    print(
        f"***** header ***** : \n{response.headers},\
            \n\n*****body ***** : \n{response.text}"
    )
    kafka_somedata = response.json()
    return kafka_somedata


def on_send_success(record_metadata):
    print("topic :", record_metadata.topic)
    print("partition : ", record_metadata.partition)
    print("offset :", record_metadata.offset)


def producer_somedata_start_streaming(**kwargs):
    """
    Producer 1 : calls the somedata API data every 10 seconds
    and send to Kafka topic based on config dict
    """
    # create the KafkaProducer instance
    producer = create_kafka_producer()
    # the script will run for 2 minutes
    end_time = time.time() + 120
    # unpack kwargs dict
    url, topic = kwargs.get("url"), kwargs.get("topic")
    while True:
        if time.time() > end_time:
            producer.close()
            break
        try:
            somedata_data = get_somedata_data(url)
            future = producer.send(topic, value=somedata_data)
            # log messages :
            record_metadata = future.get(timeout=10)
            on_send_success(record_metadata)
            print(f"{somedata_data} successfully sent to Kafka {topic}")
        finally:
            producer.flush()
            time.sleep(10)


# ********* Launch producer *********
somedata_url = "http://127.0.0.1:5000/somedata"

config_somedata = {
    "url": somedata_url,
    "topic": "somedata",
}

if __name__ == "__main__":
    producer_somedata_start_streaming(**config_somedata)
