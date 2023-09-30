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


# ************* FOOTDATA producer *************
def get_footdata_data(url) -> dict:
    """
    Get JSON results from API call to /footdata path
    """
    headers = {"Accept": "application/json"}
    response = requests.get(url, headers=headers, timeout=60)
    print(
        f"***** header ***** : \n{response.headers},\
            \n\n*****body ***** : \n{response.text}"
    )
    # JSON decoder automatically converts JSON strings into a Python dictionary
    kafka_footdata = response.json()
    print("*********** type is: ", type(kafka_footdata))
    return kafka_footdata


def producer_footdata_start_streaming(**kwargs):
    """
    Producer 2 : calls the footdata API data every 10 seconds
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
            footdata_data = get_footdata_data(url)
            producer.send(topic, value=footdata_data)
            print(f"{footdata_data} successfully sent to Kafka {topic}")
        finally:
            producer.flush()
            time.sleep(10)


footdata_url = "http://127.0.0.1:5000/footdata"

config_footdata = {
    "url": footdata_url,
    "topic": "footdata",
}

if __name__ == "__main__":
    producer_footdata_start_streaming(**config_footdata)
