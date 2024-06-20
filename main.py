import uvicorn
from dotenv import dotenv_values
from fastapi import FastAPI

from producer.kafka_producer import init_kafka_producer

app = FastAPI()
config = dotenv_values(".env")

init_kafka_producer()

if __name__ == "__main__":
    uvicorn.run("main:app", host=config["HOST_URL"], port=int(config["HOST_PORT"]))
