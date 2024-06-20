from dotenv import dotenv_values
from fastapi import APIRouter

from producer.kafka_producer import send_to_kafka
from schema.request import TransactionRequest

router = APIRouter()
config = dotenv_values(".env")
last_transaction_amount = 0.0


@router.post("/produce")
async def produce_transaction(transaction: TransactionRequest):
    global last_transaction_amount

    if transaction.amount > last_transaction_amount:
        transaction.notes = "Recent transactions exceed previous transactions"
    if transaction.amount <= last_transaction_amount:
        transaction.notes = "Recent transactions do not exceed previous transactions"

    message = transaction.notes

    send_to_kafka(config["KAFKA_TOPIC"], message)
    last_transaction_amount = transaction.amount

    return {"status": "Message produced successfully"}
