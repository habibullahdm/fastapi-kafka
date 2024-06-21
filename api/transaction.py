from dotenv import dotenv_values
from fastapi import APIRouter

from producer.kafka_producer import send_to_kafka
from protobuf import transaction_pb2 as transaction_pb
from schema.request import TransactionRequest

router = APIRouter()
config = dotenv_values(".env")
last_transaction_amount = 0


@router.post("/produce")
async def produce_transaction(transaction: TransactionRequest):
    global last_transaction_amount

    if transaction.amount > last_transaction_amount:
        transaction_notes = "Recent transactions exceed previous transactions"
    if transaction.amount <= last_transaction_amount:
        transaction_notes = "Recent transactions do not exceed previous transactions"

    transaction_proto = transaction_pb.Transaction()
    transaction_proto.transaction_id = transaction.transaction_id
    transaction_proto.amount = transaction.amount
    transaction_proto.notes = transaction_notes

    value = transaction_proto

    send_to_kafka(config["KAFKA_TOPIC"], value, transaction.transaction_id)
    last_transaction_amount = transaction.amount

    return {"status": "Message produced successfully"}
