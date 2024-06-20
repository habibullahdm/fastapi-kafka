from pydantic import BaseModel


class TransactionRequest(BaseModel):
    transaction_id: str
    amount: float
