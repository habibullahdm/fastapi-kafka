from fastapi import APIRouter

from api import transaction

api_router = APIRouter()

api_router.include_router(transaction.router,
                          prefix="/transaction",
                          tags=["transaction"])
