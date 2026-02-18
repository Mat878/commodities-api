from fastapi import FastAPI
from .gold import router as gold_router
from .silver import router as silver_router
from .oil import router as oil_router
from .gas import router as gas_router
from .wheat import router as wheat_router

app = FastAPI()

app.include_router(gold_router)
app.include_router(silver_router)
app.include_router(oil_router)
app.include_router(gas_router)
app.include_router(wheat_router)