from fastapi import FastAPI
from .gold import router

app = FastAPI()
app.include_router(router)
