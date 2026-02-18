from fastapi import FastAPI
from .gold import router
from .silver import router
from .oil import router
from .wheat import router
from gas import router

app = FastAPI()
app.include_router(router)
