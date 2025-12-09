from fastapi import FastAPI
from threading import Thread
from contextlib import asynccontextmanager
from producer import start_producing

@asynccontextmanager
async def lifespan(app):
    print("Application starting up...")
    thread = Thread(target=start_producing, daemon=True)
    thread.start()
    yield
    print("Application shutdown...")

app = FastAPI(lifespan=lifespan)

@app.get("/")
def status():
    return {"status": "Producer running..."}