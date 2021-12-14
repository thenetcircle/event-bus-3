import uvicorn
from fastapi import FastAPI


def get_app():
    app = FastAPI()

    @app.get("/")
    async def home():
        return "welcome"

    @app.get("/register-producer")
    async def register_producer():
        pass

    @app.get("/register-consumer")
    async def register_consumer():
        pass

    return app


def start(host: str, port: str):
    app = get_app()
    uvicorn.run(app, host=host, port=port)
