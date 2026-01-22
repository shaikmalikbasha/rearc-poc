from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# from .job import main

app = FastAPI()


# -------------------------------
# WebSocket connection manager
# -------------------------------
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)


manager = ConnectionManager()


# -------------------------------
# MinIO webhook
# -------------------------------
@app.post("/minio-webhook")
async def minio_webhook(request: Request):
    data = await request.json()

    for record in data.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        file_name = record["s3"]["object"]["key"]
        msg = f"New file dropped: {file_name} in bucket '{bucket}'"

        await manager.broadcast(msg)

    return {"status": "success"}


# -------------------------------
# WebSocket endpoint
# -------------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# -------------------------------
# Serve frontend
# -------------------------------
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root():
    return FileResponse("static/index.html")
