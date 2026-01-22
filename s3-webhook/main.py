from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import json

app = FastAPI()

# Store active browser connections
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

# 1. The Webhook Endpoint (MinIO hits this)
@app.post("/minio-webhook")
async def minio_webhook(request: Request):
    data = await request.json()
    
    # Extract filename and bucket from MinIO JSON structure
    for record in data.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        file_name = record["s3"]["object"]["key"]
        msg = f"New file dropped: {file_name} in bucket '{bucket}'"
        
        # Push to all connected browsers
        await manager.broadcast(msg)
    
    return {"status": "success"}

# 2. WebSocket Endpoint (Browser connects here)
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text() # Keep connection alive
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# 3. Simple Frontend to test
@app.get("/")
async def get():
    return HTMLResponse(content=HTML_CONTENT)

HTML_CONTENT = """
<!DOCTYPE html>
<html>
    <head>
        <title>MinIO Notifications</title>
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-gray-100 flex items-center justify-center h-screen">
        <div id="toast-container" class="fixed top-5 right-5 space-y-4"></div>
        <h1 class="text-2xl font-bold text-gray-700">Waiting for MinIO uploads...</h1>

        <script>
            const ws = new WebSocket("ws://localhost:8000/ws");
            ws.onmessage = function(event) {
                showToast(event.data);
            };

            function showToast(message) {
                const container = document.getElementById('toast-container');
                const toast = document.createElement('div');
                toast.className = "bg-blue-600 text-white px-6 py-3 rounded-lg shadow-lg transition-opacity duration-500 animate-bounce";
                toast.innerText = message;
                container.appendChild(toast);
                
                // Auto-remove after 5 seconds
                setTimeout(() => { toast.remove(); }, 5000);
            };
        </script>
    </body>
</html>
"""