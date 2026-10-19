from fastapi import Depends, FastAPI, WebSocket

from .common import create_handler

RPC = create_handler()
app = FastAPI()


async def connection_context(websocket: WebSocket):
    return {"client": websocket.client.host if websocket.client else None}


@app.websocket("/ws/")
async def rpc(websocket: WebSocket, context=Depends(connection_context)):
    await RPC.serve(websocket, context=context)


@app.get("/health")
async def health():
    return {"ok": True}
