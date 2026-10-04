"""
Relay for the two-player co-op of the rtype remake: two browsers (or players) cannot talk to each
other directly, so both connect here and the relay passes their messages on. It knows nothing about
the game: it only pairs a host with a client in a room.

    python relay.py                 # ws://localhost:8787 (PORT env overrides, as on Render / fly.io)

Protocol (JSON text frames, "t" = type). The first message of a connection:
    {"t":"create"[,"room":"TEST"]}  -> {"t":"created","room":"ABCD"}        the sender is the host
    {"t":"join","room":"ABCD"}      -> {"t":"joined","room":"ABCD"} to the client, {"t":"peer"} to the host
                                    or {"t":"error","msg":"no such room" | "room full"}
Then everything the host sends goes to the client and the other way round, untouched. When the host
leaves, the client gets {"t":"left","host":true} and the room is gone; when the client leaves, the host
gets {"t":"left"} and keeps the room for a new client.
"""
import asyncio
import json
import os
import random
import string
import sys
import time

import websockets

ALPHABET = "".join(c for c in string.ascii_uppercase if c not in "IO")
rooms = {}          # code -> {"host": ws, "client": ws | None}


def log(*parts):
    print(time.strftime("%H:%M:%S"), *parts, flush=True)


async def send(ws, obj):
    try:
        await ws.send(json.dumps(obj, separators=(",", ":")))
    except Exception:
        pass


async def handle(ws):
    role, code = None, None
    try:
        first = json.loads(await ws.recv())
        if first.get("t") == "create":
            code = str(first.get("room") or "").upper()[:4]
            if not code or code in rooms:
                code = "".join(random.choice(ALPHABET) for _ in range(4))
                while code in rooms:
                    code = "".join(random.choice(ALPHABET) for _ in range(4))
            rooms[code] = {"host": ws, "client": None}
            role = "host"
            await send(ws, {"t": "created", "room": code})
            log("room", code, "created", ws.remote_address)
        elif first.get("t") == "join":
            code = str(first.get("room") or "").upper()
            room = rooms.get(code)
            if room is None:
                await send(ws, {"t": "error", "msg": "no such room"})
                return
            if room["client"] is not None:
                await send(ws, {"t": "error", "msg": "room full"})
                return
            room["client"] = ws
            role = "client"
            await send(ws, {"t": "joined", "room": code})
            await send(room["host"], {"t": "peer"})
            log("room", code, "joined", ws.remote_address)
        else:
            await send(ws, {"t": "error", "msg": "create or join first"})
            return

        async for message in ws:
            room = rooms.get(code)
            if room is None:
                break
            target = room["client"] if role == "host" else room["host"]
            if target is not None:
                try:
                    await target.send(message)
                except Exception:
                    pass
    except (websockets.ConnectionClosed, json.JSONDecodeError, asyncio.CancelledError):
        pass
    finally:
        room = rooms.get(code) if code else None
        if room is not None and role == "host":
            del rooms[code]
            if room["client"] is not None:
                await send(room["client"], {"t": "left", "host": True})
                await room["client"].close()
            log("room", code, "closed by host")
        elif room is not None and role == "client" and room["client"] is ws:
            room["client"] = None
            await send(room["host"], {"t": "left"})
            log("room", code, "client left")


async def main():
    port = int(os.environ.get("PORT", "8787"))
    async with websockets.serve(handle, "0.0.0.0", port, ping_interval=20, ping_timeout=20, max_size=256 * 1024):
        log(f"relay listening on ws://0.0.0.0:{port}")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
