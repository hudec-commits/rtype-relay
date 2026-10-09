"""
Relay for the two-player co-op of the rtype remake: two browsers (or players) cannot talk to each
other directly, so both connect here and the relay passes their messages on. It knows nothing about
the game: it only pairs a host with a client in a room.

    python relay.py                 # ws://localhost:8787 (PORT env overrides, as on Render / fly.io)

Protocol (JSON text frames, "t" = type). The first message of a connection:
    {"t":"create"[,"room":"TEST"][,"public":true]}
                                    -> {"t":"created","room":"ABCD"}        the sender is the host; a public
                                       room is listed and random players may join it
    {"t":"join","room":"ABCD"}      -> {"t":"joined","room":"ABCD"} to the client, {"t":"peer"} to the host
                                    or {"t":"error","msg":"no such room" | "room full"}
    {"t":"join","room":"*"}         -> joins the public room that has waited longest for a second player,
                                       or {"t":"error","msg":"no open room"}
    {"t":"list"}                    -> {"t":"rooms","online":N,"rooms":[{"room","players","open","age"}],"players":[...]}
                                       (online = people in "players", a co-op pair two; "connections" = sockets)
                                       (public rooms only; the connection stays for more "list" requests).
                                       The first one also gets {"t":"history","lines":[{"name","text"}]}
    {"t":"say","name","text"}       -> lobby chat: {"t":"said","name","text"} to every lobby connection
                                       (the last HISTORY lines are kept for newcomers)
    {"t":"status","name","mode","score","stage","lives"}   a running game's state, again every few seconds on the
                                       same connection; "rooms" carries "players": who is playing (with the
                                       score), in a room, on the title screen
    {"t":"note","text"}             -> a line for the lobby log from a game ("PETR started a single game",
                                       "PETR game over - score 12300 (stage 2)"), then the connection closes
Every first message may carry "name". Countries: the relay looks the sender's address up in geo.bin
(geo_build.py, DB-IP Lite, CC BY 4.0; behind Render's proxy the address is in X-Forwarded-For) and adds
"cc" (two letters, "" unknown; a team "CZ+DE") to the players of "rooms", to "said" lines (chat and
events about someone), to "peer" / "joined" (the partner's), and "you" to "history" (the asker's own).
A "status" or "note" may bring its own "cc" (a co-op host knows both). The log gets country and browser. The lobby gets events as {"t":"said","sys":true,"text","ts"}
(ts = unix time, also in the history): someone online / gone, a public room opened, joined, left, closed. "peer" tells the host the client's name, "joined"
tells the client the host's.
In a room the players' own {"t":"chat",...} messages are relayed like everything else.
Then everything the host sends goes to the client and the other way round, untouched. When the host
leaves, the client gets {"t":"left","host":true} and the room is gone; when the client leaves, the host
gets {"t":"left"} and keeps the room for a new client.
"""
import asyncio
import bisect
import ipaddress
import json
import os
import random
import re
import string
import struct
import sys
import time
from array import array
from pathlib import Path

import websockets

ALPHABET = "".join(c for c in string.ascii_uppercase if c not in "IO")
rooms = {}          # code -> {"host": ws, "client": ws | None, "public": bool, "since": time}
connections = 0
lobby = {}          # connections browsing the rooms (they get the lobby chat) -> the name they gave
history = []        # the last lobby chat lines
playing = {}        # status connections of running games -> {"name", "mode", "score", "stage", "lives", "seen"}
STATUS_TIMEOUT = 15
HISTORY = 40


GEO = None          # (codes, IPv4 starts, IPv4 codes, IPv6 starts, IPv6 codes) from geo.bin
ccs = {}            # connection -> its country ("" unknown)
CC_RE = re.compile(r"^[A-Z]{2}(\+[A-Z]{2})?$")


def load_geo():
    global GEO
    path = Path(__file__).with_name("geo.bin")
    try:
        data = path.read_bytes()
    except OSError:
        print("no geo.bin: no countries", flush=True)
        return
    if data[:4] != b"GEO1":
        return
    at = 4
    (n,) = struct.unpack_from("<I", data, at)
    at += 4
    codes = [data[at + 2 * i:at + 2 * i + 2].decode("ascii") for i in range(n)]
    at += 2 * n
    tables = []
    for kind, size in (("I", 4), ("Q", 8)):
        (n,) = struct.unpack_from("<I", data, at)
        at += 4
        starts = array(kind)
        starts.frombytes(data[at:at + size * n])
        at += size * n
        idx = array("H")
        idx.frombytes(data[at:at + 2 * n])
        at += 2 * n
        tables += [starts, idx]
    GEO = (codes, *tables)


def country(ip):
    """Two letters for an address, "" when unknown (private, local, not in the table)."""
    fake = os.environ.get("RELAY_FAKE_CC", "")
    try:
        a = ipaddress.ip_address(ip)
    except ValueError:
        return fake
    if getattr(a, "ipv4_mapped", None):
        a = a.ipv4_mapped
    if not a.is_global:
        return fake                        # tests on localhost: RELAY_FAKE_CC=CZ
    if GEO is None:
        return ""
    codes, s4, c4, s6, c6 = GEO
    starts, idx, key = (s4, c4, int(a)) if a.version == 4 else (s6, c6, int(a) >> 64)
    i = bisect.bisect_right(starts, key) - 1
    c = codes[idx[i]] if i >= 0 else "ZZ"
    return "" if c == "ZZ" else c


def headers_of(ws):
    req = getattr(ws, "request", None)
    return req.headers if req is not None else getattr(ws, "request_headers", {})


def address_of(ws):
    fwd = headers_of(ws).get("X-Forwarded-For", "")
    if fwd:
        return fwd.split(",")[0].strip()
    ra = ws.remote_address
    return ra[0] if ra else ""


def agent_of(ws):
    """The browser in a few words for the log (a headless one gives a bot away)."""
    ua = headers_of(ws).get("User-Agent", "") or "-"
    for key in ("HeadlessChrome", "Edg/", "OPR/", "Firefox/", "Chrome/", "Safari/", "UnityPlayer", "Python"):
        if key in ua:
            system = next((o for o in ("Windows", "Android", "iPhone", "iPad", "Mac OS", "Linux", "CrOS") if o in ua), "")
            return (key.rstrip("/") + (" " + system if system else "")).strip()
    return ua[:40]


def clean_cc(v):
    v = str(v or "").upper()
    return v if CC_RE.match(v) else ""


def log(*parts):
    print(time.strftime("%H:%M:%S"), *parts, flush=True)


async def send(ws, obj):
    try:
        await ws.send(json.dumps(obj, separators=(",", ":")))
    except Exception:
        pass


def room_list():
    now = time.time()
    out = []
    for code, room in sorted(rooms.items(), key=lambda kv: kv[1]["since"]):
        if not room["public"]:
            continue
        out.append({"room": code, "players": 1 if room["client"] is None else 2,
                    "open": room["client"] is None, "age": int(now - room["since"])})
    players = player_list()
    # people, not connections: a game has a second connection for its status, a room host one more
    online = sum(len(p["name"].split("+")) for p in players)
    return {"t": "rooms", "online": online, "connections": connections, "rooms": out, "players": players}


def player_list():
    """Who is around: games that tell their state (score, stage, lives), the rooms, the title screens."""
    now = time.time()
    out, named = [], set()
    for ws, st in list(playing.items()):
        if now - st["seen"] > STATUS_TIMEOUT:
            continue
        out.append({"name": st["name"], "where": "playing", "mode": st["mode"], "score": st["score"],
                    "stage": st["stage"], "lives": st["lives"], "cc": st.get("cc", "")})
        named.update(n.strip() for n in st["name"].split("+"))
    for code, room in rooms.items():
        names = [room["host_name"]] + ([room["client_name"]] if room["client"] is not None else [])
        if all(n in named for n in names):
            continue
        team = [room.get("host_cc", "")] + ([room.get("client_cc", "")] if room["client"] is not None else [])
        out.append({"name": " + ".join(names), "where": "room", "room": code if room["public"] else "",
                    "full": room["client"] is not None, "cc": "+".join(c or "ZZ" for c in team) if any(team) else ""})
        named.update(names)
    for ws, name in list(lobby.items()):
        if name not in named:
            out.append({"name": name, "where": "title", "cc": ccs.get(ws, "")})
    return out


def to_int(v, default):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def clean_name(v):
    return str(v or "").strip()[:16] or "?"


async def lobby_event(text, skip=None, cc=""):
    """A line of the lobby log to everyone browsing (kept in the history like chat); cc = whose it is."""
    line = {"name": "", "text": text, "sys": True, "ts": int(time.time()), "cc": cc}
    history.append(line)
    del history[:-HISTORY]
    for other in list(lobby):
        if other is not skip:
            await send(other, dict(line, t="said"))


async def handle(ws):
    global connections
    connections += 1
    role, code = None, None
    cc = country(address_of(ws))
    ccs[ws] = cc
    try:
        first = json.loads(await ws.recv())
        if first.get("t") == "note":
            # a line for the lobby's log from a game (started, game over with the score): told, then gone
            text = str(first.get("text") or "")[:120].strip()
            if text:
                note_cc = clean_cc(first.get("cc")) or cc
                await lobby_event(text, cc=note_cc)
                log("note", text, note_cc or "-", agent_of(ws))
            return
        if first.get("t") == "status":
            # a running game tells its state every few seconds over a connection of its own
            while True:
                playing[ws] = {"name": str(first.get("name") or "?")[:40], "mode": str(first.get("mode") or "single")[:8],
                               "score": str(first.get("score") or "0")[:24], "stage": to_int(first.get("stage"), 1),
                               "lives": to_int(first.get("lives"), 0), "seen": time.time(),
                               "cc": clean_cc(first.get("cc")) or cc}
                first = json.loads(await ws.recv())
        while first.get("t") in ("list", "say"):      # a lobby browsing the rooms and chatting
            if ws not in lobby:
                lobby[ws] = clean_name(first.get("name"))
                await send(ws, {"t": "history", "lines": history, "you": cc})
                await lobby_event(f"{lobby[ws]} is online", skip=ws, cc=cc)
                log("online", lobby[ws], cc or "-", agent_of(ws))
            if first["t"] == "list":
                await send(ws, room_list())
            else:
                line = {"name": str(first.get("name") or "?")[:16], "text": str(first.get("text") or "")[:120], "cc": cc}
                if line["text"].strip():
                    history.append(line)
                    del history[:-HISTORY]
                    for other in list(lobby):
                        await send(other, dict(line, t="said"))
            first = json.loads(await ws.recv())
        if first.get("t") == "create":
            code = str(first.get("room") or "").upper()[:4]
            if not code or code in rooms:
                code = "".join(random.choice(ALPHABET) for _ in range(4))
                while code in rooms:
                    code = "".join(random.choice(ALPHABET) for _ in range(4))
            rooms[code] = {"host": ws, "client": None, "public": bool(first.get("public")), "since": time.time(),
                           "host_name": clean_name(first.get("name")), "client_name": "", "host_cc": cc, "client_cc": ""}
            role = "host"
            await send(ws, {"t": "created", "room": code})
            log("room", code, "created", "public" if rooms[code]["public"] else "private", rooms[code]["host_name"], cc or "-", agent_of(ws))
            if rooms[code]["public"]:
                await lobby_event(f"{rooms[code]['host_name']} opened room {code}", cc=cc)
        elif first.get("t") == "join":
            code = str(first.get("room") or "").upper()
            if code == "*":                   # random: the public room waiting longest
                open_rooms = [(r["since"], c) for c, r in rooms.items() if r["public"] and r["client"] is None]
                if not open_rooms:
                    await send(ws, {"t": "error", "msg": "no open room"})
                    return
                code = min(open_rooms)[1]
            room = rooms.get(code)
            if room is None:
                await send(ws, {"t": "error", "msg": "no such room"})
                return
            if room["client"] is not None:
                await send(ws, {"t": "error", "msg": "room full"})
                return
            room["client"] = ws
            room["client_name"] = clean_name(first.get("name"))
            room["client_cc"] = cc
            role = "client"
            await send(ws, {"t": "joined", "room": code, "host": room["host_name"], "cc": room.get("host_cc", "")})
            await send(room["host"], {"t": "peer", "name": room["client_name"], "cc": cc})
            if room["public"]:
                await lobby_event(f"{room['client_name']} joined {room['host_name']} in room {code}", cc=cc)
            log("room", code, "joined", room["client_name"], cc or "-", agent_of(ws))
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
        connections -= 1
        playing.pop(ws, None)
        ccs.pop(ws, None)
        gone = lobby.pop(ws, None)
        if gone is not None:
            await lobby_event(f"{gone} left", cc=cc)
        room = rooms.get(code) if code else None
        if room is not None and role == "host":
            del rooms[code]
            if room["client"] is not None:
                await send(room["client"], {"t": "left", "host": True})
                await room["client"].close()
            log("room", code, "closed by host")
            if room["public"]:
                await lobby_event(f"room {code} closed")
        elif room is not None and role == "client" and room["client"] is ws:
            room["client"] = None
            await send(room["host"], {"t": "left", "name": room["client_name"]})
            log("room", code, "client left")
            if room["public"]:
                await lobby_event(f"{room['client_name']} left room {code}", cc=room.get("client_cc", ""))


async def main():
    load_geo()
    port = int(os.environ.get("PORT", "8787"))
    async with websockets.serve(handle, "0.0.0.0", port, ping_interval=20, ping_timeout=20, max_size=256 * 1024):
        log(f"relay listening on ws://0.0.0.0:{port}")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
