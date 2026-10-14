import asyncio
import os
import sys
import urllib.parse
import websockets

PORT = int(os.environ.get("PORT", 8765))

# Mapping of room_id -> {"senders": set(), "receivers": set()}
rooms = {}

async def handler(websocket):
    raw_path = getattr(websocket, "path", getattr(getattr(websocket, "request", None), "path", "/"))
    parsed = urllib.parse.urlparse(raw_path)
    params = urllib.parse.parse_qs(parsed.query)
    
    role = params.get("role", ["receiver"])[0].lower()
    room_id = params.get("room", ["default_room"])[0]

    if room_id not in rooms:
        rooms[room_id] = {"senders": set(), "receivers": set()}

    room = rooms[room_id]

    if role == "sender":
        room["senders"].add(websocket)
        print(f"[+] Sender connected to room '{room_id}'. Total senders: {len(room['senders'])}, receivers: {len(room['receivers'])}")
        try:
            async for message in websocket:
                # Forward data to all receivers in this room
                dead_receivers = set()
                for rec in room["receivers"]:
                    try:
                        await rec.send(message)
                    except Exception:
                        dead_receivers.add(rec)
                
                # Cleanup disconnected receivers
                room["receivers"].difference_update(dead_receivers)
        except Exception as e:
            print(f"[-] Sender connection error in room '{room_id}': {e}")
        finally:
            room["senders"].discard(websocket)
            print(f"[-] Sender disconnected from room '{room_id}'")

    else:  # receiver
        room["receivers"].add(websocket)
        print(f"[+] Receiver connected to room '{room_id}'. Total receivers: {len(room['receivers'])}")
        try:
            # Keep receiver alive and listen for any messages or keepalives
            async for _ in websocket:
                pass
        except Exception as e:
            print(f"[-] Receiver connection error in room '{room_id}': {e}")
        finally:
            room["receivers"].discard(websocket)
            print(f"[-] Receiver disconnected from room '{room_id}'")

    # Clean up empty room
    if not room["senders"] and not room["receivers"]:
        rooms.pop(room_id, None)

async def main():
    host = "0.0.0.0"
    print(f"==================================================")
    print(f"  Cloud Video/Audio Relay Server running on {host}:{PORT}")
    print(f"==================================================")
    # ping_interval and ping_timeout keep cloud connections alive
    async with websockets.serve(handler, host, PORT, max_size=10_000_000, ping_interval=20, ping_timeout=20):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nRelay stopped.")
