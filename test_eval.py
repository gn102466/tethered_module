import urllib.request, json, websocket, time

# Connect to CDP to inspect the live page
tabs = json.loads(urllib.request.urlopen("http://127.0.0.1:9222/json").read())
target_tab = None
for t in tabs:
    if "5000" in t.get("url", ""):
        target_tab = t
        break

if target_tab:
    print("Found tab:", target_tab["url"])
    ws_url = target_tab["webSocketDebuggerUrl"]
    ws = websocket.create_connection(ws_url)
    
    # Evaluate a script in page
    req = {
        "id": 1,
        "method": "Runtime.evaluate",
        "params": {
            "expression": "(() => { try { vjoyDraw(); return 'vjoyDraw success. Canvas size: ' + vjoyCanvas.width + 'x' + vjoyCanvas.height; } catch(e) { return 'Error: ' + e.message; } })()"
        }
    }
    ws.send(json.dumps(req))
    res = json.loads(ws.recv())
    print("Result:", res)
    ws.close()
else:
    print("No 5000 tab found among:", [t.get("url") for t in tabs])
