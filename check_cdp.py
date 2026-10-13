import json, urllib.request, subprocess, time
from pathlib import Path

edge_path = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
proc = subprocess.Popen([edge_path, "--headless=new", "--remote-debugging-port=9222", "http://127.0.0.1:5000/"])
proc = subprocess.Popen([edge_path, "--headless=new", "--remote-debugging-port=9222", "http://12"
"7.0.0.1:5000/"])
time.sleep(2)
try:
    tabs = json.loads(urllib.request.urlopen("http://127.0.0.1:9222/json").read())
    print("Tabs found:", len(tabs))
    print(tabs[0]["title"], tabs[0]["url"])
finally:
    proc.terminate()
