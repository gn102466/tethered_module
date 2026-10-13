import urllib.request
html = urllib.request.urlopen("http://127.0.0.1:5000/").read().decode("utf-8", errors="ignore")
idx_canvas = html.find('id="vjoyCanvas"')
idx_js = html.find('const vjoyCanvas = document.getElementById')
print("Canvas pos:", idx_canvas, "JS pos:", idx_js)
