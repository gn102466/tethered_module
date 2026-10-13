import socket, time
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("192.168.1.201", 5005))  # target IP:port
s.sendall(b"hello over rs485\n")
print(s.recv(1024))