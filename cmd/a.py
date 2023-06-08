import requests

rsp = requests.get("http://pcdn.dq.baidu.com/v3/accesslog", stream=True)
print (rsp.raw._connection.sock.getpeername()[0])