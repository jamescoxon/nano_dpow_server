import websocket
from websocket import create_connection
import json, time, ctypes

node_server = 'ws://yapraiwallet.space:5000/group/'

try:
    ws = create_connection(node_server)
except:
    print('\nError - unable to connect to backend server\nTry again later or change the server in config.ini')
    sys.exit()

while 1:
    work_to_do = json.loads(str(ws.recv()))
    print(work_to_do)

    try:
        hash = work_to_do['hash']
        lib=ctypes.CDLL("./libmpow.so")
        lib.pow_generate.restype = ctypes.c_char_p
        work = lib.pow_generate(ctypes.c_char_p(hash.encode("utf-8"))).decode("utf-8")
        print(work)
        data = json.dumps({'hash' : hash, 'work' : work, 'address': 'test'})
        ws.send(data)
    except:
        print("Error")

    time.sleep(5)

