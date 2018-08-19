# MIT License
#
# Copyright (c) 2018 James Coxon
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import tornado.httpserver
import tornado.websocket
import tornado.ioloop
import tornado.web
from tornado import gen
from tornado.concurrent import Future, chain_future
import functools
import socket
import requests
import json
import time
import argparse
import random
import rethinkdb

parser = argparse.ArgumentParser()
parser.add_argument("--rai_node_uri", help='rai_nodes uri, usually 127.0.0.1', default='127.0.0.1')
parser.add_argument("--rai_node_port", help='rai_node port, usually 7076', default='7076')
parser.add_argument("--internal_port", help='internal port which nginx proxys', default='5000')

args = parser.parse_args()


rai_node_address = 'http://%s:%s' % (args.rai_node_uri, args.rai_node_port)

wss = []

rethinkdb.set_loop_type("tornado")
connection = rethinkdb.connect( "localhost", 28015)


class Work(tornado.web.RequestHandler):
    @gen.coroutine
    def account_xrb(self, account):
    	# Given a string containing a hex address, encode to public address
    	# format with checksum
    	# each index = binary value, account_lookup['00001'] == '3'
    	account_map = "13456789abcdefghijkmnopqrstuwxyz"
    	account_lookup = {}
    	# populate lookup index for binary string to base-32 string character
    	for i in range(32):
        	account_lookup[BitArray(uint=i,length=5).bin] = account_map[i]
    	# hex string > binary
    	account = BitArray(hex=account)

    	# get checksum
    	h = blake2b(digest_size=5)
    	h.update(account.bytes)
    	checksum = BitArray(hex=h.hexdigest())

    	# encode checksum
    	# swap bytes for compatibility with original implementation
    	checksum.byteswap()
    	encode_check = ''
    	for x in range(0,int(len(checksum.bin)/5)):
        	# each 5-bit sequence = a base-32 character from account_map
        	encode_check += account_lookup[checksum.bin[x*5:x*5+5]]

    	# encode account
    	encode_account = ''
    	while len(account.bin) < 260:
        	# pad our binary value so it is 260 bits long before conversion
        	# (first value can only be 00000 '1' or 00001 '3')
        	account = '0b0' + account
    	for x in range(0,int(len(account.bin)/5)):
        	# each 5-bit sequence = a base-32 character from account_map
        	encode_account += account_lookup[account.bin[x*5:x*5+5]]

    	# build final address string
    	raise gen.Return('xrb_'+encode_account+encode_check)

    @gen.coroutine
    def get_account_from_hash(self, hash):
     	get_account = '{ "action" : "block_account", "hash" : "%s"}' % hash
     	r = requests.post(rai_node_address, data = get_account)
     	resulting_data = r.json()
     	if 'account' in resulting_data:
            account = resulting_data['account']
            raise gen.Return(account)
     	elif 'error' in resulting_data:
            if resulting_data['error'] == 'Block not found':
         	#This maybe a public key for an Open Block, convert to xrb address
                print("Open Block")
                account = self.account_xrb(hash)
                raise gen.Return(account)
     	else:
                raise gen.Return('Error')

    @gen.coroutine
    def validate_work(self, hash, work):
     	get_validation = '{ "action" : "work_validate", "hash" : "%s", "work": "%s" }' % (hash, work)
     	r = requests.post(rai_node_address, data = get_validation)
	#     print(time.strftime("%d/%m/%Y %H:%M:%S") + " " + r.text)
     	resulting_validation = r.json()
     	raise gen.Return(resulting_validation['valid'])

    @gen.coroutine
    def wsSend(self, message):
    	print("Sending via WS")
    	#randomise our wss list then cycle through until we manage to send a message
    	random.shuffle(wss)
    	for ws in wss:
            if not ws.ws_connection.stream.socket:
            	print ("Web socket does not exist anymore!!!")
            	wss.remove(ws)
            else:
            	ws.write_message(message)
            	break

    @gen.coroutine
    def get_work_via_ws(self, hash):
        conn = yield connection

        #1 Send request to websocket clients to process
        print("Send via WS 1")
        print(wss)
        yield self.wsSend('{"hash" : "%s"}' % hash)

    	#2 While we wait lets setup the db entries
        account = yield self.get_account_from_hash(hash)
        if account == 'Error':
            result = '("status" : "error"}'
            raise gen.Return(result)

        else:
            # Check for previous account
            print("Account " + account)
            data = yield rethinkdb.db("pow").table("hashes").filter({"account": account}).run(conn)
            while (yield data.fetch_next()):
                print("update")
                yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == account).update({"hash" : hash, "work": "1"}).run(conn)
                break
            else:
                print("insert")
                yield rethinkdb.db("pow").table("hashes").insert({"account":account, "hash":hash, "work":"1"}).run(conn)
        x = 0
        print("Waiting for work...")
        while x < 20:
            print(x)
            ws_data = yield rethinkdb.db("pow").table("hashes").filter({"hash": hash}).run(conn)
            while (yield ws_data.fetch_next()):
                new_work = yield ws_data.next()
                print(new_work['work'])
                if new_work['work'] != "0" and new_work['work'] != "1":
                    work_output = new_work['work']
                    raise gen.Return(work_output)

            else:
                x = x + 1
                yield gen.sleep(1.0)

        result = '{"status" : "timeout"}'
        raise gen.Return(result)


    @gen.coroutine
    def post(self):
        post_data = json.loads(self.request.body.decode('utf-8'))
        hash = post_data['hash']
        key = post_data['key']
        print(time.strftime("%d/%m/%Y %H:%M:%S") + " " + hash + " " + key)

        conn = yield connection

        work_output = ' '
        #1 Check key is valid

        #2 Do we have hash in db?
        data = yield rethinkdb.db("pow").table("hashes").filter({"hash": hash}).run(conn)
        while (yield data.fetch_next()):
            precache_work = yield data.next()
            print(precache_work)
            print('Found cached work value %s' % precache_work['work'])
            work_output = precache_work['work']
            if work_output == "0" or work_output == "1":
                 print("Empty Work, get new")
                 work_output = yield self.get_work_via_ws(hash)
                 print(work_output)
                 yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['hash'] == hash).update({"work": work_output}).run(conn)
            break
        else:
        #3 If not then request pow via websockets
            print('Not in db, calculating work...')
            work_output = yield self.get_work_via_ws(hash)
            print(work_output)
            yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['hash'] == hash).update({"work": work_output}).run(conn)

        #4 Return work
        return_json = '{"work" :"%s"}' % work_output
        print(time.strftime("%d/%m/%Y %H:%M:%S") + " " + return_json)
        self.write(return_json)

class WSHandler(tornado.websocket.WebSocketHandler):

    def validate_work(self, hash, work):
     	get_validation = '{ "action" : "work_validate", "hash" : "%s", "work": "%s" }' % (hash, work)
     	r = requests.post(rai_node_address, data = get_validation)
	#     print(time.strftime("%d/%m/%Y %H:%M:%S") + " " + r.text)
     	resulting_validation = r.json()
     	return resulting_validation['valid']

    def open(self):
        print ('new connection')
        if self not in wss:
            wss.append(self)
        print(self)

    @gen.coroutine
    def on_message(self, message):
        print("1")
        message =  message

        print ('message received:  %s' % message)
        try:
            ws_data = json.loads(message)
            hash = ws_data['hash']
            work = ws_data['work']
            payout_account = ws_data['address']
            #insert in to db


            if self.validate_work(hash, work) == "1":
                print("Saving data")
                conn = yield connection
                print("hash " + hash + " work: " + work)
                yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['hash'] == hash).update({"work": work}).run(conn)
                print("Work validated")

            else:
                print("failed")

        except:
            print("error")

    def on_close(self):
        print ('connection closed')
        if self in wss:
            wss.remove(self)

    def check_origin(self, origin):
        return True

application = tornado.web.Application([
    (r'/group/', WSHandler),
    (r"/work", Work),
])

if __name__ == "__main__":
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(int(args.internal_port))
    myIP = socket.gethostbyname(socket.gethostname())
    print( '*** Websocket Server Started at %s***' % myIP)
    tornado.ioloop.IOLoop.instance().start()
