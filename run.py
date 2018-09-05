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
from bitstring import BitArray
from pyblake2 import blake2b
import rethinkdb
import datetime

parser = argparse.ArgumentParser()
parser.add_argument("--rai_node_uri", help='rai_nodes uri, usually 127.0.0.1', default='127.0.0.1')
parser.add_argument("--rai_node_port", help='rai_node port, usually 7076', default='7076')
parser.add_argument("--internal_port", help='internal port which nginx proxys', default='5000')

args = parser.parse_args()


rai_node_address = 'http://%s:%s' % (args.rai_node_uri, args.rai_node_port)

wss_demand = []
wss_precache = []
wss_work = []
hash_to_precache = []

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
        print(r.text)

        resulting_data = r.json()
        if 'account' in resulting_data:
            account = resulting_data['account']
            raise gen.Return(account)
        elif 'error' in resulting_data:
            if resulting_data['error'] == 'Block not found':
         	#This maybe a public key for an Open Block, convert to xrb address
                print("Open Block")
                account = yield self.account_xrb(hash)
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
        print("Sending via WS Demand")
        #randomise our wss list then cycle through until we manage to send a message
        random.shuffle(wss_demand)
        print(wss_demand)
        print(wss_work)

        for ws in wss_demand:
            if not ws.ws_connection.stream.socket:
                print ("Web socket does not exist anymore!!!")
                wss_demand.remove(ws)
                wss_work.remove(ws)
            else:
                if ws not in wss_work:
                    ws.write_message(message)
                    wss_work.append(ws)
                    return '{"status":"sent"}'
                else:
                    print(str(ws) + " already in use")
        return '{"status":"failed"}'

    @gen.coroutine
    def get_work_via_ws(self, hash):
        conn = yield connection

        #1 Send request to websocket clients to process
        send_result = yield self.wsSend('{"hash" : "%s", "type":"urgent"}' % hash)

    	#2 While we wait lets setup the db entries
        account = yield self.get_account_from_hash(hash)
        #print(account)
        if account == 'Error':
            result = '("status" : "error"}'
            raise gen.Return(result)

        else:
            # Check for previous account
            print("Account %s" % account)
            data = yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == account).run(conn)
            while (yield data.fetch_next()):
                if send_result == '{"status":"failed"}':
                    result = '{"status" : "no clients"}'
                    yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == account).update({"hash" : hash, "work": "0"}).run(conn)
                    raise gen.Return(result)
                else:
                    yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == account).update({"hash" : hash, "work": "1"}).run(conn)
                    break
            else:
                if send_result == '{"status":"failed"}':
                    result = '{"status" : "no clients"}'
                    yield rethinkdb.db("pow").table("hashes").insert({"account":account, "hash":hash, "work":"1"}).run(conn)
                    raise gen.Return(result)
                else:
                    yield rethinkdb.db("pow").table("hashes").insert({"account":account, "hash":hash, "work":"1"}).run(conn)

        x = 0
        print("Waiting for work...")
        while x < 20:
#            print(x)
            ws_data = yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['hash'] ==  hash).nth(0).default(False).run(conn)
            if ws_data != False:
                new_work = ws_data
                if new_work['work'] != "0" and new_work['work'] != "1":
                    #print(new_work['work'])
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
        hash = post_data['hash'].upper()
        key = ''
        print(time.strftime("%d/%m/%Y %H:%M:%S") + " " + hash + " " + key)

        conn = yield connection

        work_output = ' '
        #1 Check key is valid
        if 'key' in post_data:
            print("found API key")
            key = post_data['key']
            data = yield rethinkdb.db("pow").table("api_keys").filter({"api_key": key}).nth(0).default(False).run(conn)
            if data == False:
                print("incorrect API key")
                return_json= '{"status" : "incorrect key"}'
                print(time.strftime("%d/%m/%Y %H:%M:%S") + " " + return_json)
                self.write(return_json)
            else:
                print("Correct API key - continue")
        else:
            print("no API key")
            return_json= '{"status" : "no key"}'
            print(time.strftime("%d/%m/%Y %H:%M:%S") + " " + return_json)
            self.write(return_json)

        #2 Do we have hash in db?
        data = yield rethinkdb.db("pow").table("hashes").filter({"hash": hash}).nth(0).default(False).run(conn)
#        data = yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['hash'] == hash).nth(0).run(conn)
#        print(data)
        if data != False:
            precache_work = data
            print('Found cached work value %s' % precache_work['work'])
            work_output = precache_work['work']
            if work_output == "0" or work_output == "1":
                 print("Empty Work, get new")
                 work_output = yield self.get_work_via_ws(hash)
        else:
        #3 If not then request pow via websockets
            print('Not in db, calculating work...')
            work_output = yield self.get_work_via_ws(hash)

        #4 Return work
        if work_output == '{"status" : "timeout"}':
            return_json = work_output
        elif work_output == '{"status" : "no clients"}':
            return_json = work_output
        else:
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
        if self not in wss_demand:
            wss_demand.append(self)
        print(self)

    @gen.coroutine
    def on_message(self, message):
        message =  message

        print ('message received:  %s' % message)
        try:
            ws_data = json.loads(message)

            if 'work_type' in ws_data:
                print("Found work_type")
                if ws_data['work_type'] == 'any':
                    #Add to both demand and precache
                    #wss_demand.append(self)
                    wss_precache.append(self)
                elif ws_data['work_type'] == 'precache_only':
                    #Add to precache and remove from demand
                    wss_precache.append(self)
                    wss_demand.remove(self)
                #If its urgent_only we don't change anything
            else:
              hash = ws_data['hash'].upper()
              work = ws_data['work']
              payout_account = ws_data['address'].lower()
              #insert in to db


              if self.validate_work(hash, work) == "1":
                  conn = yield connection
                  print("hash " + hash + " work: " + work)
                  yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['hash'] == hash).update({"work": work}).run(conn)

                  #Add work record to client database to allow payouts
                  clients_data = yield rethinkdb.db("pow").table("clients").filter(rethinkdb.row['account'] == payout_account).nth(0).default(False).run(conn)
                  if clients_data != False:
                      client = clients_data
                      count = int(client['count'])
                      total_count = count + 1
                      yield rethinkdb.db("pow").table("clients").filter(rethinkdb.row['account'] == payout_account).update({"count": total_count, "time":time.time()}).run(conn)
                  else:
                      yield rethinkdb.db("pow").table("clients").insert({"account":payout_account, "count":1, "time":time.time()}).run(conn)

                  #Remove from work list
                  if self in wss_work:
                      print("Removing %s from wss_work" % self)
                      wss_work.remove(self)


              else:
                  print("failed")

        except:
            print("error")

    def on_close(self):
        print ('connection closed')
        if self in wss_demand:
            wss_demand.remove(self)
            try:
                wss_work.remove(self)
            except:
                pass

    def check_origin(self, origin):
        return True


application = tornado.web.Application([
    (r'/group/', WSHandler),
    (r"/work", Work),
])

@gen.coroutine
def push_precache():
    hash_count = 0
    work_count = 0
    for hash in hash_to_precache:
        hash_count = hash_count + 1
#        print("Got work to push")
#        print(wss_precache)
#        print(wss_work)
        message = '{"hash" : "%s", "type" : "precache"}' % hash

        for work_clients in wss_precache:
#            print("Sending via WS Precache")
            #randomise our wss list then cycle through until we manage to send a message
            try:
                if not work_clients.ws_connection.stream.socket:
                    print ("Web socket does not exist anymore!!!")
                    wss_precache.remove(work_clients)
                    wss_work.remove(work_clients)
                else:
                    if work_clients not in wss_work:
                            work_count = work_count + 1
                            work_clients.write_message(message)
                            wss_work.append(work_clients)
                            try:
                                hash_to_precache.remove(hash)
                            except:
                                pass
  #                  else:
 #                           print(str(work_clients) + " already in use")
            except:
                    wss_precache.remove(work_clients)
                    wss_work.remove(work_clients)

    print("Work Count: %d, Hash Count: %d" % (hash_count, work_count))
    #tornado.ioloop.IOLoop.instance().add_timeout(datetime.timedelta(seconds=5), push_precache)

@gen.coroutine
def precache_update():
    count_updates = 0
    work_count = 0
    up_to_date = 0
    not_in_queue = 0
    not_up_to_data = 0
    delete_error = 0
    conn = yield connection
 #   print("precache_update")
    precache_data = yield rethinkdb.db("pow").table("hashes").run(conn)
    while (yield precache_data.fetch_next()):
     try:
         user = yield precache_data.next()
         count_updates = count_updates + 1
         get_frontier = '{ "action" : "account_info", "account" : "%s" }' % user['account']
         r = requests.post(rai_node_address, data = get_frontier)
         results = r.json()
         if user['work'] == "1":
             #Reset work as taken too long
             work_count = work_count + 1
#             print("%s : Request too long, reset" % user['account'])
             yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == user['account']).update({"work": "0"}).run(conn)
             hash_to_precache.append(user['hash'])
         elif results['frontier'] == user['hash']:
             up_to_date = up_to_date + 1
             if user['work'] == "0":
                 if user['hash'] not in hash_to_precache:
                     not_in_queue = not_in_queue + 1
#                     print('hash not in queue, adding')
                     hash_to_precache.append(user['hash'])
         else:
             not_up_to_data = not_up_to_data + 1
#             print("%s : Not upto date, precache" % user['account'])
             hash_to_precache.append(user['hash'])
             yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == user['account']).update({"work": "0", "hash": results['frontier']}).run(conn)
     except:
         delete_error = delete_error + 1
#         print("Error - deleting %s" % user['id'])
         yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['id'] == user['id']).delete().run(conn)
         pass

    print("Count: %d, Work: %d, Up to date:  %d, Not in queue: %d, Not up to date: %d, Delete error: %d" % ( count_updates, work_count, up_to_date, not_in_queue, not_up_to_data, delete_error))
    #tornado.ioloop.IOLoop.instance().add_timeout(datetime.timedelta(seconds=60), precache_update)

@gen.coroutine
def setupdb():
    print("Update DB")
    conn = yield connection
    data = yield rethinkdb.db("pow").table("hashes").run(conn)
    while (yield data.fetch_next()):
        documents = yield data.next()
        if documents['work'] == "0":
            hash_to_precache.append(documents['hash'])


if __name__ == "__main__":
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(int(args.internal_port))
    myIP = socket.gethostbyname(socket.gethostname())

    tornado.ioloop.IOLoop.current().run_sync(setupdb)

    print( '*** Websocket Server Started at %s***' % myIP)
#    main_loop.add_timeout(datetime.timedelta(seconds=10), precache_update)
#    main_loop.add_timeout(datetime.timedelta(seconds=10), push_precache)
    pc = tornado.ioloop.PeriodicCallback(precache_update, 30000)
    pc.start()
    push = tornado.ioloop.PeriodicCallback(push_precache, 5000)
    push.start()
    #tornado.ioloop.IOLoop.instance().start()
    main_loop = tornado.ioloop.IOLoop.instance()

    main_loop.start()
