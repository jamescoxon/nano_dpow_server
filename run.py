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

def print_time(message):
    print(time.strftime("%d/%m/%Y %H:%M:%S") + " " + str(message))

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
        get_block = '{ "action" : "block", "hash" : "%s"}' % hash
        print(get_block)
        r = requests.post(rai_node_address, data = get_block)
        print_time(r.text)

        get_account = '{ "action" : "block_account", "hash" : "%s"}' % hash
        print(get_account)
        r = requests.post(rai_node_address, data = get_account)
        print_time(r.text)

        resulting_data = r.json()
        if 'account' in resulting_data:
            account = resulting_data['account']
            raise gen.Return(account)
        elif 'error' in resulting_data:
            if resulting_data['error'] == 'Block not found':
         	#This maybe a public key for an Open Block, convert to xrb address
                print_time("Open Block")
                account = yield self.account_xrb(hash)
                raise gen.Return(account)
        else:
                raise gen.Return('Error')

    @gen.coroutine
    def validate_work(self, hash, work):
     	get_validation = '{ "action" : "work_validate", "hash" : "%s", "work": "%s" }' % (hash, work)
     	r = requests.post(rai_node_address, data = get_validation)
     	resulting_validation = r.json()
     	raise gen.Return(resulting_validation['valid'])

    @gen.coroutine
    def wsSend(self, message):
        print_time("Sending via WS Demand")
        #randomise our wss list then cycle through until we manage to send a message
        random.shuffle(wss_demand)
        print_time(wss_demand)
        print_time(wss_work)

        for ws in wss_demand:
            if not ws.ws_connection.stream.socket:
                print_time ("Web socket does not exist anymore!!!")
                wss_demand.remove(ws)
                wss_work.remove(ws)
            else:
                if ws not in wss_work:
                    ws.write_message(message)
                    wss_work.append(ws)
                    return '{"status":"sent"}'
                else:
                    print_time(str(ws) + " already in use")
        return '{"status":"failed"}'

    @gen.coroutine
    def get_work_via_ws(self, hash):
        conn = yield connection

        #1 Send request to websocket clients to process
        send_result = yield self.wsSend('{"hash" : "%s", "type" : "urgent"}' % hash)

    	#2 While we wait lets setup the db entries
        account = yield self.get_account_from_hash(hash)
        #print_time(account)
        if account == 'Error':
            result = '("status" : "error"}'
            raise gen.Return(result)

        else:
            # Check for previous account
            print_time("Account %s" % account)
            data = yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == account).run(conn)
            while (yield data.fetch_next()):
                if send_result == '{"status" : "failed"}':
                    result = '{"status" : "no clients"}'
                    yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == account).update({"hash" : hash, "work": "0"}).run(conn)
                    raise gen.Return(result)
                else:
                    yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == account).update({"hash" : hash, "work": "1"}).run(conn)
                    break
            else:
                if send_result == '{"status" : "failed"}':
                    result = '{"status" : "no clients"}'
                    yield rethinkdb.db("pow").table("hashes").insert({"account":account, "hash":hash, "work":"1"}).run(conn)
                    raise gen.Return(result)
                else:
                    yield rethinkdb.db("pow").table("hashes").insert({"account":account, "hash":hash, "work":"1"}).run(conn)

        x = 0
        print_time("Waiting for work...")
        while x < 20:
#            print_time(x)
            ws_data = yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['hash'] ==  hash).nth(0).default(False).run(conn)
            if ws_data != False:
                new_work = ws_data
                if new_work['work'] != "0" and new_work['work'] != "1":
                    #print_time(new_work['work'])
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
        print_time(hash + " " + key)

        conn = yield connection

        work_output = ' '
        #1 Check key is valid
        if 'key' in post_data:
            print_time("found API key")
            key = post_data['key']
            data = yield rethinkdb.db("pow").table("api_keys").filter({"api_key": key}).nth(0).default(False).run(conn)
            if data == False:
                print_time("incorrect API key")
                return_json= '{"status" : "incorrect key"}'
                print_time(return_json)
                self.write(return_json)
                return
            else:
                print_time("Correct API key from %s - continue" % data['username'])
                new_count = int(data['count']) + 1
                yield rethinkdb.db("pow").table("api_keys").filter(rethinkdb.row['api_key'] == key).update({"count": new_count}).run(conn)
        else:
            print_time("no API key")
            return_json= '{"status" : "no key"}'
            print_time(return_json)
            self.write(return_json)
            return
        #2 Do we have hash in db?
        data = yield rethinkdb.db("pow").table("hashes").filter({"hash": hash}).nth(0).default(False).run(conn)
#        data = yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['hash'] == hash).nth(0).run(conn)
#        print_time(data)
        if data != False:
            precache_work = data
            print_time('Found cached work value %s' % precache_work['work'])
            work_output = precache_work['work']
            if work_output == "0" or work_output == "1":
                 print_time("Empty Work, get new")
                 work_output = yield self.get_work_via_ws(hash)
        else:
        #3 If not then request pow via websockets
            print_time('Not in db, calculating work...')
            work_output = yield self.get_work_via_ws(hash)

        #4 Return work
        if work_output == '{"status" : "timeout"}':
            return_json = work_output
        elif work_output == '{"status" : "no clients"}':
            return_json = work_output
        else:
            return_json = '{"work" :"%s"}' % work_output

        print_time(return_json)
        self.write(return_json)

class WSHandler(tornado.websocket.WebSocketHandler):

    def __repr__(self):
        return 'busy' if self in wss_work else 'free'

    def validate_work(self, hash, work):
     	get_validation = '{ "action" : "work_validate", "hash" : "%s", "work": "%s" }' % (hash, work)
     	r = requests.post(rai_node_address, data = get_validation)
     	resulting_validation = r.json()
     	return resulting_validation['valid']

    def open(self):
        print_time ('new connection')
        if self not in wss_demand:
            wss_demand.append(self)
        print_time(self)

    @gen.coroutine
    def on_message(self, message):
        message =  message

        print_time ('message: %s' % message)
        try:
            ws_data = json.loads(message)

            if 'work_type' in ws_data:
                print_time("Found work_type")
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
                  print_time("hash " + hash + " work: " + work)
                  yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['hash'] == hash).update({"work": work}).run(conn)
                  if hash in hash_to_precache:
                      hash_to_precache.remove(hash)

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
                      print_time("Removing %s from wss_work" % self)
                      wss_work.remove(self)


              else:
                  print_time("failed")

        except:
            print_time("error")

    def on_close(self):
        print_time ('connection closed')
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
#    print_time(hash_to_precache)
    for hash in hash_to_precache:
        hash_count = hash_count + 1
#        print_time("Got work to push")
#        print_time(wss_precache)
#        print_time(wss_work)
        random.shuffle(wss_precache)
        print_time(wss_precache)
        print_time(wss_work)
        for work_clients in wss_precache:
#            print_time("Sending via WS Precache")
            try:
                if not work_clients.ws_connection.stream.socket:
                    print_time ("Web socket does not exist anymore!!!")
                    wss_precache.remove(work_clients)
                    wss_work.remove(work_clients)
                else:
                    if work_clients not in wss_work:
                            work_count = work_count + 1
                            message = '{"hash" : "%s", "type" : "precache"}' % hash
                            work_clients.write_message(message)
                            wss_work.append(work_clients)
                            try:
                                hash_to_precache.remove(hash)
                            except:
                                print_time("Failed to remove hash from precache list")
                                pass
                            break
  #                  else:
 #                           print_time(str(work_clients) + " already in use")
            except:
                    try:
                        wss_precache.remove(work_clients)
                        wss_work.remove(work_clients)
                        break
                    except:
                        pass

 #   print_time("Work Count: %d, Hash Count: %d" % (hash_count, work_count))
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
 #   print_time("precache_update")
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
#             print_time("%s : Request too long, reset" % user['account'])
             yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == user['account']).update({"work": "0"}).run(conn)
             if user['hash'] not in hash_to_precache:
                 hash_to_precache.append(user['hash'])
         elif results['frontier'] == user['hash']:
             up_to_date = up_to_date + 1
             if user['work'] == "0":
                 if user['hash'] not in hash_to_precache:
                     not_in_queue = not_in_queue + 1
#                     print_time('hash not in queue, adding')
                     if user['hash'] not in hash_to_precache:
                         hash_to_precache.append(user['hash'])
         else:
             not_up_to_data = not_up_to_data + 1
#             print_time("%s : Not upto date, precache" % user['account'])
             if user['hash'] not in hash_to_precache:
                 hash_to_precache.append(user['hash'])
             yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == user['account']).update({"work": "0", "hash": results['frontier']}).run(conn)
     except:
         get_account = '{ "action" : "block_account", "hash" : "%s"}' % user['hash']
         print(get_account)
         r = requests.post(rai_node_address, data = get_account)
         print_time(r.text)

         delete_error = delete_error + 1
         print_time("Error - deleting %s" % user['id'])
         yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['id'] == user['id']).delete().run(conn)
         pass

    print_time("Count: %d, Work: %d, Up to date:  %d, Not in queue: %d, Not up to date: %d, Delete error: %d" % ( count_updates, work_count, up_to_date, not_in_queue, not_up_to_data, delete_error))
    #tornado.ioloop.IOLoop.instance().add_timeout(datetime.timedelta(seconds=60), precache_update)

@gen.coroutine
def setupdb():
    print_time("Update DB")
    conn = yield connection
    data = yield rethinkdb.db("pow").table("hashes").run(conn)
    while (yield data.fetch_next()):
        documents = yield data.next()
        if documents['work'] == "0":
            if documents['hash'] not in hash_to_precache:
                hash_to_precache.append(documents['hash'])


if __name__ == "__main__":
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(int(args.internal_port))
    myIP = socket.gethostbyname(socket.gethostname())

    tornado.ioloop.IOLoop.current().run_sync(setupdb)

    print_time( '*** Websocket Server Started at %s***' % myIP)
#    main_loop.add_timeout(datetime.timedelta(seconds=10), precache_update)
#    main_loop.add_timeout(datetime.timedelta(seconds=10), push_precache)
    pc = tornado.ioloop.PeriodicCallback(precache_update, 30000)
    pc.start()
    push = tornado.ioloop.PeriodicCallback(push_precache, 5000)
    push.start()
    #tornado.ioloop.IOLoop.instance().start()
    main_loop = tornado.ioloop.IOLoop.instance()

    main_loop.start()
