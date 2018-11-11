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

import nanoutils as nano
import tornado.httpserver
import tornado.websocket
import tornado.ioloop
import tornado.web
from tornado import gen
import socket
import requests
import json
import time
import argparse
import random
import rethinkdb
import hashlib
from enum import Enum

# import datetime
# from tornado.concurrent import Future, chain_future

parser = argparse.ArgumentParser()
parser.add_argument("--rai_node_uri", help='rai_nodes uri, usually 127.0.0.1', default='127.0.0.1')
parser.add_argument("--rai_node_port", help='rai_node port, usually 7076', default='7076')
parser.add_argument("--internal_port", help='internal port which nginx proxys', default='5000')
parser.add_argument("-v", "--verbose", help='more prints to help debugging', action='store_true')

args = parser.parse_args()

rai_node_address = 'http://{uri}:{port}'.format(uri=args.rai_node_uri, port=args.rai_node_port)

wss_demand = []
wss_precache = []
wss_work = []
hash_to_precache = []

worker_counter = 0

rethinkdb.set_loop_type("tornado")
connection = rethinkdb.connect("localhost", 28015)

def print_time_debug(message):
    if args.verbose:
        print("(DEBUG)", end=' ')
        print_time(message)


def print_time(message):
    print(time.strftime("%d/%m/%Y %H:%M:%S") + " " + str(message))


def print_lists(work=False, demand=False, precache=False):
    if not (work or demand or precache):
        return
    s = "State of lists:"
    if work:
        s += "\n\t\t\twss_work: {}".format(wss_work)
    if demand:
        s += "\n\t\t\twss_demand: {}".format(wss_demand)
    if precache:
        s += "\n\t\t\twss_precache: {}".format(wss_precache)
    print_time(s)


class WorkState(Enum):
    needs = "0"
    doing = "1"


class Work(tornado.web.RequestHandler):
    def data_received(self, chunk):
        pass

    @gen.coroutine
    def account_xrb(self, hex_acc):
        raise gen.Return(nano.hex_to_account(hex_acc))

    @gen.coroutine
    def get_account_from_hash(self, hash_hex):

        get_account = '{ "action" : "block_account", "hash" : "%s"}' % hash_hex
        print_time_debug("action block_account request:\n{}".format(get_account))
        r = requests.post(rai_node_address, data=get_account)
        print_time_debug("action block_account response:\n{}".format(r.text))

        resulting_data = r.json()
        if 'account' in resulting_data:
            account = resulting_data['account']
            raise gen.Return(account)
        elif 'error' in resulting_data:
            if resulting_data['error'] == 'Block not found':
                # This maybe a public key for an Open Block, convert to xrb address
                print_time("Open Block")
                account = yield self.account_xrb(hash_hex)
                print_time("The account is {}".format(account))
                raise gen.Return(account)
            else:  # other errors, for instance invalid hash
                raise gen.Return('Error')
        else:
            raise gen.Return('Error')

    @gen.coroutine
    def ws_demand(self, message):
        print_time("Sending via WS Demand")
        # randomise our wss list then cycle through until we manage to send a message
        random.shuffle(wss_demand)
        print_lists(work=1, demand=1)

        for ws in wss_demand:
            if not ws.ws_connection.stream.socket:
                print_time("Web socket does not exist anymore!!!")
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
    def get_work_via_ws(self, hash_hex):
        conn = yield connection

        # Get account to setup DB entries and check if invalid hash
        account = yield self.get_account_from_hash(hash_hex)
        if account == 'Error':
            result = '{"status" : "bad hash"}'
            raise gen.Return(result)

        # Get appropriate threshold value
        # TODO after prioritization PoW is implemented, calculate here an appropriate multiplier
        multiplier = 1.0
        threshold = nano.threshold_multiplier(nano.NANO_DIFFICULTY, multiplier)
        threshold_str = nano.threshold_to_str(threshold)

        # Send request to websocket clients to process
        send_result = yield self.ws_demand('{"hash" : "%s", "type" : "urgent", "threshold" : "%s"}' % (hash_hex,threshold_str))

        # Check for previous account
        print_time("Account %s" % account)
        data = yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == account).run(conn)
        while (yield data.fetch_next()):
            if send_result == '{"status" : "failed"}':
                result = '{"status" : "no clients"}'
                yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == account).update(
                    {"hash": hash_hex, "work": WorkState.needs.value, "threshold": threshold_str}).run(conn)
                raise gen.Return(result)
            else:
                yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == account).update(
                    {"hash": hash_hex, "work": WorkState.doing.value, "threshold": threshold_str}).run(conn)
                break
        else:
            if send_result == '{"status" : "failed"}':
                result = '{"status" : "no clients"}'
                yield rethinkdb.db("pow").table("hashes").insert(
                    {"account": account, "hash": hash_hex, "work": WorkState.needs.value, "threshold": threshold_str}).run(conn)
                raise gen.Return(result)
            else:
                yield rethinkdb.db("pow").table("hashes").insert(
                    {"account": account, "hash": hash_hex, "work": WorkState.doing.value, "threshold": threshold_str}).run(conn)

        x = 0
        print_time("Waiting for work...")
        while x < 20:
            #            print_time(x)
            ws_data = yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['hash'] == hash_hex).nth(0).default(False).run(conn)
            if ws_data:
                new_work = ws_data
                if new_work['work'] != WorkState.needs.value and new_work['work'] != WorkState.doing.value:
                    # print_time(new_work['work'])
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
        if 'hash' in post_data:
            hash_hex = post_data['hash'].upper()
        else:
            return_json = '{"status" : "no hash"}'
            print_time(return_json)
            self.write(return_json)
            return

        conn = yield connection

        # 1 Check key is valid
        if 'key' in post_data:
            print_time("found API key")
            key = post_data['key']
            key_hashed = hashlib.sha512(key.encode('utf-8')).hexdigest()
            data = yield rethinkdb.db("pow").table("api_keys").filter({"api_key": key_hashed}).nth(0).default(False).run(conn)
            if not data:
                print_time("incorrect API key")
                return_json = '{"status" : "incorrect key"}'
                print_time(return_json)
                self.write(return_json)
                return
            else:
                print_time("Correct API key from %s - continue" % data['username'])
                new_count = int(data['count']) + 1
                yield rethinkdb.db("pow").table("api_keys").filter(rethinkdb.row['api_key'] == key_hashed).update(
                    {"count": new_count}).run(conn)
        else:
            print_time("no API key")
            return_json = '{"status" : "no key"}'
            print_time(return_json)
            self.write(return_json)
            return
        # 2 Do we have hash in db?
        data = yield rethinkdb.db("pow").table("hashes").filter({"hash": hash_hex}).nth(0).default(False).run(conn)
        #        data = yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['hash'] == hash).nth(0).run(conn)
        #        print_time(data)
        if data:
            precache_work = data
            print_time('Found cached work value %s' % precache_work['work'])
            work_output = precache_work['work']
            if work_output == WorkState.needs.value or work_output == WorkState.doing.value:
                print_time("Empty work, get new")
                work_output = yield self.get_work_via_ws(hash_hex)
        else:
            # 3 If not then request pow via websockets
            print_time('Not in DB, getting on demand...')
            work_output = yield self.get_work_via_ws(hash_hex)

        # 4 Return work
        if work_output == '{"status" : "timeout"}':
            return_json = work_output
        elif work_output == '{"status" : "no clients"}':
            return_json = work_output
        elif work_output == '{"status" : "bad hash"}':
            return_json = work_output
        else:
            return_json = '{"work" :"%s"}' % work_output

        print_time(return_json)
        self.write(return_json)


class WSHandler(tornado.websocket.WebSocketHandler):

    worker_counter = 0

    def __init__(self, *args, **kwargs):
        WSHandler.worker_counter += 1
        self.id = WSHandler.worker_counter
        super().__init__(*args, **kwargs)

    def __repr__(self):
        return '{id} ({state})'.format(id=self.id or '?',
                                       state='busy' if self in wss_work else 'free')

    @staticmethod
    def validate_work(hash_hex, work, threshold):
        get_validation = '{ "action" : "work_validate", "hash" : "%s", "work": "%s", "threshold": "%s" }' % (hash_hex, work, threshold)
        r = requests.post(rai_node_address, data=get_validation)
        resulting_validation = r.json()
        if 'error' in resulting_validation:
            raise gen.Return('Error in validation: {}'.format(resulting_validation))
        return int(resulting_validation['valid'])

    def open(self):
        print_time('New worker connected - {}'.format(self.id))
        if self not in wss_demand:
            wss_demand.append(self)

    @gen.coroutine
    def on_message(self, message):
        print_time('Message from worker {}: {}'.format(self.id, message))
        try:
            ws_data = json.loads(message)

            if 'work_type' in ws_data:
                # handle setup message for work type
                work_type = ws_data['work_type']
                print_time("Found work_type -> {}".format(work_type))
                try:
                    self.update_work_type(work_type)
                    self.write_message('{"status": "success"}')
                except Exception as e:
                    print_time(e)
                    self.write_message('{"status": "error", "description": "%s"}' % e)
            else:
                # handle work message
                hash_hex = ws_data['hash'].upper()
                work = ws_data['work']
                payout_account = ws_data['address'].lower()

                if work == 'error':
                    raise Exception("'Something wrong with the client, work returned as error'")

                # check the threshold at which this was computed
                # defaults to NANO_DIFFICULTY if not found (e.g. early entries in DB)
                conn = yield connection
                data = yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['hash'] == hash_hex).nth(0).default(False).run(conn)
                if data:
                    threshold_str = data.get("threshold") or nano.threshold_to_str(nano.NANO_DIFFICULTY)
                else:
                    threshold_str = nano.threshold_to_str(nano.NANO_DIFFICULTY)
                print_time_debug("Validating hash {},  work {}, threshold {}".format(hash_hex, work, threshold_str))

                # validate the work from client
                valid = self.validate_work(hash_hex, work, threshold_str)
                if valid:
                    yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['hash'] == hash_hex).update(
                        {"work": work}).run(conn)
                    if hash_hex in hash_to_precache:
                        hash_to_precache.remove(hash_hex)

                    # Add work record to client database to allow payouts
                    clients_data = yield rethinkdb.db("pow").table("clients").filter(
                        rethinkdb.row['account'] == payout_account).nth(0).default(False).run(conn)
                    if clients_data:
                        client = clients_data
                        count = int(client['count'])
                        total_count = count + 1
                        yield rethinkdb.db("pow").table("clients").filter(
                            rethinkdb.row['account'] == payout_account).update(
                            {"count": total_count, "time": time.time()}).run(conn)
                    else:
                        yield rethinkdb.db("pow").table("clients").insert(
                            {"account": payout_account, "count": 1, "time": time.time()}).run(conn)

                    # Remove from work list
                    if self in wss_work:
                        print_time("Removing {} from wss_work".format(self))
                        wss_work.remove(self)

                else:
                    raise Exception("Failed to validate work - {} for worker {}".format(validation, self))

        except Exception as e:
            # TODO probably a good place to give some kind of punishment e.g. 1 minute without getting work
            print_time("Error {}".format(e))
            if self in wss_work:
                print_time("Removing {} from wss_work after exception".format(self))
                wss_work.remove(self)

    def update_work_type(self, work_type):
        # remove from any lists
        self.remove_from_lists()

        if work_type == 'any':
            # Add to both demand and precache
            wss_demand.append(self)
            wss_precache.append(self)
        elif work_type == 'precache_only':
            # Add to precache
            wss_precache.append(self)
        elif work_type == 'urgent_only':
            # Add to demand
            wss_demand.append(self)
        else:
            raise Exception('Invalid work type {}'.format(work_type))

    def on_close(self):
        print_time('Worker disconnected - {}'.format(self.id))
        self.remove_from_lists()

    def remove_from_lists(self):
        for l in [wss_work, wss_demand, wss_precache]:
            if self in l:
                l.remove(self)

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

    # Get appropriate threshold value
    # TODO what is a good threshold value for precaching?
    multiplier = 1.0
    threshold = nano.threshold_multiplier(nano.NANO_DIFFICULTY, multiplier)
    threshold_str = nano.threshold_to_str(threshold)

    for hash_hex in hash_to_precache:
        hash_count = hash_count + 1
        print_time("Got precache work to push")
        random.shuffle(wss_precache)
        print_lists(work=1, precache=1)
        hash_handled = False
        for work_clients in wss_precache:  # type: WSHandler
            print_time("Sending via WS Precache")
            try:
                if not work_clients.ws_connection.stream.socket:
                    print_time("Web socket does not exist anymore!!!")
                    wss_precache.remove(work_clients)
                    wss_work.remove(work_clients)
                else:
                    if work_clients not in wss_work:
                        work_count = work_count + 1
                        message = '{"hash" : "%s", "type" : "precache", "threshold" : "%s"}' % (hash_hex, threshold_str)
                        work_clients.write_message(message)
                        wss_work.append(work_clients)
                        print_time_debug(message)
                        try:
                            hash_to_precache.remove(hash_hex)
                            hash_handled = True
                        except Exception as e:
                            print_time("Failed to remove hash from precache list: {}".format(e))
                            pass
                        break
            #                  else:
            #                           print_time(str(work_clients) + " already in use")
            except Exception as e:
                print_time('Error when sending via WS precache: {}'.format(e))
                if work_clients in wss_precache:
                    wss_precache.remove(work_clients)
                    print_time('Client {} removed from precache work'.format(work_clients.id))
                if work_clients in wss_work:
                    wss_work.remove(work_clients)

        if hash_handled:
            print_time("Hash was given to a precache client")
        else:
            print_time("Precache not handled - no free workers?")


#   print_time("Work Count: %d, Hash Count: %d" % (hash_count, work_count))
# tornado.ioloop.IOLoop.instance().add_timeout(datetime.timedelta(seconds=5), push_precache)

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
            r = requests.post(rai_node_address, data=get_frontier)
            results = r.json()
            if user['work'] == WorkState.doing.value:
                # Reset work as taken too long
                work_count = work_count + 1
                #             print_time("%s : Request too long, reset" % user['account'])
                yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == user['account']).update(
                    {"work": WorkState.needs.value}).run(conn)
                if user['hash'] not in hash_to_precache:
                    hash_to_precache.append(user['hash'])
            elif results['frontier'] == user['hash']:
                up_to_date = up_to_date + 1
                if user['work'] == WorkState.needs.value:
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
                yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['account'] == user['account']).update(
                    {"work": WorkState.needs.value, "hash": results['frontier']}).run(conn)
        except Exception as e:
            print_time_debug(e)
            print_time('Checking to see if it is a case of a mistaken open block')

            # In this error, perhaps the system mistakenly added as an open block, when the node simply didn't have that block yet.
            # in that case, the next RPC will return a valid account now, and not error
            get_account = '{ "action" : "block_account", "hash" : "%s"}' % user['hash']
            r = requests.post(rai_node_address, data=get_account)
            account_data = r.json()

            if 'account' in account_data:
                print_time('Found an account for a new account, deleting last entry and setting up another for precache')

                print_time("Deleting %s" % user['id'])
                yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['id'] == user['id']).delete().run(conn)

                # Add to precache

                # Get appropriate threshold value
                # TODO what is a good threshold value for precaching?
                multiplier = 1.0
                threshold = nano.threshold_multiplier(nano.NANO_DIFFICULTY, multiplier)
                threshold_str = nano.threshold_to_str(threshold)

                yield rethinkdb.db("pow").table("hashes").insert(
                    {"account": account_data['account'], "hash": user['hash'], "work": WorkState.needs.value, "threshold": threshold_str}).run(conn)

                hash_to_precache.append(user['hash'])

            else:  # 'error' or otherwise:
                print_time('Still no valid account, deleting entry completely from DB')
                delete_error = delete_error + 1
                print_time("Deleting %s" % user['id'])
                yield rethinkdb.db("pow").table("hashes").filter(rethinkdb.row['id'] == user['id']).delete().run(conn)


    print_time("Count: {:d}, Work: {:d}, Up to date:  {:d}, Not in queue: {:d}, Not up to date: {:d}, Delete error: {:d}".format(
                count_updates, work_count, up_to_date, not_in_queue, not_up_to_data, delete_error))
    print_lists(work=1, precache=1, demand=1)
    # tornado.ioloop.IOLoop.instance().add_timeout(datetime.timedelta(seconds=60), precache_update)


@gen.coroutine
def setup_db():
    print_time("Update DB")
    conn = yield connection
    data = yield rethinkdb.db("pow").table("hashes").run(conn)
    while (yield data.fetch_next()):
        documents = yield data.next()
        if documents['work'] == WorkState.needs.value:
            if documents['hash'] not in hash_to_precache:
                hash_to_precache.append(documents['hash'])


if __name__ == "__main__":
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(int(args.internal_port))
    myIP = socket.gethostbyname(socket.gethostname())

    tornado.ioloop.IOLoop.current().run_sync(setup_db)

    print_time('*** Websocket Server Started at %s***' % myIP)
    #    main_loop.add_timeout(datetime.timedelta(seconds=10), precache_update)
    #    main_loop.add_timeout(datetime.timedelta(seconds=10), push_precache)
    pc = tornado.ioloop.PeriodicCallback(precache_update, 30000)
    pc.start()
    push = tornado.ioloop.PeriodicCallback(push_precache, 5000)
    push.start()
    # tornado.ioloop.IOLoop.instance().start()
    main_loop = tornado.ioloop.IOLoop.instance()

    try:
        main_loop.start()
    except KeyboardInterrupt:
        pass
