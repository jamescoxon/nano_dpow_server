import requests
import json
import configparser
from threading import Thread

def formatted_time(t):
    return t.strftime("%Y-%m-%d %H:%M:%S.{:d}".format(int(t.microsecond/1e4)))


def post_js(js, uri):
    try:
        res = requests.post(uri, headers={"Content-Type": "application/json"}, data=js)
        if res.status_code != 200:
            print('Failed to send data to interface URI {}:\n{}'.format(uri, js))
            print('{}\n{}'.format(res.status_code, res.text))
    except Exception as e:
        print('Failed to send data to interface URI {}:\n{}'.format(uri, js))
        print('{}: {}'.format(type(e).__name__, e))


class InterfaceClient(object):
    def __init__(self):
        self.ready = False


    def read_config(self, config_path):
        try:
            config = configparser.ConfigParser()
            config.read(config_path)
            server = config.get('DEFAULT', 'server')
            key = config.get('DEFAULT', 'key')
        except Exception as e:
            print('Error reading interface config file: {}'.format(e))
            self.ready = False
            return False

        self.ready = True
        self.server = server
        self.key = key
        return True

    def pow_update(self, request_id, service_id, client_id, pow_type, time_req, time_res):
        # TODO finish later, including difficulty
        if not self.ready:
            return False

        pow_js = json.dumps({
            'action': 'pow_request',
            'api_key': self.key,
            'request_id': request_id,
            'service_id': service_id,
            'client_id': client_id,
            'pow_type': pow_type,
            'time_requested': formatted_time(time_req),
            'time_responded': formatted_time(time_res)
        })

        t = Thread(target=post_js, args=[pow_js, self.server+'pow_update'])
        t.daemon = True
        t.start()
        return True  # handled, not necessarily ok

    def clients_update(self, client_list):
        if not self.ready:
            return False

        pow_js = json.dumps({
            'api_key': self.key,
            'clients': client_list
        })

        t = Thread(target=post_js, args=[pow_js, self.server+'client_update'])
        t.daemon = True
        t.start()
        return True  # handled, not necessarily ok

    def services_update(self, service_list):
        if not self.ready:
            return False

        pow_js = json.dumps({
            'api_key': self.key,
            'services': service_list
        })

        t = Thread(target=post_js, args=[pow_js, self.server+'service_update'])
        t.daemon = True
        t.start()
        return True  # handled, not necessarily ok
