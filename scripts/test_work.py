import requests
import time
from sys import argv

hash = str(argv[1])
key= str(argv[2])

start_time = time.time()
json_request = '{"hash" : "%s", "key" : "%s" }' % (hash, key)
print(json_request)
r = requests.post('http://127.0.0.1:5000/work', data = json_request)
complete_time = time.time()
print(r.text + " took: " + str(complete_time - start_time))
