import rethinkdb
import random
import time
import hashlib

conn = rethinkdb.connect( "localhost", 28015).repl()

username = input("username: ")
print("Generate new key")
full_wallet_seed = hex(random.SystemRandom().getrandbits(128))
api_key = full_wallet_seed[2:].upper()
api_key_hashed = hashlib.sha512(api_key.encode('utf-8')).hexdigest()

print("%s %s %s" % (username, api_key, api_key_hashed))

print("Insert into DB")
data = rethinkdb.db("pow").table("api_keys").insert({"username":username, "api_key":api_key_hashed, "count": 0,  "time":time.time()}).run()
print(data)
print("complete")
