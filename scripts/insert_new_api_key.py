import rethinkdb
import random
import time

conn = rethinkdb.connect( "localhost", 28015).repl()

username = input("username: ")
print("Generate new key")
full_wallet_seed = hex(random.SystemRandom().getrandbits(128))
api_key = full_wallet_seed[2:].upper()

print("%s %s" % (username, api_key))

print("Insert into DB")
data = rethinkdb.db("pow").table("api_keys").insert({"username":username, "api_key":api_key, "count": 0,  "time":time.time()}).run()
print(data)
print("complete")
