import rethinkdb as r

clients = [
    # username,             public,     display name,           website (without https)
    ('username1',           True,       'User 1',               'www.User1Website'),
]

conn = r.connect("localhost", 28015)

for username, public, display, website in clients:
    result = r.db("pow").table("api_keys").filter(r.row['username']==username).update({
        'public': public,
        'display_name': display,
        'website': website
    }).run(conn)

    print("For username {}, got {}".format(
        username,
        [k for k,v in result.items() if v!=0]
    ))
