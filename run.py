# -*- coding: utf-8 -*-

"""
    Eve Demo
    ~~~~~~~~

    A demostration of a simple API powered by Eve REST API.

    The live demo is available at eve-demo.herokuapp.com. Please keep in mind
    that the it is running on Heroku's free tier using a free MongoHQ
    sandbox, which means that the first request to the service will probably
    be slow. The database gets a reset every now and then.

    :copyright: (c) 2015 by Nicola Iarocci.
    :license: BSD, see LICENSE for more details.
"""

import os
from eve import Eve

# Heroku support: bind to PORT if defined, otherwise default to 5000.
if 'PORT' in os.environ:
    port = int(os.environ.get('PORT'))
    # use '0.0.0.0' to ensure your REST API is reachable from all your
    # network (and not only your computer).
    host = '0.0.0.0'
else:
    port = 65000
    host = '0.0.0.0'

def manipulate_inbound_documents(resource, docs):
    if resource == 'stockfull':
        resource['_items'] = [{'a':'hello'}]
        docs['id_field'] = '0001'
        for doc in docs:
            doc['id_field'] = '001'
            doc['qr'] = 'mycqcode'

def update_stockfull(request, payload):
    accounts = app.data.driver.db['bcd_collection']
    account = accounts.insert(docs)

def post_get_callback(resource, request, payload):
    #payload["_items"] = [{'a':'b'}]
    print 'A GET on the "%s" endpoint was just performed!' % resource
    print "!!!!!!!!!!!!!!"
    print type(payload)
    print dir(payload)
    print payload.response
    print type(payload.response)
    payload.response=['{"_items": ["001":"hello world"], "_links": {"self": {"href": "stockfull", "title": "stockfull"}, "parent": {"href": "/", "title": "home"}}, "_meta": {"max_results": 25, "total": 0, "page": 1}}']
    #payload.response = [{'a': 'b'}]
    print "!!!!!!!!!!!!!!"
    #print response
app = Eve()
app.on_post_GET += post_get_callback

@app.after_request
def after_request(response):
    response.headers.add('X-Ahmed', 'Chao Yan')
    response.headers.add('X-Charlie', 'Chao Yan')
    return response

if __name__ == '__main__':
    app.run(host=host, port=port)
