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
import json
import time
from efashion_task import *
import traceback
#from req_postgres import output_all_products_in_stock

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
    if resource == 'stockfull':
        print("!!!!!!!!!!!!!!")
        print(type(payload))
        print(dir(payload))
        print(payload.response)
        #custom_response = ['{"_items": [{"001":"hello world"}], "_links": {"self": {"href": "stockfull", "title": "stockfull"}, "parent": {"href": "/", "title": "home"}}, "_meta": {"max_results": 25, "total": 0, "page": 1}}']
        #json_obj = json.loads(payload.response[0])
        #print json_obj["_items"]
        #json_obj["_items"] = [{"dfds":"dfds"}]
        #custom_response = [json.dumps(json_obj)]
        #payload.response = custom_response
        #payload.mimetype = 'text/json'
        #print payload.mimetype
        print("!!!!!!!!!!!!!!")

def post_post_callback(resource, request, payload):
    if resource == 'order':
        print("-------------------------debug----------------------")
        print(type(payload))
        print(payload.response)
        try:
            request_dict = request.json
            sku_str = request_dict['sku']
            qty_str = request_dict['qty']
            if sku_str.startswith('96') and 'fs' not in sku_str:
                return_value = create_order_from_efashion(pgcon,str(sku_str), int(qty_str))
            else:
                return_value = False
                print("not start with 96!")
            print(sku_str)
            print(qty_str)
            print(return_value)
        except:
            traceback.print_exc()
            return_value = False
            print(return_value)
            pass
        #print(request.args["sku"])
        print("-------------------------debug----------------------")

def read_insert(resource, items):
    if resource == 'order':
        #print(items)
        pass

app = Eve()
app.on_post_GET += post_get_callback
app.on_post_POST += post_post_callback
app.on_insert += read_insert

@app.after_request
def after_request(response):
    response.headers.add('X-Ahmed', 'Chao Yan')
    response.headers.add('X-Charlie', 'Chao Yan')
    return response

if __name__ == '__main__':
    app.run(host=host, port=port, threaded=True)
