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
import psycopg2
import time
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

from settings import pg
pkey = ('_id', 'code', 'cate', 'brand', 'model', 'material', 'color', 'size', 'price', 'quatity')
pgcon = psycopg2.connect(database=pg['db'], user=pg['user'], host=pg['host'], port=pg['port'])

def misc_info(pgcon):
    cur = pgcon.cursor()
    #for brand
    sql_cmd="select id,name from product_brand;"
    cur.execute(sql_cmd)
    dbrands = dict(cur.fetchall())
    dbrands[None] = ''
    #for size
    sql_cmd="select id,name from product_size;"
    cur.execute(sql_cmd)
    dsizes = dict(cur.fetchall())
    dsizes[None] = ''
    #for category
    sql_cmd = "select id,name from product_category"
    cur.execute(sql_cmd)
    dcates = dict(cur.fetchall())
    dcates[None] = ''
    cur.close()
    return (dbrands, dsizes, dcates)

dbrands, dsizes, dcates = misc_info(pgcon)

def _full_stock_lots(pgcon, lots):
    cur = pgcon.cursor()
    stock = {}
    sql_cmd = "select max(id) from stock_move"
    cur.execute(sql_cmd)
    max_stock_id = cur.fetchall()[0][0]
    sql_cmd="select product_id,sum(product_qty) from stock_move where id <= %d and location_id not in (%s) and location_dest_id in (%s) and state ='done' group by product_id" %(max_stock_id, lots, lots)
    cur.execute(sql_cmd)
    stock = dict(cur.fetchall())
    sql_cmd="select product_id,sum(product_qty) from stock_move where id <= %d and location_id in (%s) and location_dest_id not in (%s) and state in ('done','confirmed','waiting','assigned') group by product_id" %(max_stock_id, lots, lots)
    cur.execute(sql_cmd)
    for pid, qty in cur.fetchall():
        stock[pid] -= qty
    cur.close()
    res = {}
    for k, v in stock.items():
        if v > 0:
            res[k] = int(v)
    return (max_stock_id, res)

def _output_products_in_stock(pgcon):
    s = None
    price_field = None
    maxid, s = _full_stock_lots(pgcon, '21')
    price_field = 'hx_price_hk'
    if not s:
        return False
    cur = pgcon.cursor()
    res = []
    for pid, qty in s.items():
        sql_cmd = "select pp.product_tmpl_id,pp.default_code,pt.hx_product_brand_id,pt.hx_model,pt.hx_material,pt.hx_color,pp.hx_product_size,pt.%s from product_product as pp, product_template as pt where pp.id = %d and pp.product_tmpl_id = pt.id" %(price_field, pid)
        cur.execute(sql_cmd)
        product_tmpl_id,default_code,hx_product_brand_id,hx_model,hx_material,hx_color,hx_product_size,hx_price = cur.fetchall()[0]
        sql_cmd = "select categ_id from product_template where id = %d" %(product_tmpl_id)
        cur.execute(sql_cmd)
        categ_id = cur.fetchall()[0][0]
        pvalue = [pid, default_code, dcates[categ_id], dbrands[hx_product_brand_id], hx_model or '', hx_material or '', hx_color or '', dsizes[hx_product_size], hx_price, qty]
        pinfo = dict(zip(pkey, pvalue))
        #col.insert(pinfo)
        res.append(pinfo)
    return res


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
        print "!!!!!!!!!!!!!!"
        print type(payload)
        print dir(payload)
        print payload.response
        print type(payload.response)
        #custom_response = ['{"_items": [{"001":"hello world"}], "_links": {"self": {"href": "stockfull", "title": "stockfull"}, "parent": {"href": "/", "title": "home"}}, "_meta": {"max_results": 25, "total": 0, "page": 1}}']
        json_obj = json.loads(payload.response[0])
        json_obj["_items"] = _output_products_in_stock(pgcon)
        #print json_obj["_items"]
        #json_obj["_items"] = [{"dfds":"dfds"}]
        custom_response = [json.dumps(json_obj)]
        payload.response = custom_response
        #payload.mimetype = 'text/json'
        #print payload.mimetype
        print "!!!!!!!!!!!!!!"
app = Eve()
app.on_post_GET += post_get_callback

@app.after_request
def after_request(response):
    response.headers.add('X-Ahmed', 'Chao Yan')
    response.headers.add('X-Charlie', 'Chao Yan')
    return response

if __name__ == '__main__':
    app.run(host=host, port=port, threaded=True)
