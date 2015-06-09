#!/usr/bin/env python
#coding: utf8
# by wgwang svd.wang@gmail.com
#

import os
import sys
import psycopg2
import pymongo
import jinja2
from datetime import datetime
from setting import pg, mdb
try:
    from setting import interval
except:
    interval = 600
import traceback
import logging
import time
import zipfile
import tarfile

#daemon processing
pid = os.fork()
if pid != 0:
    sys.exit()
pid = os.fork()
if pid != 0:
    sys.exit()

reload(sys)
sys.setdefaultencoding('utf-8')
today = datetime.today().strftime('%Y%m%d')
rootpath = os.path.abspath(os.path.join(os.getcwd(), os.path.dirname(sys.argv[0]), '..'))
'''
logfile = os.path.join(rootpath, 'logs', 'sm.log.'+today)
logformat = '%(asctime)s -- %(message)s'
logging.basicConfig(filename=logfile, level=logging.DEBUG, format=logformat)
'''
inventory_template_file = os.path.join(rootpath, 'req_postgres/template', 'inventory.xml.template')
inventory_sale_template_file = os.path.join(rootpath, 'template', 'inventory.sale.xml.template')
inventory_datafile = os.path.join(rootpath, 'data', 'inventory.%s.' %today)

pgcon = psycopg2.connect(database=pg['db'], user=pg['user'], host=pg['host'], port=pg['port'])

pkey = ('_id', 'code', 'cate', 'brand', 'model', 'material', 'color', 'size', 'price', 'quatity', 'product_name', 'price_eu', 'pt_name', 'pt_sku')

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
    sql_cmd="select product_id,sum(product_qty) from stock_move where id <= %d and location_id in (%s) and location_dest_id not in (%s) and state in ('done','confirmed','draft','assigned') group by product_id" %(max_stock_id, lots, lots)
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
    '''
    col = stockdb[company]
    col_meta = stockdb.meta
    col_meta.save({'_id': company, 'maxid': maxid})
    '''
    cur = pgcon.cursor()
    res = []
    for pid, qty in s.items():
        sql_cmd = "select pp.product_tmpl_id,pp.default_code,pt.hx_product_brand_id,pt.hx_model,pt.hx_material,pt.hx_color,pp.hx_product_size,pt.%s,pp.name, pt.hx_price_eu, pt.name, pt.default_code from product_product as pp, product_template as pt where pp.id = %d and pp.product_tmpl_id = pt.id" %(price_field, pid)
        cur.execute(sql_cmd)
        product_tmpl_id,default_code,hx_product_brand_id,hx_model,hx_material,hx_color,hx_product_size,hx_price,product_name, hx_price_eu, pt_name, pt_sku = cur.fetchall()[0]
        product_name = product_name.replace('&','_')
        pt_name = pt_name.replace('&','_')
        sql_cmd = "select categ_id from product_template where id = %d" %(product_tmpl_id)
        cur.execute(sql_cmd)
        categ_id = cur.fetchall()[0][0]
        pvalue = [pid, default_code, dcates[categ_id], dbrands[hx_product_brand_id], hx_model or '', hx_material or '', hx_color or '', dsizes[hx_product_size], hx_price, qty, product_name, hx_price_eu, pt_name, pt_sku]
        pinfo = dict(zip(pkey, pvalue))
        #col.insert(pinfo)
        res.append(pinfo)
    template = jinja2.Template(open(inventory_template_file).read().decode('utf-8'))
    r = template.render(ps = res).encode('utf-8')
    fname = inventory_datafile+'hk'+'.xml'
    #write xml file
    fw = open(fname, 'w+')
    fw.write(r)
    fw.close()
    '''
    #write xml.zip file
    fw = zipfile.ZipFile(fname+'.zip', 'w',  zipfile.ZIP_DEFLATED)
    fw.writestr(os.path.basename(fname), r)
    fw.close()
    #write tar.gz file
    fw = tarfile.open(fname+'.tar.gz', 'w:gz')
    fw.add(fname, arcname=os.path.basename(fname))
    fw.close()
    '''
    return res

def get_pt_id_from_psku(pgcon, psku_str):
    cur = pgcon.cursor()
    sql_cmd = "select id from product_template where default_code='%s';" %psku_str
    cur.execute(sql_cmd)
    pt_id = cur.fetchall()[0][0]
    #return an int value
    return pt_id

def get_children_skulist_from_parent_product(pgcon, parent_id):
    cur = pgcon.cursor()
    sql_cmd = "select default_code from product_product where product_tmpl_id=%s;" %parent_id
    cur.execute(sql_cmd)
    cur_all = cur.fetchall()
    return cur_all

return_value = get_pt_id_from_psku(pgcon, "9200000207404")
print return_value
cur_all = get_children_skulist_from_parent_product(pgcon, return_value)
print cur_all
