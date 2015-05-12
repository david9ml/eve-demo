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
inc_inventory_datafile = os.path.join(rootpath, 'data', 'inventory.%s.' %today)

#logging.info('begin...')
pgcon = psycopg2.connect(database=pg['db'], user=pg['user'], host=pg['host'], port=pg['port'])
#mcon = pymongo.Connection(host = mdb['host'], port=mdb['port'])

pkey = ('_id', 'code', 'cate', 'brand', 'model', 'material', 'color', 'size', 'price', 'quatity', 'product_name', 'price_eu')

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


def full_stock_lots(pgcon, lots):
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

def compute_onsale_price(pgcon, stockdb, company, pricelist_version):
    cur = pgcon.cursor()
    col = stockdb[company]
    p_mdb = col.find()
    p_count = col.count()
    res = []

    def _create_parent_category_list(id, lst):
        if not id:
            return []
        parent = product_category_tree.get(id)
        if parent:
            lst.append(parent)
            return _create_parent_category_list(parent, lst)
        else:
            return lst

    cur.execute("select id,parent_id from product_category")
    product_categories = cur.fetchall()
    product_category_tree = dict([(item[0], item[1]) for item in product_categories if item[1]])

    while (p_count>0):
        price = False
        p_count -= 1
        p = p_mdb.next()
        p_id = p['_id']

        sql_cmd = "select id from product_pricelist_item where price_version_id = %s"%(pricelist_version,)
        cur.execute(sql_cmd)
        item_list = cur.fetchall()
        sql_cmd = "select id,item_product_id from product_ids where item_product_id = %s and items_id in (%s)"%(p_id,','.join(str(i[0]) for i in item_list))
        cur.execute(sql_cmd)
        item_id_list = cur.fetchall()

        # category
        categ_name = p['cate']
        sql_cmd = """select id from product_category where name = '%s' """%(categ_name,)
        cur.execute(sql_cmd)
        try:
            categ_id = cur.fetchall()[0][0]
            categ_ids = _create_parent_category_list(categ_id, [categ_id])
            categ_where = '(categ_id IN (' + ','.join(map(str, categ_ids)) + '))'
        except:
            categ_where = '(categ_id IS NULL)'

        # brand
        brand_name = p['brand']
        if brand_name.find("'"):
            brand_name = brand_name.replace("'","''")
        sql_cmd = """select id from product_brand where name = '%s' """%(brand_name,)
        cur.execute(sql_cmd)
        try:
            hx_product_brand_ids = cur.fetchall()[0][0]
            brand_where = 'hx_product_brand_id = %s'
            brand_args = (hx_product_brand_ids,)
        except:
            brand_where = '(hx_product_brand_id IS NULL)'
            brand_args = ()

        if item_id_list <> []:
            cur.execute(
                    'SELECT i.price_discount, I.price_surcharge '
                    'FROM product_pricelist_item AS i, '
                    'product_pricelist_version AS v, product_pricelist AS pl, product_ids as pi '
                    'WHERE (item_product_id = %s OR (item_product_id IS NULL)) '
                    'AND (' + categ_where + ' OR (categ_id IS NULL)) '
                    'AND (' + brand_where + ' OR (hx_product_brand_id IS NULL)) '
                    'AND price_version_id = %s '
                    'AND (min_quantity IS NULL OR min_quantity <= %s) '
                    'AND i.price_version_id = v.id AND v.pricelist_id = pl.id '
                    'AND i.id = pi.items_id '
                    'ORDER BY sequence',
                    (p_id,) + brand_args + (pricelist_version,p['quatity']))
        else:
            cur.execute(
                    'SELECT i.price_discount, I.price_surcharge '
                    'FROM product_pricelist_item AS i, '
                    'product_pricelist_version AS v, product_pricelist AS pl '
                    'WHERE (' + categ_where + ' OR (categ_id IS NULL)) '
                    'AND (' + brand_where + ' OR (hx_product_brand_id IS NULL)) '
                    'AND price_version_id = %s '
                    'AND (min_quantity IS NULL OR min_quantity <= %s) '
                    'AND i.price_version_id = v.id AND v.pricelist_id = pl.id '
                    'AND i.id NOT in (SELECT items_id FROM product_ids)'
                    'ORDER BY sequence',
                    brand_args + (pricelist_version, p['quatity']))
        res2 = cur.fetchall()

        for res1 in res2:
            price = p['price'] * (1.0+(float(res1[0]) or 0.0))
            price += (float(res1[1]) or 0.0)
            if price == p['price']:
                price = False
            break
        if price:
            if company == "hk":
                stockdb.hk.update({'_id': p['_id']}, {'$set': {'on_sale_price': int(price)}})
            if company == "sh":
                stockdb.sh.update({'_id': p['_id']}, {'$set': {'on_sale_price': int(price)}})
            pinfo = {'_id':p['_id'], 'code':p['code'], 'onsale_price':int(price)}
            res.append(pinfo)


    template = jinja2.Template(open(inventory_sale_template_file).read().decode('utf-8'))
    r = template.render(ps = res).encode('utf-8')
    fname = inventory_datafile+company+'.onsale'+'.xml'
    #write xml file
    fw = open(fname, 'w')
    fw.write(r)
    fw.close()


def full_stock_company(pgcon, stockdb, company):
    s = None
    price_field = None
    if company == 'hk':
        #21: lussomoda
        maxid, s = full_stock_lots(pgcon, '21')
        price_field = 'hx_price_hk'
    elif company == 'sh':
        # 12: 光复路仓库, 15: 黄金城道店, 14: 久光店, 13: 陕西南路店
        maxid, s = full_stock_lots(pgcon, '12,13,14,15')
        price_field = 'hx_price_cn'
    if not s:
        return
    col = stockdb[company]
    col_meta = stockdb.meta
    col_meta.save({'_id': company, 'maxid': maxid})
    cur = pgcon.cursor()
    res = []
    for pid, qty in s.items():
        #sql_cmd = "select product_tmpl_id,default_code,hx_product_brand_id,hx_model,hx_material,hx_color,hx_product_size,%s from product_product where id = %d" %(price_field, pid)
        sql_cmd = "select pp.product_tmpl_id,pp.default_code,pt.hx_product_brand_id,pt.hx_model,pt.hx_material,pt.hx_color,pp.hx_product_size,pt.%s from product_product as pp, product_template as pt where pp.id = %d and pp.product_tmpl_id = pt.id" %(price_field, pid)
        cur.execute(sql_cmd)
        product_tmpl_id,default_code,hx_product_brand_id,hx_model,hx_material,hx_color,hx_product_size,hx_price = cur.fetchall()[0]
        sql_cmd = "select categ_id from product_template where id = %d" %(product_tmpl_id)
        cur.execute(sql_cmd)
        categ_id = cur.fetchall()[0][0]
        pvalue = [pid, default_code, dcates[categ_id], dbrands[hx_product_brand_id], hx_model or '', hx_material or '', hx_color or '', dsizes[hx_product_size], hx_price, qty]
        pinfo = dict(zip(pkey, pvalue))
        col.insert(pinfo)
        res.append(pinfo)
    template = jinja2.Template(open(inventory_template_file).read().decode('utf-8'))
    r = template.render(ps = res).encode('utf-8')
    fname = inventory_datafile+company+'.xml'
    #write xml file
    fw = open(fname, 'w')
    fw.write(r)
    fw.close()
    #write xml.zip file
    fw = zipfile.ZipFile(fname+'.zip', 'w',  zipfile.ZIP_DEFLATED)
    fw.writestr(os.path.basename(fname), r)
    fw.close()
    #write tar.gz file
    fw = tarfile.open(fname+'.tar.gz', 'w:gz')
    fw.add(fname, arcname=os.path.basename(fname))
    fw.close()

def full_stock(pgcon, stockdb):
    full_stock_company(pgcon, stockdb, 'hk')
    full_stock_company(pgcon, stockdb, 'sh')
    compute_onsale_price(pgcon, stockdb, 'hk', 10)
    compute_onsale_price(pgcon, stockdb, 'sh', 13)


def inc_stock_lots(pgcon, lots, beg):
    cur = pgcon.cursor()
    stock = {}
    sql_cmd = "select max(id) from stock_move"
    cur.execute(sql_cmd)
    max_stock_id = cur.fetchall()[0][0]
    if max_stock_id <= beg:
        return None
    sql_cmd="select product_id,sum(product_qty) from stock_move where id > %d and id <= %d and location_id not in (%s) and location_dest_id in (%s) and state ='done' group by product_id" %(beg, max_stock_id, lots, lots)
    cur.execute(sql_cmd)
    stock = dict(cur.fetchall())
    sql_cmd="select product_id,sum(product_qty) from stock_move where id > %d and id <= %d and location_id in (%s) and location_dest_id not in (%s) and state in ('done','confirmed','waiting','assigned') group by product_id" %(beg, max_stock_id, lots, lots)
    cur.execute(sql_cmd)
    for pid, qty in cur.fetchall():
        stock.setdefault(pid, 0)
        stock[pid] -= qty
    cur.close()
    res = {}
    for k, v in stock.items():
        if v != 0:
            res[k] = int(v)
    return (max_stock_id, res)

def inc_stock_company(pgcon, stockdb, company):
    s = None
    price_field = None
    col_meta = stockdb.meta
    beg = col_meta.find_one({'_id': company})
    if not beg:
        return
    beg = beg['maxid']
    if company == 'hk':
        #21: lussomoda
        ret = inc_stock_lots(pgcon, '21', beg)
        price_field = 'hx_price_hk'
    elif company == 'sh':
        # 12: 光复路仓库, 15: 黄金城道店, 14: 久光店, 13: 陕西南路店
        ret = inc_stock_lots(pgcon, '12,13,14,15', beg)
        price_field = 'hx_price_cn'
    if not ret:
        return
    maxid, s = ret
    if not s:
        return
    col = stockdb[company + '_inc']
    col_meta.update({'_id': company}, {'$set': {'maxid': maxid}})
    cur = pgcon.cursor()
    for pid, qty in s.items():
        p_mdb = col.find_one({'_id': pid})
        if p_mdb:
            col.update({'_id':pid}, {"$inc":{'quatity':qty}});
            continue
        #sql_cmd = "select product_tmpl_id,default_code,hx_product_brand_id,hx_model,hx_material,hx_color,hx_product_size,%s from product_product where id = %d" %(price_field, pid)
        sql_cmd = "select pp.product_tmpl_id,pp.default_code,pt.hx_product_brand_id,pt.hx_model,pt.hx_material,pt.hx_color,pp.hx_product_size,pt.%s from product_product as pp, product_template as pt where pp.id = %d and pp.product_tmpl_id = pt.id" %(price_field, pid)
        cur.execute(sql_cmd)
        product_tmpl_id,default_code,hx_product_brand_id,hx_model,hx_material,hx_color,hx_product_size,hx_price = cur.fetchall()[0]
        sql_cmd = "select categ_id from product_template where id = %d" %(product_tmpl_id)
        cur.execute(sql_cmd)
        categ_id = cur.fetchall()[0][0]
        pvalue = [pid, default_code, dcates[categ_id], dbrands[hx_product_brand_id], hx_model, hx_material, hx_color, dsizes[hx_product_size], hx_price, qty]
        pinfo = dict(zip(pkey, pvalue))
        col.insert(pinfo)

def inc_stock_company_once(pgcon, stockdb, company):
    try:
        beg_maxid = stockdb.meta.find_one({'_id':company})['maxid']
        inc_stock_company(pgcon, stockdb, company)
        end_maxid = stockdb.meta.find_one({'_id':company})['maxid']
        logging.info('inc_stock_%s: beg_maxid=%d, end_maxid=%d' %(company, beg_maxid, end_maxid))
    except:
        try:
            time.sleep(300)
            pgcon = psycopg2.connect(database=pg['db'], user=pg['user'], host=pg['host'], port=pg['port'])
            beg_maxid = stockdb.meta.find_one({'_id':company})['maxid']
            inc_stock_company(pgcon, stockdb, company)
            end_maxid = stockdb.meta.find_one({'_id':company})['maxid']
            logging.info('inc_stock_%s: beg_maxid=%d, end_maxid=%d' %(company, beg_maxid, end_maxid))
        except:
            e = traceback.format_exc()
            logging.warning('inc_stock_company_once exception: \n %s' %(e))
            os.system('echo -e "%s" | mail -s "[WARNING][STOCKAPI]error message - %s" "wangwenguang@hxpop.com"' %(e, today))

def inc_stock(pgcon, stockdb):
    while True:
        if datetime.now().hour  == 23:
            logging.info('timeout, exit.')
            break
        inc_stock_company_once(pgcon, stockdb, 'hk')
        inc_stock_company_once(pgcon, stockdb, 'sh')
        time.sleep(interval)

def stock(pgcon, stockdb):
    full_stock(pgcon, stockdb)
    inc_stock(pgcon, stockdb)

dbrands, dsizes, dcates = misc_info(pgcon)

def _inc_stock_lots(pgcon, lots, beg):
    cur = pgcon.cursor()
    stock = {}
    sql_cmd = "select max(id) from stock_move"
    cur.execute(sql_cmd)
    max_stock_id = cur.fetchall()[0][0]
    if max_stock_id <= beg:
        return None
    sql_cmd="select product_id,sum(product_qty) from stock_move where id > %d and id <= %d and location_id not in (%s) and location_dest_id in (%s) and state ='done' group by product_id" %(beg, max_stock_id, lots, lots)
    cur.execute(sql_cmd)
    stock = dict(cur.fetchall())
    sql_cmd="select product_id,sum(product_qty) from stock_move where id > %d and id <= %d and location_id in (%s) and location_dest_id not in (%s) and state in ('done','confirmed','waiting','assigned') group by product_id" %(beg, max_stock_id, lots, lots)
    cur.execute(sql_cmd)
    for pid, qty in cur.fetchall():
        stock.setdefault(pid, 0)
        stock[pid] -= qty
    cur.close()
    res = {}
    for k, v in stock.items():
        if v != 0:
            res[k] = int(v)
    return (max_stock_id, res)

def _out_inc_products_in_stock(pgcon, stockdb):
    s = None
    price_field = None
    col_meta = stockdb.meta
    beg = col_meta.find_one({'_id': '21'})
    if not beg:
        return
    beg = beg['maxid']
    #21: lussomoda
    ret = _inc_stock_lots(pgcon, '21', beg)
    price_field = 'hx_price_hk'
    if not ret:
        return
    maxid, s = ret
    if not s:
        return
    #col = stockdb[company + '_inc']
    #col_meta.update({'_id': company}, {'$set': {'maxid': maxid}})
    cur = pgcon.cursor()
    res = []
    for pid, qty in s.items():
        '''
        p_mdb = col.find_one({'_id': pid})
        if p_mdb:
            col.update({'_id':pid}, {"$inc":{'quatity':qty}});
            continue
        '''
        sql_cmd = "select pp.product_tmpl_id,pp.default_code,pt.hx_product_brand_id,pt.hx_model,pt.hx_material,pt.hx_color,pp.hx_product_size,pt.%s, pp.name from product_product as pp, product_template as pt where pp.id = %d and pp.product_tmpl_id = pt.id" %(price_field, pid)
        cur.execute(sql_cmd)
        product_tmpl_id,default_code,hx_product_brand_id,hx_model,hx_material,hx_color,hx_product_size,hx_price, product_name = cur.fetchall()[0]
        sql_cmd = "select categ_id from product_template where id = %d" %(product_tmpl_id)
        cur.execute(sql_cmd)
        categ_id = cur.fetchall()[0][0]
        pvalue = [pid, default_code, dcates[categ_id], dbrands[hx_product_brand_id], hx_model, hx_material, hx_color, dsizes[hx_product_size], hx_price, qty, product_name]
        pinfo = dict(zip(pkey, pvalue))
        res.append(pinfo)

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
    '''
    col = stockdb[company]
    col_meta = stockdb.meta
    col_meta.save({'_id': company, 'maxid': maxid})
    '''
    cur = pgcon.cursor()
    res = []
    for pid, qty in s.items():
        sql_cmd = "select pp.product_tmpl_id,pp.default_code,pt.hx_product_brand_id,pt.hx_model,pt.hx_material,pt.hx_color,pp.hx_product_size,pt.%s,pp.name from product_product as pp, product_template as pt where pp.id = %d and pp.product_tmpl_id = pt.id" %(price_field, pid)
        cur.execute(sql_cmd)
        product_tmpl_id,default_code,hx_product_brand_id,hx_model,hx_material,hx_color,hx_product_size,hx_price,product_name = cur.fetchall()[0]
        sql_cmd = "select categ_id from product_template where id = %d" %(product_tmpl_id)
        cur.execute(sql_cmd)
        categ_id = cur.fetchall()[0][0]
        pvalue = [pid, default_code, dcates[categ_id], dbrands[hx_product_brand_id], hx_model or '', hx_material or '', hx_color or '', dsizes[hx_product_size], hx_price, qty, product_name]
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

def _out_inc_products_in_stock_v2(pgcon):
    today = time.strftime('%Y%m%d')
    mdb = pymongo.Connection(port=18811)
    col = mdb[today]['hk'+'_inc']
    try:
        cur = col.find()
        cur.batch_size(1000)
        r = list(cur)
    except pymongo.errors.AutoReconnect:
        cur = col.find()
        cur.batch_size(1000)
        r = list(cur)
    except:
        print "File don't exist! Please contact to yanchao727@gmail.com"
        return
    r_inc_ids = [i['_id'] for i in r]
    col = mdb[today]['hk']
    try:
        cur = col.find({'_id':{'$in':r_inc_ids}}, fields=['quatity'])
        cur.batch_size(1000)
        r_full = list(cur)
    except pymongo.errors.AutoReconnect:
        cur = col.find({'_id':{'$in':r_inc_ids}}, fields=['quatity'])
        cur.batch_size(1000)
        r_full = list(cur)
    except:
        print "File don't exist! Please contact to yanchao727@gmail.com"
        return
    r_full = dict([(i['_id'], i['quatity']) for i in r_full])
    pgcon_cur = pgcon.cursor()
    for p in r:
        sql_cmd = "select pp.name, pt.hx_price_eu from product_product as pp, product_template as pt where pp.product_tmpl_id = pt.id and pp.id=%s" % p['_id']
        pgcon_cur.execute(sql_cmd)
        product_name, hx_price_eu = pgcon_cur.fetchall()[0]
        x = p['quatity'] + r_full.get(p['_id'], 0)
        p['quatity'] =  x >= 0 and x or 0
        p['product_name']=product_name.replace('&', ' ')
        p['price_eu']=hx_price_eu
    template = jinja2.Template(open(inventory_template_file).read().decode('utf-8'))
    render_obj = template.render(ps = r).encode('utf-8')
    fname = inventory_datafile+'hk.inc.latest'+'.xml'
    #write xml file
    fw = open(fname, 'w+')
    fw.write(render_obj)
    fw.close()

_out_inc_products_in_stock_v2(pgcon)
#return_value = _output_products_in_stock(pgcon)

