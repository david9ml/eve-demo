#!/usr/bin/env python
#coding: utf-8
# by yanchao  email:yanchao727@gmail.com
#
import random
import psycopg2
from datetime import datetime
import sys
import os
sys.path.append('/home/aphrodite/aphrodite')
sys.path.append('/home/aphrodite')
os.environ['DJANGO_SETTINGS_MODULE'] = 'aphrodite.settings'
from django.conf import settings
#from aphrodite.aphrodite.product.models import *
from model_efashion_used import *

import logging
logger = logging.getLogger('django')

import pytz
tz = pytz.timezone('Asia/Shanghai')
from celery.task import task
from celery import Celery
import traceback

pgcon = psycopg2.connect(database='hxdb', user='chao', host='127.0.0.1', port='18068')

'''
order = Sale_Order(partner_id=79,
        pricelist_id=28,
        location_id = 21,
        company_id = 4,
)
order.save()
new_line = order.order_lines.create(
            order = order,
            product_id = 79,
            qty = 1,
            price_unit = 3912.0,
            subtotal = 3912.0,
)

celery = Celery()
celery.config_from_object('settings')
default_order = {
                'partner_name' : 'abc',
                'partner_id' : 16639,
                'pricelist_id' : 28,
                'location_id' : 21,
                'company_id' : 4,
                }
new_lines = []
#new_lines.append({'lu_line_id':new_line.id, 'product_id':line.get('product_id'), 'price_unit':line.get('price_unit'), 'qty':line.get('qty')})
new_lines.append({'lu_line_id':new_line.id,'product_id':71086, 'price_unit':3912.0, 'qty':1})
default_order.update({'lu_order_id':order.id,'lines':new_lines})
#print(default_order)
'''

def get_product_info_by_sku(pgcon, sku_str):
    cur = pgcon.cursor()
    try:
        sql_cmd = "select pp.id, pp.name from product_product as pp where pp.default_code='%s'" %sku_str
        cur.execute(sql_cmd)
        product_id, product_name = cur.fetchall()[0]
        sql_cmd_2 = "select pt.hx_price_hk from product_product as pp, product_template as pt where pp.id = %d and pp.product_tmpl_id = pt.id" %product_id
        cur.execute(sql_cmd_2)
        product_price_hk  = cur.fetchall()[0][0]
    except:
        product_id = None
        product_name = None
        product_price_hk = None
        traceback.print_exc()
    return product_id, product_name, product_price_hk


def create_order_from_efashion(pgcon, sku_str, qty_num, price_unit=3912.0):
    try:
        product_id, product_name, price_unit = get_product_info_by_sku(pgcon, sku_str)
        print(product_id)
        print(product_name)
        print(price_unit)
    except:
        traceback.print_exc()
        return False
    try:
        default_order2 = {'lines': [{'qty': qty_num, 'price_unit': price_unit, 'product_id': product_id, 'lu_line_id': 902}], 'pricelist_id': 28, 'location_id': 21, 'lu_order_id': 507, 'company_id': 4, 'partner_id': 29777, 'partner_name': '易时尚'}
        #default_order2 = {'lines': [{'qty': qty_num, 'product_id': 71086, 'lu_line_id': 902}], 'pricelist_id': 28, 'location_id': 21, 'lu_order_id': 507, 'company_id': 4, 'partner_id': 28273, 'partner_name': '易时尚'}
        order_task = celery.send_task("erp_tasks.create_from_lussomoda",args=[default_order2],routing_key = 'erp_tasks', queue='erp_tasks',expires=14)
    except:
        traceback.print_exc()
        return False
    return True
'''
a, b, c = get_product_info_by_sku(pgcon, '9600000703096')
print(a)
print(b)
print(c)
'''
#create_order_from_efashion(pgcon,'9600000703096', 1)
