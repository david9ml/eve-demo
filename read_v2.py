#!/usr/bin/env python
#coding: utf8
#
# Chao Yan yanchao727@gmail.com
#
import psycopg2

from datetime import datetime
now = datetime.now().strftime('%Y_%m_%d')
fname1 = '%s_1佣金.csv'%now
fname2 = '%s_2业务拓展.csv'%now
#fname3 = '%s_3关联交易.csv'%now
####################################
#
# setting
#
####################################
port = 12345
fromdate = '2015-06-01'
todate = '2015-07-01'
eur = 0.1175
cny = 0.7942
#
####################################
rate = {
        'EUR':eur,
        'CNY':cny,
        'HKD':1,
        }

conn = psycopg2.connect(host='127.0.0.1', port=port, user='', dbname='')
cr = conn.cursor()
####################################
#
# 佣金
#
####################################
cr.execute("\
        select (select name from res_partner where id in (select partner_id from res_users where id =so.user_id)), so.name,\
        (select name from purchase_order where id in (select purchase_order_id from product_item where id in (select items_id from move_history_item_ids where move_id = sm.id))),\
        sm.date,sm.price_unit*sm.product_qty,\
        (select name from res_currency where id in (select currency_id from product_pricelist where id = so.pricelist_id)),\
        sm.location_id,sm.location_dest_id,sp.origin \
        from stock_move as sm, stock_picking as sp, sale_order as so, product_product as pp, product_template as pt \
        where sm.state='done' \
        and sm.date > %s \
        and sm.date < %s \
        and sm.picking_id=sp.id \
        and sm.product_id = pp.id \
        and pp.product_tmpl_id = pt.id \
        and pt.categ_id = 110 \
        and pt.name like %s \
        and sp.sale_id = so.id \
        and sp.company_id = 4 \
        order by sp.id \
        ",(fromdate,todate,'%佣金%'))
yongjin = []
file = open(fname1,"w")
file.write('销售员\t销售单\t采购单\t出库日期\t佣金(HKD)\n')
for user,so,po,date,price,cur,location_id,location_dest_id,origin in cr.fetchall():
    yongjin.append(origin)
    if location_dest_id == 9 and location_id != 9:
        price /= rate[cur]
    if location_dest_id != 9 and location_id == 9:
        price /= rate[cur] * (-1)
    file.write('%s\t%s\t%s\t%s\t%.2f\n'%(user.strip(), so.strip(), po.strip(), date, price))
    file.flush()
file.close()


####################################
#
# 业务拓展
#
####################################
cr.execute("\
        select sp.origin,\
        sp.amount_total,\
        (select name from res_currency where id in (select currency_id from product_pricelist where id = so.pricelist_id)),\
        (select sum(price_unit)/%s from product_item where in_currency=349 and id in (select items_id from move_history_item_ids where move_id in (select id from stock_move where picking_id = sp.id))),\
        (select sum(price_unit)/%s from product_item where in_currency=347 and id in (select items_id from move_history_item_ids where move_id in (select id from stock_move where picking_id = sp.id))),\
        (select sum(price_unit) from product_item where in_currency=353 and id in (select items_id from move_history_item_ids where move_id in (select id from stock_move where picking_id = sp.id))),\
        sp.date_done,rp.name,rp.vip_degree, rp.mobile,\
        (select name from res_partner where id in (select partner_id from res_users where id =so.user_id)),\
        (select name from res_partner where id in (select partner_id from res_users where id = rp.user_id) ),\
        so.note,\
        (select avg(out_stock_date-in_stock_date) from product_item where id in (select items_id from move_history_item_ids where move_id in (select id from stock_move where picking_id = sp.id)))  \
        from stock_picking as sp, sale_order as so, res_partner as rp, res_users as ru \
        where sp.date_done > %s \
        and sp.date_done < %s \
        and sp.state = 'done' \
        and sp.company_id=4  \
        and sp.sale_id = so.id \
        and sp.user_id = ru.id \
        and sp.partner_id = rp.id \
        order by sp.id \
        ",(eur,cny,fromdate,todate))
file = open(fname2,"w")
#file1 = open(fname3,"w")
file.write('订单号\t订单额(HKD)\t 成本(HKD)\t 毛利率(%)\t 出库日期\t客户\tvip\t客户号码\t开单员\t客户-销售员\t 订单备注\t平均库龄(天)\n')
#file1.write('订单号\t订单额(HKD)\t 成本(HKD)\t 毛利率(%)\t 出库日期\t客户\tvip\t客户号码\t开单员\t客户-销售员\t 订单备注\t平均库龄(天)\n')
for so,price,cur,cost1,cost2,cost3,date,customer, vip_degree,mobile,seller,saleperson,note,days in cr.fetchall():
    if so in yongjin:
        continue
    price /= rate[cur]
    cost = (cost1 or 0) + (cost2 or 0) + (cost3 or 0)
    if price < 0:
        cost = -cost
    '''
    if customer == '上海岑石':
        file1.write('%s\t%.2f\t%.2f\t%.2f\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' %(so,price,cost,price and (abs(price)-abs(cost))/abs(price)*100 or 0,date,customer,vip_degree, mobile,seller,saleperson,note or '',days.days))
        file1.flush()
        continue
    '''
    file.write('%s\t%.2f\t%.2f\t%.2f\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' %(so,price,cost,price and (abs(price)-abs(cost))/abs(price)*100 or 0,date,customer,vip_degree,mobile,seller,saleperson,note or '',days.days))
    file.flush()
#file1.close()

cr.execute("\
        select sp.origin,sp.amount_total,\
        (select sum(price_unit)/%s from product_item where in_currency=349 and id in (select items_id from move_history_item_ids where move_id in (select id from stock_move where picking_id = sp.id))),\
        (select sum(price_unit)/%s from product_item where in_currency=347 and id in (select items_id from move_history_item_ids where move_id in (select id from stock_move where picking_id = sp.id))),\
        (select sum(price_unit) from product_item where in_currency=353 and id in (select items_id from move_history_item_ids where move_id in (select id from stock_move where picking_id = sp.id))),\
        sp.date_done,(select name from res_partner where id = sp.partner_id),(select vip_degree from res_partner where id = sp.partner_id), (select mobile from res_partner where id = sp.partner_id),\
        (select name from res_partner where id = (select partner_id from res_users where id = (select user_id from pos_order where id=ps.id))),\
        (select name from res_partner where id in (select partner_id from res_users where id = (select user_id from res_partner where id = sp.partner_id)) ),\
        ps.note, \
        (select avg(out_stock_date-in_stock_date) from product_item where id in (select items_id from move_history_item_ids where move_id in (select id from stock_move where picking_id = sp.id))) \
        from stock_picking as sp, pos_order as ps\
        where sp.date_done > %s \
        and sp.date_done < %s \
        and sp.state = 'done' \
        and sp.company_id=4  \
        and sp.pos_id is not null \
        and sp.pos_id = ps.id \
        order by sp.id \
        ",(eur,cny,fromdate,todate))
for so,price,cost1,cost2,cost3,date,customer,vip_degree,mobile,seller,saleperson,note,days in cr.fetchall():
    cost = (cost1 or 0) + (cost2 or 0) + (cost3 or 0)
    if price < 0:
        cost = -cost
    file.write('%s\t%.2f\t%.2f\t%.2f\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n'%(so,price,cost,price and (abs(price)-abs(cost))/abs(price)*100 or 0,date,customer or '',vip_degree,mobile or '',seller or '',saleperson or '',note or '',days.days))
    file.flush()


file.close()

conn.close()



