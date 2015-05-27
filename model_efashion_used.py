#!/usr/bin/env python
#coding: utf-8
# by zhangxue  email:zhangxue@hxpop.com
#
from django.db import models,transaction,utils
from django.db.models import Sum,Count,Q,Min,Max
from django.contrib.auth.models import User
from django.utils.timezone import now
try:
    from product.settings import COMPANY_ID,LOCATION_ID,PRICELIST_ID
except:
    COMPANY_ID = 4
    LOCATION_ID = 21
    PRICELIST_ID = 28
    pass
from django.core.urlresolvers import reverse

import random
from datetime import datetime

import logging
logger = logging.getLogger('django')

import pytz
tz = pytz.timezone('Asia/Shanghai')
from celery.task import task
from celery import Celery
celery = Celery()
celery.config_from_object('aphrodite.settings')

class Product_Brand(models.Model):
    class Meta:
        app_label = 'product'
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=64)

    # xiongxiong
    def get_absolute_url_of_categ(self, category):
        ''' /catgory/<self.id>-0-0-0-0-0-0'''
        if category.parent.name == '产品':
            return reverse('category', kwargs={'category': category.name,
                'sub_categ':0, 'price':0, 'size':0, 'material':0, 'order':0,
                'brand': self.id})
        if category.parent.parent.name == '产品':
            return reverse('category', kwargs={'category': category.parent.name,
                'sub_categ':category.id, 'price':0, 'size':0, 'material':0, 'order':0,
                'brand': self.id})

    def get_absolute_url(self):
        ''' In xianhuo page '''
        return reverse('xianhuo', kwargs={'sub_categ':0,
            'price':0, 'size':0, 'material':0, 'order':0, 'brand': self.id})

    def img(self):
        return '{}_{}.jpg'.format(self.id, self.name.replace(' ', '-').upper())

    def __str__(self):
        return self.name
    # xiongxiong

    ###############################################
    # zhangxue
    ###############################################
    @classmethod
    @task
    def create_brand(cls,vals):
        """ create brand from erp """
        try:
            if isinstance(cls,str):
                cls = eval(cls)
            res = cls(**vals)
            res.save()
            return res.id
        except Exception as e:
            logger.warn('create brand[[%r]] fail:%r', vals, e)
            return 'Error: Creat brand[[%r]] fail: %r' %(vals, e)

    @classmethod
    @task
    def write_brand(cls,id,vals):
        try:
            if isinstance(cls,str):
                cls = eval(cls)
            p = cls.objects.filter(id=id)
            if not p:
                vals.update({'id':id})
                return Product_Brand.create_brand(cls, vals)
            for key,value in vals.items():
                if value == False:
                    exec("p.update(%s=None)"%key)
                else:
                    exec("p.update(%s='''%s''')"%(key,value))
            return id
        except Exception as e:
            logger.warn('write brand[[%r]] fail:%r', vals, e)
            return 'Error: Write brand[[%r]] fail: %r' %(vals, e)

class Product_Category(models.Model):
    class Meta:
        app_label = 'product'
    id = models.IntegerField(primary_key=True)
    parent = models.ForeignKey('self', db_column='parent_id', null=True, related_name='children')
    big_parent = models.ForeignKey('self', db_column='big_parent_id', null=True, related_name='manychildren')
    name = models.CharField(max_length=64)

    # xiongxiong #
    @classmethod
    def big_categ(cls, name):
        """ 获取顶级品类 """
        all_products = cls.objects.get(name='产品')
        return cls.objects.filter(name=name, parent=all_products).first()

    @classmethod
    def sub_categ(cls, name):
        """ 获取大品类下的所有子品类 """
        categ = cls.big_categ(name)
        if not categ:
            return None
        return cls.objects.filter(parent=categ)
    def get_absolute_url(self):
        ''' /catgory/<self.id>-0-0-0-0-0-0'''
        if self.parent.name == '产品':
            return reverse('category', kwargs={'category': self.name,
                'sub_categ':0, 'price':0, 'size':0, 'material':0, 'order':0, 'brand':0})
        if self.parent.parent.name == '产品':
            return reverse('category', kwargs={'category': self.parent.name,
                'sub_categ':self.id, 'price':0, 'size':0, 'material':0, 'order':0, 'brand':0})

    ##############

    ###############################################
    # zhangxue
    ###############################################
    @classmethod
    @task
    def create_category(cls,vals):
        """ create category from erp """
        try:
            if isinstance(cls,str):
                cls = eval(cls)
            res = cls(**vals)
            res.save()
            return res.id
        except Exception as e:
            logger.warn('create category[[%r]] fail:%r', vals, e)
            return 'Error: Creat category[[%r]] fail: %r'%(vals, e)

    @classmethod
    @task
    def write_category(cls,id,vals):
        """ modify """
        try:
            if isinstance(cls,str):
                cls = eval(cls)
            p = cls.objects.filter(id=id)
            if not p:
                vals.update({'id':id})
                return Product_Category.create_category(cls, vals)
            for key,value in vals.items():
                if key in ('parent_id','big_parent_id'):
                    key = key.split('_id')[0]
                if value == False:
                    exec("p.update(%s=None)"%key)
                else:
                    exec("p.update(%s='''%s''')"%(key,value))
            return id
        except Exception as e:
            logger.warn('write category[[%r]] to db fail:%r', vals, e)
            return 'Error: Write category[[%r]] fail: %r' %(vals, e)

class Product_Template(models.Model):
    class Meta:
        app_label = 'product'
    id = models.IntegerField(primary_key=True)
    iid = models.CharField(max_length=16,default='0000000000000')
    name = models.CharField(max_length=128)
    categ =  models.ForeignKey('Product_Category')
    brand =  models.ForeignKey('Product_Brand', null=True)
    model = models.CharField(max_length=32, null=True)
    material = models.CharField(max_length=32, null=True)
    color = models.CharField(max_length=32, null=True, default='NULL')
    hx_price_cn = models.FloatField(null=True)
    hx_price_hk = models.FloatField(null=True)
    hx_price_eu = models.FloatField(null=True)

    ##########################################
    # xiongxiong
    def get_absolute_url(self):
        return reverse('product.views.read_inventory', args=[str(self.id)])

    def price_special(self):
        return self.inventorys.filter(qty_canbesold__gt=0).aggregate(
                Max('price_special'))['price_special__max']

    @classmethod
    def filter_price_special(cls, authenticated=False):
        ''' 获取所有特惠产品 '''
        # price_special > 0表示正在做特卖
        templates = cls.objects.filter(inventorys__qty_canbesold__gt=0, inventorys__price_special__gt=0).annotate(
                date=Max('inventorys__date'),          # for date order
                price_special=Max('inventorys__price_special'),
                qty=Sum('inventorys__qty_canbesold'))

        return templates

    @classmethod
    def filter_xianhuo(cls, authenticated=False):
        ''' 现货产品 '''
        templates = cls.objects.annotate(
                date=Max('inventorys__date'),          # for date order
                qty=Sum('inventorys__qty_canbesold'))

        templates = templates.exclude(qty=0)
        return templates

    @classmethod
    def filter_price_consignment(cls, authenticated=False):
        ''' 代销产品 '''
        templates = cls.objects.annotate(
                date=Max('inventorys__date'),          # for date order
                price_consignment=Max('inventorys__price_consignment'),
                qty=Sum('inventorys__qty_canbesold'))

        templates = templates.exclude(qty=0)
        templates = templates.filter(qty__gt=0, price_consignment__gt=0)
        return templates

    @classmethod
    def categ_filter(cls, category):
        return {'categ': category}
    @classmethod
    def brand_filter(cls, brand):
        return {'brand': brand}
    ##########################################

    ###############################################
    # zhangxue
    ###############################################
    @classmethod
    @task
    def create_template(cls,vals):
        try:
            if isinstance(cls,str):
                cls = eval(cls)
            res = cls(**vals)
            res.save()
            return res.id
        except Exception as e:
            logger.warn('create product template[[%r]] fail:%r', vals, e)
            return 'Error: Create product template[[%r]] fail: %r' %(vals, e)

    @classmethod
    @task
    def write_template(cls,id,vals):
        try:
            if isinstance(cls,str):
                cls = eval(cls)
            p = cls.objects.filter(id=id)
            if not p:
                vals.update({'id':id})
                return Product_Template.create_template(cls, vals)
            for key,value in vals.items():
                if key in ('categ_id', 'brand_id'):
                    key = key.split('_id')[0]
                if value == False:
                    exec("p.update(%s=None)"%key)
                else:
                    exec("p.update(%s='''%s''')"%(key,value))
            return id
        except Exception as e:
            logger.warn('write product template[[%r]] fail:%r', vals, e)
            return 'Error: Write product template[[%r]] fail: %r' %(vals, e)

    def small_image(self):
        return self.image_url_from_attr(isize='s')

    def image_url_from_attr(self, isize='m'):
        default_url = 'http://img.yvogue.hk/media/default.jpg'
        pbid =  self.brand_id
        pmodel = self.model
        pmaterial = self.material
        pcolor = self.color
        if not pbid:
            return default_url

        pmodel = pmodel and pmodel.lower().strip().lstrip('$') or ''
        if pmodel.startswith('%_sn'):
            pmodel = pmodel[4:]
        elif pmodel.startswith('%_a') or pmodel.startswith('%_b') or pmodel.startswith('%_c'):
            pmodel = pmodel[3:]
        pmodel = pmodel.lstrip('%_').strip()

        pmaterial = pmaterial and pmaterial.strip() or ''
        pcolor = pcolor and pcolor.strip() or ''
        url = 'http://img.yvogue.hk/pimg/p%s/%d/m%s/m%s/c%s.jpg' %(isize, pbid, pmodel, pmaterial, pcolor)
        return url.lower()

    def check_image_url(self):
        pbid =  self.brand_id
        pmodel = self.model and self.model + '%' or '%'
        pmaterial = self.material and self.material + '%' or '%'
        pcolor = self.color and  self.color + '%' or '%'
        if not pbid:
            return False

        try:
            import psycopg2
            conn = psycopg2.connect(host='127.0.0.1', port=17810, user='pimage', dbname='pimage')
            cr = conn.cursor()
            cr.execute("select id from pimage_image where album_id in (select id from pimage_album where brand_id= %s and model like %s and material like %s and color like %s) and hide=False",(pbid,pmodel,pmaterial,pcolor))
            if cr.fetchall():
                conn.close()
                return True
            conn.close()
            return False
        except:
            return True
        return False

class Product_Product(models.Model):
    class Meta:
        app_label = 'product'
    id = models.IntegerField(primary_key=True)
    template =  models.ForeignKey('Product_Template', db_column='template_id')
    iid = models.CharField(max_length=16,default='0000000000000')
    name = models.CharField(max_length=128,default='default')
    size = models.CharField(max_length=32,default='NULL')

    ###############################################
    # xiongxiong #
    def small_image(self):
        return self.template.image_url_from_attr(isize='s')
    def _stock_number(self):
        number = self.product_inventory_set.aggregate(Sum('qty_canbesold'))['qty_canbesold__sum']
        return number if number else 0
    def price_special(self):
        return self.product_inventory_set.filter(qty_canbesold__gt=0).aggregate(Max('price_special'))['price_special__max']
    ###############################################


    ###############################################
    # zhangxue
    ###############################################
    @classmethod
    @task
    def create_product(cls,vals):
        """ create product from erp and create template """
        try:
            if isinstance(cls,str):
                cls = eval(cls)
            res = cls(**vals)
            res.save()
            return res.id
        except Exception as e:
            logger.warn('create product product[[%r]] fail:%r', vals, e)
            return 'Error: Create product product[[%r]] fail: %r' %(vals, e)

    @classmethod
    @task
    def write_product(cls,id,vals):
        """ modify """
        try:
            if isinstance(cls,str):
                cls = eval(cls)
            p = cls.objects.filter(id=id)
            if not p:
                vals.update({'id':id})
                return Product_Product.create_product(cls, vals)
            for key,value in vals.items():
                if key in ('template_id', ):
                    key = key.split('_id')[0]
                if value == False:
                    exec("p.update(%s=None)"%key)
                else:
                    exec("p.update(%s='''%s''')"%(key,value))
            return id
        except Exception as e:
            logger.warn('write product product[[%r]] fail:%r', vals, e)
            return 'Error: Write product product[[%r:%r]] fail: %r' %(id, vals, e)

class Product_Inventory(models.Model):
    date_create = models.DateTimeField(default=lambda:now())
    date_write = models.DateTimeField(default=lambda:now())
    template =  models.ForeignKey('Product_Template', db_column='template_id', related_name='inventorys')
    product = models.ForeignKey('Product_Product', db_column='product_id')
    name = models.CharField(max_length=128)
    price_special = models.FloatField(null=True)
    price_consignment = models.FloatField(null=True)
    qty_canbesold = models.IntegerField(default=0)
    qty_history = models.IntegerField(default=0)
    location_id = models.IntegerField(default=LOCATION_ID)
    #入库时间
    date = models.DateTimeField(default=lambda:now())
    company_id = models.IntegerField(default=COMPANY_ID)

    # xiongxiong
    class Meta:
        app_label = 'product'
    # add permission
        permissions = (
                ("view_price_special", "Can see price_special"),)

    def __init__(self, *args, **kwargs):
        res = super(Product_Inventory, self).__init__(*args, **kwargs)
        if self and self.pk:
            self.name = self.template.name
        return res

    @classmethod
    def filter_price_special(cls):
        return cls.objects.exclude(Q(price_special__lt=0.01)|Q(price_special=None))

    @classmethod
    def filter_price_consignment(cls):
        return cls.objects.exclude(Q(price_consignment__lt=0.01)|Q(price_consignment=None))

    @classmethod
    def filter_xianhuo(cls):
        return cls.objects.exclude(qty_canbesold=0)

    @classmethod
    def filter_categ(cls, categ):
        return cls.objects.filter(template__categ=categ)
    @classmethod
    def categ_filter(cls, category):
        return {'template__categ': category}

    @classmethod
    def brand_filter(cls, brand):
        return {'template__brand': brand}
    # xiongxiong

    ###############################################
    # zhangxue
    ###############################################
    @classmethod
    @task
    def create_write(cls,vals):
        if isinstance(cls,str):
            cls = eval(cls)
        #更新特惠价
        if vals.get('special',False) and 'price_special' in vals:
            p = cls.objects.filter(product_id=vals.get('product_id'),company_id=vals.get('company_id',COMPANY_ID))
            if p:
                p.update(price_special=vals['price_special'])
            return vals.get('product_id')
        #更新代销价
        if vals.get('consignment',False) and 'price_consignment' in vals:
            p = cls.objects.filter(product_id=vals.get('product_id'),company_id=vals.get('company_id',COMPANY_ID))
            if p:
                p.update(price_consignment=vals['price_consignment'])
            return vals.get('product_id')
        #更新产品可售数量、历史销售记录
        p = cls.objects.filter(product_id=vals.get('product_id'),company_id=vals.get('company_id',COMPANY_ID),location_id=vals.get('location_id',LOCATION_ID)).first()
        if p:
            if 'qty_canbesold' in vals:
                p.qty_canbesold = vals.get('qty_canbesold')
            p.date_write = now()
            p.qty_history += vals.get('qty_history',0)
            p.save()
            return p.id
        try:
            product_id = vals.get('product_id')
            product_obj = Product_Product.objects.filter(id=vals.get('product_id')).first()
            date = now()
            if vals.get('date',False):
                if isinstance(vals['date'],str):
                    date = datetime.strptime(vals['date'],'%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc).astimezone(pytz.utc)
                elif isinstance(vals['date'],datetime):
                    date = vals['date'].replace(tzinfo=pytz.utc).astimezone(pytz.utc)
            res = cls(
                    product_id = product_id,
                    template_id = product_obj.template_id,
                    name = product_obj.template.name,
                    location_id = vals.get('location_id'),
                    company_id = vals.get('company_id'),
                    qty_canbesold = vals.get('qty_canbesold'),
                    price_special = vals.get('price_special'),
                    date=date,
                    qty_history = vals.get('qty_history',0),
                    )
            res.save()
            return res.id
        except Exception as e:
            logger.warn('create or write product inventory[[%r]] fail:%r', vals, e)
            return 'Error: Create or write product inventory[[%r]] fail: %r' %(vals, e)

    @classmethod
    def read_inventory(cls, inventory_id=None, type='spot', fields={}, context=None):
        result_inventorys = []
        company_id = fields.get('company_id',COMPANY_ID)
        product_id = fields.get('product_id',False)
        template_id = fields.get('template_id',False)
        #返回特惠产品
        if type == 'special':
            #返回单个产品库存数量
            if product_id:
                p = cls.objects.filter(company_id=company_id, product_id=product_id, price_special__gt=0).values_list('qty_canbesold',flat=True)
                return p and p[0] or 0
            #返回产品模板的所有库存情况
            if template_id:
                result = cls.objects.filter(company_id=company_id, template_id=template_id, qty_canbesold__gt=0, price_special__gt=0).values_list('template_id','name').annotate(Sum('qty_canbesold'))
            else:
                result = cls.objects.filter(company_id=company_id, qty_canbesold__gt=0, price_special__gt=0).values_list('template_id','name').annotate(Sum('qty_canbesold'))
        #返回现货产品
        else:
            if product_id:
                p = cls.objects.filter(company_id=company_id,product_id=product_id).values_list('qty_canbesold',flat=True)
                return p and p[0] or 0
            if template_id:
                result = cls.objects.filter(company_id=company_id, template_id=template_id, qty_canbesold__gt=0).values_list('template_id','name').annotate(Sum('qty_canbesold'))
            else:
                result = cls.objects.filter(company_id=company_id, qty_canbesold__gt=0).values_list('template_id','name').annotate(Sum('qty_canbesold'))
        for res in result:
            template = Product_Template.objects.filter(id=res[0]).first()
            other_template = Product_Inventory.objects.filter(template__brand=template.brand, template__categ=template.categ,template__model=template.model, template__material=template.material, qty_canbesold__gt=0).exclude(template__color=template.color).distinct('template').all()
            same_cate = Product_Inventory.objects.filter(template__categ=template.categ, qty_canbesold__gt=0).exclude(template=template).distinct('template').all()
            res_inventory = {
                    'name':res[1],
                    'template_id':res[0],
                    'template':template,
                    'img':template.image_url_from_attr('l'),
                    'categ_id':[template.categ_id, template.categ.name],
                    'brand_id':[template.brand_id, template.brand.name],
                    'model':template.model,
                    'material':template.material,
                    'color':template.color,
                    'sizes':[],
                    'qty_total':res[-1],
                    'price_eu':template.hx_price_eu,
                    'price_hk':template.hx_price_hk,
                    'price_special':0,
                    'price_discount':0,
                    'date':'',
                    'qty_history':0,
                    'other_template': other_template,
                    'same_cate':same_cate,
                    }
            inventorys = cls.objects.filter(template_id=res[0],qty_canbesold__gt=0).all()
            for inventory in inventorys:
                if inventory.product.size:
                    res_inventory['sizes'].append({'name':inventory.product.size,'num':inventory.qty_canbesold, 'product_id':inventory.product_id})
                #特惠价取最大的
                if inventory.price_special and inventory.price_special > res_inventory['price_special']:
                    res_inventory['price_special'] = inventory.price_special
                    res_inventory['price_discount'] = template.hx_price_hk and int(inventory.price_special / template.hx_price_hk * 100) or 0
                #入库时间为最早的
                if (not res_inventory['date']) or inventory.date < res_inventory['date']:
                    res_inventory['date'] = inventory.date
                """
                if inventory.product.color and inventory.product.color not in  res_inventory['colors']:
                    res_inventory['colors'].append(inventory.product.color)
                if inventory.template.image_url_from_attr('l') not in res_inventory['imgs']:
                    res_inventory['imgs'].append(inventory.template.image_url_from_attr('l'))
                res_inventory['inventorys'].append({
                    'inventory_id':inventory.id,
                    'size':inventory.product.size,
                    'color':inventory.product.color,
                    'lussomoda_price':inventory.template.hx_price_hk,
                    'price_special':inventory.price_special,
                    'qty':inventory.qty_canbesold,
                    'qty_history':inventory.qty_history,
                    'img':inventory.image_url_from_attr('l')
                    })
                #欧洲价 价格范围
                if (not res_inventory['price_eu_range'][0]) or inventory.template.hx_price_eu < res_inventory['price_eu_range'][0]:
                    res_inventory['price_eu_range'][0] = inventory.product.hx_price_eu
                if (not res_inventory['price_eu_range'][1]) or inventory.product.hx_price_eu > res_inventory['price_eu_range'][1]:
                    res_inventory['price_eu_range'][1] = inventory.product.hx_price_eu
                #香港价 价格范围
                if (not res_inventory['price_hk_range'][0]) or inventory.product.hx_price_hk < res_inventory['price_hk_range'][0]:
                    res_inventory['price_hk_range'][0] = inventory.product.hx_price_hk
                if (not res_inventory['price_hk_range'][1]) or inventory.product.hx_price_hk > res_inventory['price_hk_range'][1]:
                    res_inventory['price_hk_range'][1] = inventory.product.hx_price_hk
            #如果价格范围唯一
            if res_inventory['price_eu_range'][0] == res_inventory['price_eu_range'][1]:
                res_inventory['price_eu_range'] = res_inventory['price_eu_range'][0]
            else:
                res_inventory['price_eu_range'] = str(res_inventory['price_eu_range'][0]) + '-' + str(res_inventory['price_eu_range'][1])
            if res_inventory['price_hk_range'][0] == res_inventory['price_hk_range'][1]:
                res_inventory['price_hk_range'] = res_inventory['price_hk_range'][0]
            else:
                res_inventory['price_hk_range'] = str(res_inventory['price_hk_range'][0]) + '-' + str(res_inventory['price_hk_range'][1])
            #产品页首图片
            res_inventory.update({'img':res_inventory['imgs'][0]})
                """
            #产品尺寸排序
            res_inventory['sizes'] = sorted(res_inventory['sizes'], key = lambda x:x['name'])
            result_inventorys.append(res_inventory)
        return result_inventorys

    @classmethod
    def read_inventorys(cls, type, company_id=COMPANY_ID):
        result_inventorys = []
        if type == 'special':
            result = cls.objects.filter(company_id=company_id, qty_canbesold__gt=0, price_special__gt=0).values_list('template_id','name').annotate(Sum('qty_canbesold'))
        else:
            result = cls.objects.filter(company_id=company_id, qty_canbesold__gt=0).values_list('template_id','name').annotate(Sum('qty_canbesold'))
        for res in result:
            template = Product_Template.objects.filter(id=res[0]).first()
            res_inventory = {
                    'name':res[1],
                    'template_id':res[0],
                    'imgs':template.image_url_from_attr('l'),
                    'qty_total':res[-1],
                    'price_eu':template.hx_price_eu,
                    'price_hk':template.hx_price_hk,
                    'price_special':0,
                    'date':'',
                    'qty_history':0
                    }
            inventorys = cls.objects.filter(template_id=res[0]).all()
            for inventory in inventorys:
                #特惠价取最大的
                if inventory.price_special and inventory.price_special > res_inventory['price_special']:
                    res_inventory['price_special'] = inventory.price_special
                #入库时间为最早的
                if (not res_inventory['date']) or inventory.date < res_inventory['date']:
                    res_inventory['date'] = inventory.date
            result_inventorys.append(res_inventory)
        return result_inventorys

    ###----------------------home page---------------------------###
    @classmethod
    def read_new(cls,company_id=COMPANY_ID):
        result_new = []
        cates = Product_Category.objects.filter(parent_id=65).all()
        for categ in cates:
            result_old = {'name':categ.name, 'products':[]}
            result = cls.objects.filter(company_id=company_id, qty_canbesold__gt=0, template__categ__big_parent=categ).order_by('-date').all()[:50]
            result = cls.objects.filter(id__in=result).distinct('template').all()
            if len(result) < 6:
                continue
            result = random.sample(list(result),6)
            for res in result:
                if not res.template.check_image_url():
                    continue
                res_inventory = {
                    'name':res.name,
                    'template_id':res.template_id,
                    'url':res.template.get_absolute_url(),
                    'img':res.template.image_url_from_attr('m'),
                    'hx_price_eu':res.template.hx_price_eu,
                    'hx_price_hk':res.template.hx_price_hk,
                    'price_special':res.price_special,
                    }
                result_old['products'].append(res_inventory)
            if result_old['products'] and len(result_old['products'])>5:
                result_old['products'] = result_old['products'][:6]
                result_new.append(result_old)
        return result_new

    @classmethod
    def read_hot(cls,company_id=COMPANY_ID):
        result_new = []
        cates = Product_Category.objects.filter(parent_id=65).all()
        for categ in cates:
            result_old = {'name':categ.name, 'products':[]}
            result = cls.objects.filter(company_id=company_id, qty_canbesold__gt=0, template__categ__big_parent=categ).order_by('-qty_history').all()[:50]
            result = cls.objects.filter(id__in=result).distinct('template').all()
            if len(result) < 6:
                continue
            result = random.sample(list(result),6)
            for res in result:
                if not res.template.check_image_url():
                    continue
                res_inventory = {
                    'name':res.name,
                    'template_id':res.template_id,
                    'url':res.template.get_absolute_url(),
                    'img':res.template.image_url_from_attr('m'),
                    'hx_price_eu':res.template.hx_price_eu,
                    'hx_price_hk':res.template.hx_price_hk,
                    'price_special':res.price_special,
                    }
                result_old['products'].append(res_inventory)
            if result_old['products'] and len(result_old['products'])>5:
                result_old['products'] = result_old['products'][:6]
                result_new.append(result_old)
        return result_new

    @classmethod
    def read_cheap(cls,company_id=COMPANY_ID):
        result_new = []
        cates = Product_Category.objects.filter(parent_id=65).all()
        for categ in cates:
            result_old = {'name':categ.name, 'products':[]}
            result = cls.objects.filter(company_id=company_id, qty_canbesold__gt=0, price_special__gt=0, template__categ__big_parent=categ).order_by('-price_special').all()[:50]
            result = cls.objects.filter(id__in=result).distinct('template').all()
            if len(result) < 6:
                continue
            result = random.sample(list(result),6)
            for res in result:
                if not res.template.check_image_url():
                    continue
                res_inventory = {
                    'name':res.name,
                    'template_id':res.template_id,
                    'url':res.template.get_absolute_url(),
                    'img':res.template.image_url_from_attr('m'),
                    'hx_price_eu':res.template.hx_price_eu,
                    'hx_price_hk':res.template.hx_price_hk,
                    'price_special':res.price_special,
                    }
                result_old['products'].append(res_inventory)
            if result_old['products'] and len(result_old['products'])>5:
                result_old['products'] = result_old['products'][:6]
                result_new.append(result_old)
        return result_new

    ###----------------------product page---------------------------###
    @classmethod
    def read_same_cate(cls,template_id):
        template_list = []
        template_obj = Product_Template.objects.filter(id=template_id).first()
        categ_id = template_obj.categ_id
        brand_id = template_obj.brand_id
        #同品牌或者同品类
        result = list(cls.objects.filter(Q(company_id=COMPANY_ID, qty_canbesold__gt=0), Q(template__categ_id=categ_id)|Q(template__brand_id=brand_id)).exclude(template_id=template_id).all())
        inventorys = random.sample(result, min(len(result),12))
        #同品牌不同品类
        #inventory2 = Product_Inventory.objects.filter(company_id=COMPANY_ID, qty_canbesold__gt=0, template__brand_id=brand_id).exclude(template_id=template_id, template__categ_id=categ_id).first()
        #不同品牌同品类
        #inventory3 = Product_Inventory.objects.filter(company_id=COMPANY_ID, qty_canbesold__gt=0, template__categ_id=categ_id).exclude(template_id=template_id, template__brand_id=brand_id).first()
        #卖的好
        result = list(cls.objects.filter(company_id=COMPANY_ID, qty_canbesold__gt=0, template__categ_id=categ_id).exclude(template_id=template_id).order_by('-qty_history').all()[:10])
        inventory4 = random.sample(result, min(len(result),4))
        #最新
        result = list(cls.objects.filter(company_id=COMPANY_ID, qty_canbesold__gt=0, template__categ_id=categ_id).exclude(template_id=template_id).order_by('-date').all()[:10])
        inventory5 = random.sample(result, min(len(result),4))
        inventorys.extend(inventory4)
        inventorys.extend(inventory5)
        result = []
        for inventory_obj in inventorys:
            if inventory_obj.template_id in template_list or not inventory_obj.template.check_image_url():
                continue
            template_list.append(inventory_obj.template_id)
            res_inventory = {
                    'name':inventory_obj.name,
                    'template_id':inventory_obj.template_id,
                    'url':inventory_obj.template.get_absolute_url(),
                    'img':inventory_obj.template.image_url_from_attr('m'),
                    'hx_price_eu':inventory_obj.template.hx_price_eu,
                    'hx_price_hk':inventory_obj.template.hx_price_hk,
                    'price_special':inventory_obj.price_special,
                    }
            result.append(res_inventory)
        return result

    @classmethod
    def read_same_buyer(cls,template_id):
        #购买此产品的客户
        buyers = Sale_Order.objects.filter(order_lines__product__template_id = template_id).values_list('partner_id',flat=True)
        #购买此产品的客户购买的其他产品
        other_products = Sale_Order_Line.objects.filter(order__partner_id=buyers).exclude(product__template_id=template_id).values_list('product_id',flat=True)
        inventorys = cls.objects.filter(product_id__in=other_products, company_id=COMPANY_ID , qty_canbesold__gt=0).distinct('template').all()
        result = []
        for inventory_obj in inventorys:
            if not inventory_obj.template.check_image_url():
                continue
            res_inventory = {
                    'name':inventory_obj.name,
                    'template_id':inventory_obj.template_id,
                    'url':inventory_obj.template.get_absolute_url(),
                    'img':inventory_obj.template.image_url_from_attr('m'),
                    'hx_price_eu':inventory_obj.template.hx_price_eu,
                    'hx_price_hk':inventory_obj.template.hx_price_hk,
                    'price_special':inventory_obj.price_special,
                    }
            result.append(res_inventory)
        return result


    def get_price_vip(self,user):
        groups = user.groups.all()
        vip = groups and groups[0]
        if self.price_special and vip and vip.name in ('VIP2','VIP5'):
            return self.price_special
        return self.template.hx_price_hk

    @classmethod
    def guess_u_like(cls,items,company_id=COMPANY_ID):
        #不包含特惠产品
        result_new = []
        if items:
            categ_id_list = []
            brand_id_list = []
            product_id_list = []
            for item in items:
                categ_id_list.append(item.product.template.categ_id)
                brand_id_list.append(item.product.template.brand_id)
                product_id_list.append(item.product_id)
            #同品牌或者同品类
            result = cls.objects.filter(Q(company_id=COMPANY_ID, qty_canbesold__gt=0), Q(template__categ_id__in=categ_id_list)|Q(template__brand_id__in=brand_id_list)).exclude(product_id__in=product_id_list, price_special__gt=0).all()
        else:
            result = cls.objects.filter(company_id=company_id, qty_canbesold__gt=0).exclude(price_special__gt=0).order_by('-qty_history').all()[:50]
        result = list(cls.objects.filter(id__in=result).distinct('template').all())
        inventorys = random.sample(result, min(len(result),12))
        for inventory_obj in inventorys:
            if not inventory_obj.template.check_image_url():
                continue
            res_inventory = {
                    'name':inventory_obj.name,
                    'template_id':inventory_obj.template_id,
                    'product_id':inventory_obj.product_id,
                    'url':inventory_obj.template.get_absolute_url(),
                    'img':inventory_obj.template.image_url_from_attr('m'),
                    'price':inventory_obj.template.hx_price_hk,
                    }
            result_new.append(res_inventory)
        return result_new[:5]




class Sale_Order(models.Model):
    class Meta:
        app_label = 'product'
    DRAFT = 'draft'
    CONFIRMED = 'confirmed'
    PAID = 'paid'
    TRANSFERRED = 'transferred'
    CANCEL = 'cancel'
    STATUS = (
        (DRAFT, '等待审核'),
        (CONFIRMED, '等待付款'),
        (PAID, '等待发货'),
        (TRANSFERRED, '完成'),
        (CANCEL, '取消'),
    )

    name = models.CharField(max_length=64,default='SO')
    partner = models.ForeignKey(User, db_column='partner_id', related_name='order_list')
    pricelist_id = models.IntegerField(default=PRICELIST_ID)
    location_id = models.IntegerField()
    qty_total = models.IntegerField(default=0)
    price_total = models.FloatField(default=0)
    state = models.CharField(max_length=16, choices=STATUS, default=DRAFT)
    date_create = models.DateTimeField(default=now())
    date_confirmed = models.DateTimeField(null=True)
    date_paid = models.DateTimeField(null=True)
    date_transferred = models.DateTimeField(null=True)
    company_id = models.IntegerField(default=COMPANY_ID)

    @classmethod
    @transaction.commit_manually
    def create_order(cls,vals):
        sid = transaction.savepoint()
        try:
            #默认库位是LOCATION_ID
            order = cls(
                    partner_id=vals.get('partner_id',1),
                    pricelist_id=vals.get('pricelist_id',PRICELIST_ID),
                    location_id = LOCATION_ID,
                    company_id = vals.get('company_id',COMPANY_ID),
                    )
            order.save()
            transaction.savepoint_commit(sid)
            qty_total = 0
            price_total = 0
            new_lines = []
            default_order = {
                    'partner_name' : vals.get('partner_name'),
                    'partner_id' : vals.get('erp_partner_id'),
                    'pricelist_id' : vals.get('pricelist_id',PRICELIST_ID),
                    'location_id' : LOCATION_ID,
                    'company_id' : vals.get('company_id',COMPANY_ID),
                    }
            for line in vals.get('lines',[]):
                qty_total += line.get('qty')
                price_total += line.get('qty') * line.get('price_unit')
                new_line = order.order_lines.create(
                            order = order,
                            product_id = line.get('product_id'),
                            qty = line.get('qty'),
                            price_unit = line.get('price_unit'),
                            subtotal = round(line.get('qty') * line.get('price_unit'), 2),
                            )
                new_lines.append({'lu_line_id':new_line.id, 'product_id':line.get('product_id'), 'price_unit':line.get('price_unit'), 'qty':line.get('qty')})
            order.qty_total = qty_total
            order.price_total = round(price_total,2)
            order.save()
            default_order.update({'lu_order_id':order.id,'lines':new_lines})
            #创建订单14s即超时，取消task
            order_task = celery.send_task("erp_tasks.create_from_lussomoda",args=[default_order],routing_key = 'erp_tasks', queue='erp_tasks',expires=14)
            transaction.commit()
            return order.id,order_task
        except Exception as e:
            print('ERROR: Create Order  ',e)
            transaction.savepoint_rollback(sid)
        transaction.commit()
        return False,False

    @classmethod
    @task
    def write_order(cls,id,vals):
        if isinstance(cls,str):
            cls = eval(cls)
        order = cls.objects.filter(id=id).first()
        if not order:
            #erp下单太慢导致yvogue下单失败，需删除erp订单
            order_task = celery.send_task("erp_tasks.delete_order",args=[id],routing_key = 'erp_tasks', queue='erp_tasks')
            return {'status':False, 'info':'No order create %s'%id, 'lu_order_id':id}
        for key,value in vals.items():
            if key == 'lines':
                lines = []
                for line in value:
                    if line[0] == 0:
                        new_line = order.order_lines.create(
                                order = order,
                                product_id = line[2].get('product_id'),
                                qty = line[2].get('qty'),
                                price_unit = line[2].get('price_unit'),
                                subtotal = round(line[2].get('qty') * line[2].get('price_unit'),2),
                                )
                        lines.append({'product_id':line[2].get('product_id'), 'lu_line_id':new_line.id})
                    elif line[0] == 1:
                        new_line = Sale_Order_Line.objects.filter(id=line[1])
                        for key,value in line[2].items():
                            exec("new_line.update(%s='''%s''')"%(key,value))
                    else:
                        new_line = Sale_Order_Line.objects.filter(id=line[1])
                        new_line.delete()

                qty_total = 0
                price_total = 0
                for line_obj in order.order_lines.all():
                    qty_total += line_obj.qty
                    subtotal = round(line_obj.qty * line_obj.price_unit,2)
                    line_obj.subtotal = subtotal
                    line_obj.save()
                    price_total += subtotal
                order.qty_total = qty_total
                order.price_total = round(price_total,2)
                order.save()

                if lines:
                    line_task = celery.send_task("erp_tasks.write_order",args=[{'lu_order_id':id,'lines':lines}],routing_key = 'erp_tasks', queue='erp_tasks')
            else:
                exec("order.%s = '''%s'''"%(key,value))
            order.save()
        return order.id

    @classmethod
    def read_order_num(cls, partner_id, context=None):
        if not partner_id:
            return False
        company_id = context.get('company_id',COMPANY_ID)
        order_num = {'draft':0, 'confirmed':0, 'paid':0, 'transferred':0, 'cancel':0}
        result_state = cls.objects.filter(company_id=company_id, partner_id=partner_id).values_list('state').annotate(Count('id'))
        for res in result_state:
            order_num[res[0]] = res[1]
        return order_num

    @classmethod
    def read_order(cls, ids, partner_id, state, context=None):
        result_orders = []
        company_id = context.get('company_id',COMPANY_ID)
        #partner_id = fields.get('partner_id', False)
        #state = fields.get('state', False)
        if ids:
            result = cls.objects.filter(id__in=ids).order_by('-id').all()
        elif partner_id and not state:
            result = cls.objects.filter(company_id=company_id, partner_id=partner_id).order_by('-id').all()
        elif partner_id and state:
            result = cls.objects.filter(company_id=company_id, partner_id=partner_id, state=state).order_by('-id').all()
        else:
            result = cls.objects.filter(company_id=company_id).order_by('-id').all()
        for res in result:
            #订单详情
            if ids:
                lines = []
                order_lines = res.order_lines.all()
                for line in order_lines:
                    lines.append({
                        'line_id':line.id,
                        'product':{
                            'id':line.product_id,
                            'name':line.product.name,
                            'material':line.product.template.material,
                            'model':line.product.template.model,
                            'color':line.product.template.color,
                            'size':line.product.size,
                            'get_absolute_url':line.product.template.get_absolute_url(),
                            'img':line.product.template.image_url_from_attr('s'),
                            },
                        'qty':line.qty,
                        'price_unit':line.price_unit,
                        'subtotal':line.subtotal,
                        })
                result_orders.append({
                        'id':res.id,
                        'name':res.name,
                        'qty_total':res.qty_total,
                        'price_total':res.price_total,
                        'state':res.state,
                        'status':res.get_state_display(),
                        'date_create':res.date_create,
                        'date_confirmed':res.date_confirmed,
                        'date_paid':res.date_paid,
                        'date_transferred':res.date_transferred,
                        'lines':lines,
                        })
            #订单列表
            else:
                result_orders.append({
                    'id':res.id,
                    'name':res.name,
                    'qty_total':res.qty_total,
                    'price_total':res.price_total,
                    'state':res.state,
                    'status':res.get_state_display(),
                    'date_create':res.date_create,
                    'date_confirmed':res.date_confirmed,
                    'date_paid':res.date_paid,
                    'date_transferred':res.date_transferred,
                    'detail_url':res.get_order_url(),
                    })
        return result_orders

    @classmethod
    def read_order_list(cls, partner_id, state, context=None):
        if not partner_id:
            return False
        order_num = cls.read_order_num(partner_id, context=context)
        order_list = cls.read_order([], partner_id, state, context)
        return { 'order_num':order_num, 'order_list':order_list}

    @classmethod
    def read_order_detail(cls, id, context=None):
        if not id:
            return False
        if not context:
            context = {}
        order_list = cls.read_order([id], None, None, context)
        if order_list:
            return { 'order':order_list[0]}

    @classmethod
    @task
    def action_cancelled(cls, id, iferp=None):
        if isinstance(cls,str):
            cls = eval(cls)
        order = cls.objects.filter(id=id).first()
        #action from lussomoda partner
        if not iferp:
            if order.state != 'draft':
                return {'status':False,'info':'Error:Not drft order %s'%id}
            order_task = celery.send_task("erp_tasks.action_cancel_lussomoda", args = [id], routing_key = 'erp_tasks', queue ='erp_tasks')
        #action from erp user
        order.state = 'cancel'
        order.save()
        return id

    @classmethod
    @task
    def action_confirmed(cls, id):
        if isinstance(cls,str):
            cls = eval(cls)
        order = cls.objects.filter(id=id).first()
        order.state = 'confirmed'
        order.date_confirmed = now()
        order.save()
        return id

    @classmethod
    @task
    def action_paid(cls, id):
        if isinstance(cls,str):
            cls = eval(cls)
        order = cls.objects.filter(id=id).first()
        order.state = 'paid'
        order.date_paid = now()
        order.save()
        return id

    @classmethod
    @task
    def action_transferred(cls, id):
        if isinstance(cls,str):
            cls = eval(cls)
        order = cls.objects.filter(id=id).first()
        order.state = 'transferred'
        order.date_transferred = now()
        order.save()
        return id

    def get_order_url(self):
        return '/order/detail/'+ str(self.id) + '/'

class Sale_Order_Line(models.Model):
    class Meta:
        app_label = 'product'
    order = models.ForeignKey('Sale_Order', db_column='order_id', related_name='order_lines')
    product = models.ForeignKey('Product_Product', db_column='product_id')
    qty = models.IntegerField()
    price_unit = models.FloatField()
    subtotal = models.FloatField()

