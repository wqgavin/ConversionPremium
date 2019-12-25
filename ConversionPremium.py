from jqdata import *
from jqlib.technical_analysis import *
import numpy as np
import pandas as pd
import time
import requests
import json
import re
code_depot = 0

# 可转债爬虫
def code_spider():
    base_url = 'https://www.jisilu.cn/data/cbnew/cb_list/?___jsl=LST___t=1551579067513'
    code_ini_data = get_code_dict(base_url)
    return code_ini_data
# 可转债信息爬虫内部
def get_code_dict(url):
    headers = {
        'Referer': 'https://www.jisilu.cn/data/cbnew/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0'
    }
    response = requests.get(url, headers= headers)
    text = response.text
    
    # 爬取转债代码
    bond_list = re.findall('pre_bond_id\"\:\"s[h|z]\d{6}', text)
    bond_list = re.findall('s[h|z]\d{6}', str(bond_list))
    
    # 爬取转债名称
    bond_name_list = re.findall(u'bond_nm\"\:\"(.+?)\"', text)
    
    # 爬取正股代码
    stock_list = re.findall('stock_cd\"\:\"\d{6}', text)
    stock_list = re.findall('\d{6}', str(stock_list))
    
    # 爬取正股名称
    stock_name_list = re.findall(u'stock_nm\"\:\"(.+?)\"', text)
    
    # 爬取转股日期
    convert_list = re.findall('convert_price\"\:\"(.+?)\"', text)
    convert_list = re.findall('\d{1,2}.\d{3}', str(convert_list))
    convert_list = map(float, convert_list)
    convert_dt_list = re.findall('convert_dt\"\:\"(.+?)\"', text)
    
    # 爬取最后日期
    maturity_dt_list = re.findall('\"maturity_dt\"\:\"(.+?)\"', text)
    data = {"bond_code": bond_list,"bond_name": bond_name_list ,"stock_code": stock_list, "stock_name": stock_name_list, "convert_price": convert_list, "convert_dt": convert_dt_list, "maturity_dt": maturity_dt_list}
    code_ini_data = pd.DataFrame(data)
    
    # 修改为sina交易代码
    for i in range(len(code_ini_data.index)):
        if code_ini_data['bond_code'][i][0:2] == 'sz':
            code_ini_data.loc[[i], ['stock_code']] = 'sz' + code_ini_data['stock_code'][i]
        elif code_ini_data['bond_code'][i][0:2] == 'sh':
            code_ini_data.loc[[i], ['stock_code']] = 'sh' + code_ini_data['stock_code'][i]
    return code_ini_data



# 可转债盘口数据
def get_bond_data(url):
    real_price = 0
    a1_v = 0
    a1_p = 0
    a2_v = 0
    a2_p = 0
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0'
    }
    response = requests.get(url, headers= headers)
    text = response.text
    if len(text) >= 100:
        ret = re.split(',',text)
        real_price = float(ret[3])
        a1_v = int(ret[20]) / 10
        a1_p = float(ret[21])
        a2_v = int(ret[22]) / 10
        a2_p = float(ret[23])
    return real_price, a1_v, a1_p, a2_v, a2_p


# 正股盘口数据
def get_stock_data(url):
    real_price = 0
    b1_v = 0
    b1_p = 0
    b2_v = 0
    b2_p = 0
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0'
    }
    response = requests.get(url, headers= headers)
    text = response.text
    if len(text) >= 100:
        ret = re.split(',',text)
        real_price = float(ret[3])
        b1_v = int(ret[10]) / 100
        b1_p = float(ret[11])
        b2_v = int(ret[12]) / 100
        b2_p = float(ret[13])
    return real_price, b1_v, b1_p, b2_v, b2_p



# 整合数据集
def bond_spider():
    code_data = []
    bond_list = []
    stock_list = []
    bond_price_list = []
    bond_a1_v_list = []
    bond_a1_p_list = []
    bond_a2_v_list = []
    bond_a2_p_list = []
    stock_price_list = []
    stock_b1_v_list = []
    stock_b1_p_list = []
    stock_b2_v_list = []
    stock_b2_p_list = []
    base_url = 'http://hq.sinajs.cn/list={}'
    code_dict = code_depot
    bond_list = code_dict['bond_code']
    stock_list = code_dict['stock_code']
    
    # 遍历可转债及正股行情数据
    for i in range(len(code_dict.index)):
        get_bond = base_url.format(code_dict['bond_code'][i])
        bond_price, bond_a1_v, bond_a1_p, bond_a2_v, bond_a2_p = get_bond_data(get_bond)
        bond_price_list.append(bond_price)
        bond_a1_v_list.append(bond_a1_v)
        bond_a1_p_list.append(bond_a1_p)
        bond_a2_v_list.append(bond_a2_v)
        bond_a2_p_list.append(bond_a2_p)
        
        get_stock = base_url.format(code_dict['stock_code'][i])
        stock_price, stock_b1_v, stock_b1_p, stock_b2_v, stock_b2_p = get_stock_data(get_stock)
        stock_price_list.append(stock_price)
        stock_b1_v_list.append(stock_b1_v)
        stock_b1_p_list.append(stock_b1_p)
        stock_b2_v_list.append(stock_b2_v)
        stock_b2_p_list.append(stock_b2_p)
    # 整合数据
    data = {"bond_code": bond_list, "bond_price": bond_price_list, "bond_a1_p": bond_a1_p_list, "bond_a1_v": bond_a1_v_list, \
            "bond_a2_p": bond_a2_p_list, "bond_a2_v": bond_a2_v_list, "stock_code": stock_list,"stock_price": stock_price_list, \
            "stock_b1_p": stock_b1_p_list, "stock_b1_v": stock_b1_v_list, "stock_b2_p": stock_b2_p_list, "stock_b2_v": stock_b2_v_list}
    code_data_init = pd.DataFrame(data)
    code_data = pd.merge(code_data_init,code_dict,on=None,how='outer',sort=False) 
    code_data = code_data[code_data['bond_price'] != 0]
    return code_data



# 溢价筛选
def get_bond_premium():
    code_data = bond_spider()
    
    # 转股价值计算
    code_data['convertible_value'] = code_data['stock_price'] * 100 / code_data['convert_price']
    
    # 溢价率计算
    code_data['premium_rate'] = code_data['bond_price'] / code_data['convertible_value'] - 1 
    
    code_data = code_data.sort_index(axis = 0,ascending = True,by = 'premium_rate').reset_index(drop=True)
    code_data = code_data.head(1)
    code_data['convertible_value'] = code_data['convertible_value'].apply(lambda x: format(x, '.2f'))
    code_data['premium_rate'] = code_data['premium_rate'].apply(lambda x: format(x, '.2%'))
    if len(code_data.index) > 0:
        print('------------{}可转债溢价报价------------'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
        for i in range(0,len(code_data.index)):
            
            print('\n套利时间:{0},\n套利溢价:{1},\n转债代码:{2},\n转债名称：{3},\n债券卖一:{4},\n卖一数量:{5},\n转债卖二:{6},\n卖二数量:{7},\n转股价值:{8},\n正股代码:{9},\n正股名称:{10}\n正股买一:{11},\n买一数量:{12},\n正股买二:{13},\n买二数量:{14},\n转股日期:{15},\n到期时间：{16}'.format( \
                     time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), code_data['premium_rate'][i], code_data['bond_code'][i], code_data['bond_name'][i], \
                     code_data['bond_a1_p'][i], code_data['bond_a1_v'][i], code_data['bond_a2_p'][i], code_data['bond_a2_v'][i], code_data['convertible_value'][i], \
                     code_data['stock_code'][i], code_data['stock_name'][i], code_data['stock_b1_p'][i], code_data['stock_b1_v'][i], code_data['stock_b2_p'][i], \
                     code_data['stock_b2_v'][i], code_data['maturity_dt'][i], code_data['convert_dt'][i]))




if __name__ == '__main__':
  code_depot = code_spider()
  code_depot.head(5)
  
# 可转债盘口数据打印
  real_price, a1_v, a1_p, a2_v, a2_p = get_bond_data('http://hq.sinajs.cn/list=sh113539')
  print('可转债：{}，最新价格：{}，卖一量：{}，卖一价：{}，卖二量：{}，卖二价：{}'.format('113539',real_price, a1_v, a1_p, a2_v, a2_p))
# 正股盘口数据打印
  real_price, b1_v, b1_p, b2_v, b2_p = get_stock_data('http://hq.sinajs.cn/list=sh603079')
  print('正股：{}，最新价格：{}，买一量：{}，买一价：{}，买二量：{}，买二价：{}'.format('603079',real_price, b1_v, b1_p, b2_v, b2_p))  
# 获取整合数据并打印
  code_data = bond_spider()
  code_data.head(5)  
# 打印溢价可转债列表
  get_bond_premium()  

