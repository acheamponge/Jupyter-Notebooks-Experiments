import csv
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd 
import datetime
from matplotlib.dates import strpdate2num
 
customers=pd.read_csv("orders.csv")
#print datetime.datetime.now().date()
#print (datetime.datetime.fromtimestamp(int(customers['order_placed_tm'].values.strftime('%Y%m%d')))
customers['ordertime'] = datetime.datetime.strptime(str(datetime.datetime.now().replace(microsecond=0)),'%Y-%m-%d %H:%M:%S') 
customers['new'] = customers['order_placed_tm'].values.astype('datetime64[s]')
customers['Days_since_order'] = customers['ordertime'] - customers['new']
customers['Total_Purchase'] = customers['is_repeat_purchase'] + customers['is_first_time_purchase']
customers['Date of registeration'] = (customers['order_placed_tm']-customers['seconds_since_user_registered']).values.astype('datetime64[s]')
header = ['Days_since_order', 'user_id', 'revenue', 'is_repeat_purchase', 'Date of registeration']
customers['last_order'] = customers['order_placed_tm'].values.astype('datetime64[s]')
all = pd.DataFrame(customers.groupby(['user_id']).agg({'revenue':sum, 'Total_Purchase': sum, 'Days_since_order': ['min', 'max'], 'Date of registeration' : ['max'], 'is_pcba': sum, 'is_bare_pcb': sum,'last_order': ['max']}))
#all.keys()


all['Average'] = (all[('Days_since_order', 'max')] - all[('Days_since_order', 'min')])/ all[('Total_Purchase', 'sum')] 

#all

all.sort_values([('Days_since_order', 'min')]).head(85).to_csv('top_customers.csv')
