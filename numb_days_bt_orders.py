import pandas as pd
from pandas import *
import matplotlib.pyplot as plt
import numpy as np
#%matplotlib inline
orders=pd.read_csv("orders.csv")
customers = orders[['user_id','order_placed_tm']]
customer_group = customers.groupby(['user_id','order_placed_tm'])

def display_order_gaps(gaps):
    plt.hist(gaps)
    plt.ylabel('Count')
    plt.xlabel('Days')
    
def get_order_pairs(a):
    order_times = list(sorted(a))
    order_pairs = list()
    for i in range(0, len(order_times)-1):
        order_pairs.append((order_times[i], order_times[i+1]))
    return order_pairs

def get_order_gaps(order_ts_pairs):
    gaps = list()
    for o1, o2 in order_ts_pairs:
        gaps.append((o2-o1)/86400)
    print gaps

def read_csv(txt):
    customers = orders[['user_id','order_placed_tm']]
   # customer_group = customers.groupby(['user_id','order_placed_tm'])
    b = {user_id: pd.Series(grp['order_placed_tm']).tolist() for user_id, grp in customers.groupby(['user_id'])}
    c =[]
    for v in b.values():
        c.append(v)
    return c

def lists(ll):
    t = []
    for i in ll:
        if len(i) == 1:
            pass
        else:
            t = ((get_order_gaps(get_order_pairs(i))))
    return t
    
lists(read_csv(('order.csv')))