
from __future__ import print_function
import argparse
import sys
import requests
import csv
from functools import partial
from itertools import chain
from collections import defaultdict
import os
import re
import json
from datetime import datetime, timedelta
import utils as u
from toolz import dicttoolz as dz
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import strpdate2num 


def get_status_for(order):
    return get_json('/order_statuses/' + order['id'])

def get_rfq_for(order):
    return get_json('/order_quotes/' + order['order_rfq_id'])

def get_pcb_quote_for(order):
    rfq = get_rfq_for(order)
    board = get_json('/boards/' + rfq['board_id'])
    return get_json('/gerber/pcb_price_quotes/' + board['bundle_id'])

def get_bundle_for(order):
    rfq = get_rfq_for(order)
    board = get_json('/boards/' + rfq['board_id'])
    return get_json('/gerber/bundles/' + board['bundle_id'])

def get_bom_quote_for(order):
    try:
        rfq = get_rfq_for(order)
        board = get_json('/boards/' + rfq['board_id'])
        bom = get_json('/bom/boms/' + board['bom_id'])
        return get_json('/bom/bom_quotes/' + bom['bom_quote_id'])
    except:
        print('didnt get rfq')
        #print(order)

def print_json(obj):
    print(json.dumps(obj, indent=2))

def merge_users(get_json, orders):
    users = {u['user_id']: u for u in get_json('/auth/users')}
    return [dz.merge(o, {'user_obj': users[o['user_id']]}) for o in orders]

def order_created_at(o):
    try:
        return o.get('created_at', None) or o['rfq_obj'].get('time', None) or o['rfq_obj'].get('created_at', None) or o['rfq_obj']['updated_at']
    except:
        print('Badness!!!!!')
        print_json(o)
        raise Exception('bad')

def apply_manual_adjustments(row):
#     Oren's order - split payment into 2 parts, second chunk collected separately
    if row['id'] == 'd20b00d5-2041-4a4a-a154-7ff2ba820382':
        row['pcba_revenue'] += 481
    return row

def backfill_missing_data(row):
    val = lambda *ks: dz.get_in(ks, row, None)
    if val('user_obj', 'email') == 'peter@pcb.ng':
        row['user_obj']['created_at'] = 1456870022
    return row

def summarize(orders):
    seen_customers = set()
    num = lambda b: "1" if b else "0"
    for o in orders:
        o = backfill_missing_data(o)
        val = lambda *ks: dz.get_in(ks, o, None)
        subitems_total = lambda i: sum(float(si['price']) for si in i['subitems'])
        is_bare_pcb = (val('rfq_obj', 'order_type') == 'pcb-only')
        user_registered_at = val('user_obj', 'created_at') or 1459535237
        #print(val('rfq_obj', 'shipping_address') or val('rfq_obj'))
        row = {'id': val('id'),
               'user': val('user_obj', 'email'),
               'user_id': val('user_obj', 'user_id'),
               'is_bare_pcb': '1' if is_bare_pcb else '0',
               'is_pcba': num(val('rfq_obj', 'order_type') in ['pcb-and-assembly', None]),
               'revenue': val('rfq_obj', 'total_price'),
               'pcba_revenue': sum(subitems_total(i) for i in val('rfq_obj', 'items') if i['description'] == 'PCB'),
               'parts_revenue': sum(subitems_total(i) for i in val('rfq_obj', 'items') if i['description'] == 'Parts'),
               'other_revenue': sum(subitems_total(i) for i in val('rfq_obj', 'items') if i['description'] not in ['PCB', 'Parts']),
               'units_ordered': val('rfq_obj', 'quantity'),
               'board_area': val('pcb_quote_obj', 'board_width_mm') * val('pcb_quote_obj', 'board_height_mm') / (25.4 * 25.4),
               'parts_per_unit': len(val('bom_quote_obj', 'items')) if not is_bare_pcb else 0,
               'unique_parts_per_unit': len(set(i['dknum'].lower().replace(' ','') for i in val('bom_quote_obj', 'items'))) if not is_bare_pcb else 0,
               'is_domestic': num(val('rfq_obj', 'shipping_address', 'address-country') in ['US', None]),
               'is_international': num(val('rfq_obj', 'shipping_address', 'address-country') not in ['US', None]),
               'is_first_time_purchase': num(val('user_id') not in seen_customers),
               'is_repeat_purchase': num(val('user_id') in seen_customers),
               'year_month': datetime.fromtimestamp(order_created_at(o)).strftime('%Y-%m'),
               'order_placed_tm': order_created_at(o),
               'is_4layer_pcb': num('internal1' in val('bundle_obj', 'layers_urls') or 'internal2' in val('bundle_obj', 'layers_urls')),
               'is_2layer_pcb': num('internal1' not in val('bundle_obj', 'layers_urls') and 'internal2' not in val('bundle_obj', 'layers_urls')),
               'seconds_since_user_registered': order_created_at(o) - user_registered_at}
        seen_customers.add(val('user_id'))
        yield apply_manual_adjustments(row)

def dump_csv(f, rows):
    keys = ['year_month', 'order_placed_tm', 'id', 'user', 'user_id', 'is_first_time_purchase', 'is_repeat_purchase','pcba_revenue']
    writer = csv.DictWriter(f, fieldnames=keys)
    writer.writeheader()
    for r in rows:
        writer.writerow(r)

def new_customers(rows):
        rows['new'] = rows['order_placed_tm'].values.astype('datetime64[s]')
        alls = pd.DataFrame(rows.groupby(['user']).agg({'is_repeat_purchase':sum, 'new': max}))
        all = (alls.loc[alls['is_repeat_purchase'] == 0])
        print (str(len(all[(all['new'] < datetime.now().date()) & (all['new'] > (datetime.now().date() - timedelta(days =7)))]))+ ' new customers for the week ending ' + str( datetime.now().date()))

def main(get_json):
    orders = get_json('/admin/orders')
 # merge order status
    orders = [dz.merge(order, {'status_obj': get_status_for(order)}) for order in orders]
     # filter out errors and cancellations
    placed_orders = [o for o in orders if o['status_obj'].get('status', None) not in ['error', 'cancelled']]
    # merge rfq
    placed_orders = [dz.merge(o, {'rfq_obj': get_rfq_for(o),
                                  'bundle_obj': get_bundle_for(o),
                                  'pcb_quote_obj': get_pcb_quote_for(o),
                                  'bom_quote_obj': get_bom_quote_for(o)})
                    for o in placed_orders]
    # order by time
    placed_orders = sorted(placed_orders, key=order_created_at)
    # merge user records
    placed_orders = merge_users(get_json, placed_orders)

    rows = pd.DataFrame(list(summarize(placed_orders)))
    new_customers(rows)
           


if __name__ == '__main__':
    
    token = u.get_auth_token()
    get_json = partial(u.download_json, token)
    main(get_json)
    
