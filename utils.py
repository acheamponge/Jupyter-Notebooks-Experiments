import requests
import csv
from functools import partial
from itertools import chain
from collections import defaultdict
import os
import re
import json
from datetime import datetime

baseurl = 'https://api.pcb.ng/api/'

def get_auth_token():
    r = requests.post(baseurl + '/auth/tokens', json={'email':'dev@pcb.ng', 'password':'password'})
    r.raise_for_status()
    return r.json()['token']

def download_json(token, urlpath):
    r = requests.get(baseurl + urlpath, headers={'authorization': 'Token ' + token})
    r.raise_for_status()
    return r.json()

def get_cached_json(token, cache_path, urlpath):
    if not os.path.exists(cache_path):
        os.makedirs(cache_path)
    clean_path = re.sub(r'[^a-zA-Z0-9_.]', '', urlpath)
    fname = os.path.join(cache_path, clean_path + '.json')
    if os.path.exists(fname):
        with open(fname, 'r') as f:
            return json.load(f)
    else:
        obj = download_json(token, urlpath)
        with open(fname, 'w') as f:
            json.dump(obj, f)
        return obj

def per_user(objs, oid_to_uid=dict()):
    d = defaultdict(list)
    for obj in objs:
        if obj:
            uid = obj.get('user_id', None) or oid_to_uid.get(obj['id'], None)
            if uid:
                d[uid].append(obj)
    return d

def join_on_id(objs, other):
    obj_by_id = {o['id']: o for o in objs}
    user_id_by_obj_id = dict()
    for uid, others in other.iteritems():
        for o in others:
            user_id_by_obj_id[o['id']] = o['user_id']
    d = defaultdict(list)
    for obj in objs:
        if obj:
            uid = user_id_by_obj_id.get(obj['id'], None)
            if uid:
                d[uid].append(obj)
    return d 

def count(seq):
    return len(list(seq))

def pct(total, seq):
    c = count(seq)
    p = round(100.0*c/float(total), 1)
    return str(c) + '  (' + str(p) + '%)'
