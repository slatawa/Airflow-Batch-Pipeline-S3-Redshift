import re
import pandas as pd
import math

def extract_valid_search_data(search_data):
    valid_search = []
    searches = [rec.replace('\\\\n:', ',').replace('\\n', '') for rec in search_data.split('--\\\\n-:')][1:]
    search_list = []
    for rec in searches:
        search_list.append({x.split(':')[0]: x.split(':')[1] for x in rec.split(',')})
    for rec in search_list:
        if rec['enabled'] == 'True' and int(rec['clicks']) > 2:
            valid_search.append(rec)
        else:
            pass
    return valid_search


def calc_avg_listings(search_data):
    total_listings = 0
    if search_data:
        for rec in search_data:
            total_listings += int(rec['listings'])
    else:
        return 0
    return int(math.ceil(total_listings / len(search_data)))


def calc_search_type(search_data):
    rental_search = 0
    sales_search = 0
    none_type = 0
    for rec in search_data:
        if rec['type'] == 'Sales':
            sales_search += 1
        elif rec['type'] == 'Rental':
            rental_search += 1
        else:
            none_type += 1
    return pd.Series([rental_search, sales_search, none_type], index=['rental_search', 'sales_search', 'none_type'])


def extract_valid_searches(search_data):
    if search_data:
        return search_data['search_id']
    else:
        return ''