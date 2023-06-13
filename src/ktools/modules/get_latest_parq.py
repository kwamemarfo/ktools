#!/usr/bin/env python
# coding: utf-8


import re
import requests

s = requests.Session()

user_name = get_ipython().getoutput('cat ~/.k5login')
user_name = user_name[0].split("@")[0]

def get_latest_parquet(parquet_name):

    headers = {'X-Data-Refinery-Client-Id': user_name,               'X-Data-Refinery-Consumer-Type': 'INDIVIDUAL_USER', 'Content-Type': 'application/json'}

    s.headers.update(headers)
    batches = s.get("http://gbl20174697:12000/v4/batches").json()["snapshots"]
    batches = list(reversed(batches))
    
    for snap_date in batches:
        snap_date = snap_date['runId']
        asset_list = get_asset(snap_date, parquet_name)
        if len(asset_list) > 1:
            asset_list =             [x[0].split("asset': '")[-1].rstrip("'") if 'asset' in x[0] else x[0] for x in asset_list]
            print("SEARCH TOO AMBIGIOUS -- Suggestion:")
            print(len(asset_list))
            print(asset_list)
            break;
        elif len(asset_list) < 1:
            pass;
        else:
            parquet_location = get_location(asset_list, snap_date)
            if str(parquet_location).startswith("hdfs:"):
                print(parquet_location)
                return parquet_location;
            else:
                pass
    return;




def get_asset(snap_date, search_term):
    assets_link = f"http://gbl20174697:12000/v4/assets/{snap_date}/index"
    s = requests.Session()
    headers = {'X-Data-Refinery-Client-Id': user_name,               'X-Data-Refinery-Consumer-Type': 'INDIVIDUAL_USER', 'Content-Type': 'application/json'}

    s.headers.update(headers)
    assets = s.get(assets_link).json()
    asset_list = []
    search_term_pre = search_term.strip().split(" ")[0]
    if search_term_pre in ["mdas", "gdas", "cdas"]:
        search_term = " ".join(search_term.strip().split(" ")[1:])
        search_term = re.sub(r'[^a-zA-Z0-9]+', '.*?', search_term)
        for x in assets[search_term_pre]:
            check_if_present = re.findall(f"type.*?asset\W+.*?{search_term}.*[\"']", str(x))
            if check_if_present:
                asset_list.append(check_if_present)
    else:
        search_term = re.sub(r'[^a-zA-Z0-9]+', '.*?', search_term)
        for i in assets:
            for x in assets[i]:
                check_if_present = re.findall(f"type.*?asset\W+.*?{search_term}.*[\"']", str(x))
                if check_if_present:
                    asset_list.append(check_if_present)
    return asset_list


def get_location(assetsList, snap_date):
    location_link = f"http://gbl20174697:12000/v4/assets/{snap_date}"
    for i in assetsList:
        ### INCLUDE GDA'S FROM HERE
        if "mda" in i[0]:
            source = i[0].split("source': '")[-1].split("'")[0]
            asset = i[0].split("asset': '")[-1].split("'")[0]
            post_data = '{"mdas": [{"source": "%s","asset": "%s","includePartitions": false}]}' % (source, asset)
            parq_location = s.post(location_link, data=post_data).json()

            parq_location = str(parq_location).split("path': '")[-1].split("'")[0]
    return parq_location



