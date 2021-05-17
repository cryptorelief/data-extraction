import os
import requests
import json
from hashlib import sha256
from data_extractor import modify

def get_data(fname):
    r = requests.get("https://life-api.coronasafe.network/data/{}.json".format(fname))
    data = r.json()["data"]
    return data

def hash_data(data):
    hashed = []
    for d in data:
        hashed.append(sha256(str(d).encode('utf-8')).hexdigest())
    return hashed

def get_diff(fname):
    new_data = []
    data = get_data(fname)
    data = modify(data)
    # convert timestamp objects to string so they are comparable with json input
    for entry in data:
        if(entry.get("last_verified_on", "")):
            entry['last_verified_on'] = str(entry['last_verified_on'])
        if(entry.get("created_on", "")):
            entry['created_on'] = str(entry['created_on'])
    extracted_data_file_obj = open("extracted_data.json", "r") # Get all the entries that are already present in the database
    extracted_data = json.load(extracted_data_file_obj)["data"]
    extracted_data_file_obj.close()
    hashed_data = hash_data(extracted_data)
    for d in data:
        d_hashed = sha256(str(d).encode('utf-8')).hexdigest()
        if(d_hashed not in hashed_data):
            new_data.append(d)
    return new_data
