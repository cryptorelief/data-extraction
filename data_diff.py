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
    # read archived data
    extracted_data_file_obj = open("extracted_data.json", "r") # Get all the entries that are already present in the database
    extracted_data = json.load(extracted_data_file_obj)["data"]
    extracted_data_file_obj.close()
    # hash archived data
    hashed_data = hash_data(extracted_data)
    # compare hashes to get new data
    for d in data:
        d_hashed = sha256(str(d).encode('utf-8')).hexdigest()
        if(d_hashed not in hashed_data):
            new_data.append(d)
    # add new data to archived data
    extracted_data+=new_data
    # write new archived data
    extracted_data_json = {"data":extracted_data}
    extracted_data_file = open("extracted_data.json","w")
    json.dump(extracted_data_json, extracted_data_file,indent=4,default=str)
    extracted_data_file.close()
    return new_data
