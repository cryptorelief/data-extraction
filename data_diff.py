import os
import requests
import json
from hashlib import sha256
from data_extractor import modify
import pandas as pd
import io
from copy import deepcopy


def csv_data2df(csv_data):
    csv_data_file_obj = io.StringIO(csv_data)
    df = pd.read_csv(csv_data_file_obj)
    return df


def df2list(df):
    data = []
    columns = list(df.keys().values)
    for i in range(len(df)):
        new_dict = {}
        for col in columns:
            new_dict[col] = df[col][i] if not(pd.isna(df[col][i])) else None
        new_dict['row_num'] = i+1
        data.append(new_dict)
        del new_dict
    return data


def get_data(data_source, filenames=None):
    if(data_source=="coronasafe"):
        data = []
        for filename in filenames:
            r = requests.get("https://life-api.coronasafe.network/data/{}.json".format(filename))
            data += r.json()["data"]
    elif(data_source=="nlp_supply"):
        r = requests.get("https://docs.google.com/spreadsheets/d/1z8quqkyMubvIJDY6mRVugZeecgY__dH8I-ZmS0_FL14/export?format=csv&id=1z8quqkyMubvIJDY6mRVugZeecgY__dH8I-ZmS0_FL14&gid=0")
        csv_data = r.content.decode('utf-8')
        df = csv_data2df(csv_data)
        data = df2list(df)
    elif(data_source=="nlp_demand"):
        r = requests.get("https://docs.google.com/spreadsheets/d/1TwD2-0vjloYurPHa86-PDd8rQSsVrRn7YtB90FXDgJw/export?format=csv&id=1TwD2-0vjloYurPHa86-PDd8rQSsVrRn7YtB90FXDgJw&gid=0")
        csv_data = r.content.decode('utf-8')
        df = csv_data2df(csv_data)
        data = df2list(df)
    return data


def hash_data(data):
    hashed = []
    for d in data:
        hashed.append(sha256(str(d).encode('utf-8')).hexdigest())
    return hashed


def save_to_file(data, data_source):
    extracted_file_obj = open("{}_extracted.json".format(data_source),"w")
    json_data = {"data":data}
    json.dump(json_data,extracted_file_obj,indent=4,default=str)
    extracted_file_obj.close()


def get_extracted_data(data_source):
    extracted_file_obj = open("{}_extracted.json".format(data_source),"r")
    extracted_data = json.load(extracted_file_obj)["data"]
    extracted_file_obj.close()
    return extracted_data


def get_diff(data_source, filenames=None):
    new_data = []
    data = get_data(data_source, filenames)
    data = modify(data, data_source)
    data_copy = deepcopy(data)
    # convert timestamp objects to string so their hash is comparable with json input hash
    for entry in data_copy:
        if(entry.get("last_verified_on", "")):
            entry['last_verified_on'] = str(entry['last_verified_on'])
        if(entry.get("created_on", "")):
            entry['created_on'] = str(entry['created_on'])
    # read archived data, Get all the entries that are already present in the database
    extracted_data = get_extracted_data(data_source)
    # hash archived data
    hashed_data = hash_data(extracted_data)
    # compare hashes to get new data
    for d, orig in zip(data_copy, data):
        d_hashed = sha256(str(d).encode('utf-8')).hexdigest()
        if(d_hashed not in hashed_data):
            new_data.append(orig)
    # add new data to archived data
    extracted_data += new_data
    # write new archived data
    save_to_file(extracted_data, data_source)
    return new_data
