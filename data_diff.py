import os
from hashlib import sha256

# Base URL of the API
base_url = "https://127.0.0.1:5000/"

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
    r = requests.get("{}request".format(base_url)) # Query our db and get all values
    hashed_data = hash_data(r.json())
    for d in data:
        d_hashed = sha256(str(d).encode('utf-8')).hexdigest()
        if(d_hashed not in hashed_data):
            new_data.append(d)
    return new_data
