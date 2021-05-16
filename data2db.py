import requests
from data_diff import get_diff,base_url
from data_extractor import modify
import json

apis = ["oxygen_v2","ambulance_v2","medicine_v2","hospital_v2"]

def data2db(data_dict):
    r = requests.get("{}supply?external_id={}".format(base_url,data_dict["external_id"]))
    if(r.status_code==200):
        # TODO: DELETE THE CURRENT DATA
        pass
    p = requests.post("{}supply".format(base_url),data=json.dumps(data_dict))

for api in apis:
    new_data = get_diff(api)
    modified_data = modify(new_data)
    for d in modified_data:
        data2db(d)
