# A Python script to get only the relevant data from the main CoronaSafe Database

import pandas as pd
import requests
import json

def get_data(fname):
    r = requests.get("https://life-api.coronasafe.network/data/{}.json".format(fname))
    data = r.json()["data"]
    return data

def modify(fname):
    data = get_data(fname)
    new_data = []
    for d in data:
        new_dict = {}
        for k in d:
            if(d[k]):
                if(k in {"created_on","last_verified_on"}):
                    try:
                        new_dict[k] = pd.to_datetime(d[k])
                    except:
                        pass
                elif(k in {"resource_type","resource type","resource type_2"}):
                    if("resource_type" not in new_dict):
                        new_dict["resource_type"] = ""
                    new_dict["resource_type"]+="{}".format(d[k])
                elif(k in {"Details","description","Other comments","comment","Sales/Renta/Charity"}):
                    if("comments" not in new_dict):
                        new_dict["comments"] = ""
                    if(k == "Sales/Renta/Charity"):
                        d[k] = "Sales or Rental or Charity? - " + d[k]
                    new_dict["comments"]+="{}\n".format(d[k])
                elif(k=="phone_1"):
                    new_dict["phone"] = d[k]
                elif(k=="phone_2"):
                    new_dict["phone2"] = d[k]
                elif(k=="quantity_available"):
                    new_dict[k] = True if d[k] not in ["Unavailable","Undetermined","no beds currently avilable","NIL"] else False
                elif(k=="verification_status"):
                    new_dict[k] = True if (d[k].lower().startswith("verified") or "and verified" in d[k].lower() or "but verified" in d[k].lower()) else False
                elif(k in {"pin_code","pincode"}):
                    new_dict["pincode"] = d[k]
                elif(k in {"hospital_available_normal_beds","hospital_available_ventilator_beds","hospital_available_icu_beds","district","city","title","email","state","pin_code","pincode","address","price","category","source_link","hospital_available_oxygen_beds","latitude","longitude"}):
                    new_dict[k] = d[k]
        new_data.append(new_dict)
        del new_dict
    new_db = {"data":new_data}
    new_db_json = json.dumps(new_db,indent=4,default=str)
    return new_db_json

if __name__=="__main__":
    new_oxygen_v2 = modify("oxygen_v2")
    new_ambulance_v2 = modify("ambulance_v2")
    new_hospital_v2 = modify("hospital_v2")
    new_medicine_v2 = modify("medicine_v2")
