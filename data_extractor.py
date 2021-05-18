import pandas as pd
import requests
import json
from datetime import datetime

def modify(data):
    # `data` format = [{}]
    new_data = []
    for d in data:
        new_dict = {'source1': "coronasafe.network"}
        for k in d:
            if(d[k]) and d[k]!='-':
                if(k in {"created_on","last_verified_on"}):
                    try:
                        dt = pd.to_datetime(d[k].replace("Z", ""))
                        if dt > datetime.now():
                            dt = pd.to_datetime(d[k].replace("Z", ""), dayfirst=True)
                        new_dict[k] = dt
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
                elif(k=="external_id"):
                    new_dict["external_uuid"] = d[k]
                elif(k=="quantity_available"):
                    new_dict[k] = True if d[k] not in ["Unavailable","Undetermined","no beds currently avilable","NIL"] else False
                elif(k=="verification_status"):
                    new_dict[k] = True if (d[k].lower().startswith("verified") or "and verified" in d[k].lower() or "but verified" in d[k].lower()) else False
                elif(k in {"pin_code","pincode"}):
                    new_dict["pincode"] = d[k]
                elif(k in {"hospital_available_normal_beds","hospital_available_ventilator_beds","hospital_available_icu_beds","district","city","title","email","state","pin_code","pincode","address","price","category","source_link","hospital_available_oxygen_beds"}):
                    new_dict[k] = d[k]
        new_data.append(new_dict)
        del new_dict
    return new_data
