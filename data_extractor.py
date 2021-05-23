import pandas as pd
import requests
import json
from datetime import datetime

def modify(data,data_source):
    # `data` format = [{}]
    new_data = []
    if(data_source=="coronasafe"):
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
                        new_dict["verified"] = d[k].lower().startswith("verified") or ("and verified" in d[k].lower()) or ("but verified" in d[k].lower())
                    elif(k in {"pin_code","pincode"}):
                        new_dict["pincode"] = d[k]
                    elif(k in {"hospital_available_normal_beds","hospital_available_ventilator_beds","hospital_available_icu_beds","district","city","title","email","state","pin_code","pincode","address","price","category","source_link","hospital_available_oxygen_beds"}):
                        new_dict[k] = d[k]
            new_data.append(new_dict)
            del new_dict
    elif(data_source=="nlp_supply"):
        for d in data:
            new_dict = {'source1': "nlp_supply"}
            for k in d:
                if(k in {"Resource Type","Resource Category"}):
                    if("resource_type" not in new_dict):
                        new_dict["resource_type"] = ""
                    if(d[k]):
                        new_dict["resource_type"] += d[k].replace("Hopital","Hospital")
                elif(k=="Contact Number"):
                    new_dict["phone"] = d[k]
                elif(k=="City"):
                    new_dict["city"] = d[k]
                elif(k=="Timestamp"): # TODO: CHECK THE TIMEZONE AND CONVERT TO IST
                    new_dict["created_on"] = pd.to_datetime(d[k])
                elif(k=="Source"):
                    new_dict["source2"] = d[k]
                elif(k in {"row_num"}):
                    new_dict[k] = d[k]
            new_data.append(new_dict)
            del new_dict
    return new_data
