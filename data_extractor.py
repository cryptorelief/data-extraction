import pandas as pd
import requests
import json
from datetime import datetime

def modify(data, data_source):
    # `data` format = [{}]
    new_data = []
    if(data_source=="coronasafe"):
        for record in data:
            new_dict = {'source1': "coronasafe.network"}
            for (key, value) in record.items():
                if(value) and value!='-':
                    if(key in {"created_on","last_verified_on"}):
                        try:
                            dt = pd.to_datetime(value.replace("Z", ""))
                            if dt > datetime.now():
                                dt = pd.to_datetime(value.replace("Z", ""), dayfirst=True)
                            new_dict[key] = dt
                        except:
                            pass
                    elif(key in {"resource_type","resource type","resource type_2"}):
                        if("resource_type" not in new_dict):
                            new_dict["resource_type"] = ""
                        new_dict["resource_type"] += "{}".format(value)
                    elif(key in {"Details","description","Other comments","comment","Sales/Renta/Charity"}):
                        if("comments" not in new_dict):
                            new_dict["comments"] = ""
                        if(key == "Sales/Renta/Charity"):
                            value = "Sales or Rental or Charity? - " + value
                        new_dict["comments"] += "{}\n".format(value)
                    elif(key=="phone_1"):
                        new_dict["phone"] = value
                    elif(key=="phone_2"):
                        new_dict["phone2"] = value
                    elif(key=="external_id"):
                        new_dict["external_uuid"] = value
                    elif(key=="quantity_available"):
                        new_dict[key] = (value not in ["Unavailable","Undetermined","no beds currently avilable","NIL"])
                    elif(key=="verification_status"):
                        new_dict["verified"] = value.lower().startswith("verified") or ("and verified" in value.lower()) or ("but verified" in value.lower())
                    elif(key in {"pin_code","pincode"}):
                        new_dict["pincode"] = value
                    elif(key in {"hospital_available_normal_beds","hospital_available_ventilator_beds","hospital_available_icu_beds","district","city","title","email","state","pin_code","pincode","address","price","category","source_link","hospital_available_oxygen_beds"}):
                        new_dict[key] = value
            new_data.append(new_dict)
            del new_dict
    elif(data_source=="nlp_supply"):
        for record in data:
            new_dict = {'source1': "nlp_supply"}
            for (key, value) in record.items():
                if(key in {"Resource Type","Resource Category"}):
                    if("resource_type" not in new_dict):
                        new_dict["resource_type"] = ""
                    if(value):
                        new_dict["resource_type"] += value.replace("Hopital","Hospital")
                elif(key in {"row_num"}):
                    new_dict[key] = value
                elif(key=="Contact Number"):
                    new_dict["phone"] = value
                elif(key=="City"):
                    new_dict["city"] = value
                elif(key=="Timestamp"): # TODO: CHECK THE TIMEZONE AND CONVERT TO IST
                    new_dict["created_on"] = pd.to_datetime(value)
                elif(key=="Source"):
                    new_dict["source2"] = value
            new_data.append(new_dict)
            del new_dict
    return new_data
