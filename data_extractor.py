import pandas as pd
import requests
import json

def modify(data):
    new_data = []
    for d in data:
        new_dict = {'source1': "coronasafe.network"}
        for k in d:
            if(d[k]) and d[k]!='-':
                if(k in {"created_on","last_verified_on"}):
                    try:
                        new_dict[k] = pd.to_datetime(d[k].replace("Z", ""))
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
    extracted_data_file = open("extracted_data.json","r")
    extracted_data = json.load(extracted_data_file)["data"]
    extracted_data_file.close()
    extracted_data+=new_data
    extracted_data_json = {"data":extracted_data}
    extracted_data_file = open("extracted_data.json","w")
    json.dump(extracted_data_json, extracted_data_file,indent=4,default=str)
    extracted_data_file.close()
    new_db = {"data":new_data}
    new_db_json = json.dumps(new_db,indent=4,default=str)
    return new_data
