import pandas as pd
import requests
import json
from datetime import datetime
from copy import deepcopy


def modify(data, data_source):
    # `data` format = [{}]
    new_data = []
    transform = {
        'phone_1': "phone",
        'phone_2': "phone2",
        'external_id': "external_uuid",
        'data_name': "source2",
        'pin_code': "pincode",
    }
    preserve = [
        "district",
        "city",
        "title",
        "email",
        "state",
        "pincode",
        "address",
        "price",
        "category",
        "source_link"
    ]
    if(data_source=="coronasafe"):
        for record in data:
            new_dict = {'source1': "coronasafe.network"}
            for (key, value) in record.items():
                if isinstance(value, str):
                    value = value.strip().strip("-").strip(",")
                if value:
                    if(key in preserve):
                        new_dict[key] = value
                    elif(key in transform):
                        new_dict[transform[key]] = value
                    elif(key in {"created_on","last_verified_on"}):
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
                        new_dict["comments"] = "\n".join(filter(None, [new_dict['comments'], value]))
                    elif(key=="quantity_available"):
                        new_dict['available'] = (value not in ["Unavailable","Undetermined","no beds currently avilable","NIL",""])
                        new_dict['quantity_text'] = value
                    elif(key=="verification_status"):
                        new_dict["verified"] = value.lower().startswith("verified") or ("and verified" in value.lower()) or ("but verified" in value.lower())
            # outside of the loop, we check which of the 4 hospital w/ bed types have values and build a list of (key,value) pairs
            hospital_beds = [(key, record[key]) for key in ["hospital_available_normal_beds",
                                                            "hospital_available_ventilator_beds",
                                                            "hospital_available_icu_beds",
                                                            "hospital_available_oxygen_beds"]
                             if record.get(key, "").strip()]
            for (i, (key, value)) in enumerate(hospital_beds):
                # crude way to check if best are available (does not start with "not" and "no")
                available = not value.lower().startswith("no")
                resource_type = key.replace("available", "with").replace("_", " ")
                # store comments so far (so we can add to them in the loop)
                orig_comments = new_dict.get('comments', "")
                # generate new comments from resource type text
                comments = ""
                if value.isdigit():
                    new_dict['quantity'] = int(value)
                if value.lower().strip() not in ["available", "yes", "no", ""]:
                    comments = "{}: {}".format(resource_type, value)
                # if we're on first out of 4 possible hospital w/ bed types, use the dict we were creating so far
                if i==0:
                    new_dict['available'] = available
                    new_dict['comments'] = "\n".join(filter(None, [orig_comments, comments]))
                    new_dict['resource_type'] = resource_type
                # otherwise create a new dict from existing data and add it to `new_data`
                else:
                    if not available:
                        continue
                    extra_dict = deepcopy(new_dict)
                    extra_dict['available'] = available
                    extra_dict['comments'] = "\n".join(filter(None, [orig_comments, comments]))
                    extra_dict['resource_type'] = resource_type
                    new_data.append(extra_dict)
            new_data.append(new_dict)
            del new_dict
    elif(data_source=="nlp_supply"):
        for record in data:
            new_dict = {'source1': "nlp_supply"}
            for (key, value) in record.items():
                if isinstance(value, str):
                    value = value.strip().strip("-").strip(",")
                if value or value==0:
                    if(key in {"Resource Type","Resource Category"}):
                        if("resource_type" not in new_dict):
                            new_dict["resource_type"] = ""
                        if(value):
                            new_dict["resource_type"] += value.replace("Hopital","hospital")
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
