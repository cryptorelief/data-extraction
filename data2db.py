import requests
from data_diff import get_diff
from data_extractor import modify
import json
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from credentials import USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME
import datetime
import pandas
from db import Supply, Demand, Contact, get_session

def get_existing_entry(table, data_dict, data_source):
    with get_session() as session:
        if(data_source=="coronasafe"):
            existing_entry = session.query(table).filter_by(external_uuid=data_dict['external_uuid']).first()
        elif(data_source in {"nlp_supply","nlp_demand"}):
            existing_entry = session.query(table).filter_by(row_num=data_dict['row_num']).first()
        return existing_entry

def data2db(table, new_data, data_source):
    contacts_objs = []
    table_objs = []
    for d in new_data:
        contact_fields = ["source","tg_user_id","tg_user_handle"]
        if(any(item in contact_fields for item in list(d.keys()))):
            contact_dict = {}
            for entry in contact_fields:
                if(entry=="tg_user_handle"):
                    contact_dict[entry] = d.get(entry,"")
                else:
                    contact_dict[entry] = d.get(entry)
                d.pop(entry)
            if(contact_dict):
                contacts_bool.append(True)
                contact = Contact(**contact_dict)
                contacts_objs.append(contact)
                d.update(contact=contact)
                existing_entry = get_existing_entry(table, d, data_source)
                if(existing_entry):
                    try:
                        if(d.get('last_verified_on', datetime.datetime(1950, 1, 1)) > last_verified):
                            table_objs.append(table(**d))
                    except:
                        pass
                else:
                    table_objs.append(table(**d))
                    
    with get_session() as session:
        try:
            session.bulk_save_objects(contacts_objs)
            session.commit()
            session.bulk_save_objects(table_objs)
            session.commit()
        except Exception as e:
            print("Error while saving: " + str(e))


def extract_transform_load(data_source):
    if(data_source=="coronasafe"):
        new_data = get_diff(data_source,["oxygen_v2","ambulance_v2","medicine_v2","hospital_v2"])
        print("Adding %d CORONASAFE records to DB" % len(new_data))
        data2db(Supply,new_data,data_source)
    elif(data_source=="nlp_supply"):
        new_data = get_diff(data_source)
        print("Adding %d NLP SUPPLY records to DB" % len(new_data))
        data2db(Supply,new_data,data_source)
    elif(data_source=="nlp_demand"):
        new_data = get_diff(data_source)
        print("Adding %d NLP DEMAND records to DB" % len(new_data))
        data2db(Demand,new_data,data_source)
