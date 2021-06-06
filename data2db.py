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

def data2db(table, new_data, data_source):
    if(data_source=="coronasafe"):
        has_contact_obj = True
    elif(data_source in {"nlp_supply","nlp_demand"}):
        has_contact_obj = True
    with get_session() as session:
        table_objs_updates = []
        table_objs_inserts = []
        contact_objs_updates = []
        contact_objs_inserts = []
        for (i,d) in enumerate(new_data):
            contact_fields = ["source","tg_user_id","tg_user_handle"]
            if(any(item in contact_fields for item in list(d.keys()))):
                contact_dict = {}
                for entry in contact_fields:
                    if(entry in list(d.keys())):
                        if(entry=="tg_user_handle"):
                            contact_dict["user_handle"] = d.get(entry,"")
                        else:
                            contact_dict[entry] = d.get(entry)
                        if(entry!="source"):
                            d.pop(entry)
            if(data_source=="coronasafe"):
                existing_entry = session.query(table).filter_by(external_uuid=d['external_uuid']).first()
            elif(data_source in {"nlp_supply","nlp_demand"}):
                existing_entry = session.query(table).filter_by(row_num=d['row_num']).first()
            if(existing_entry):
                last_verified = existing_entry.last_verified_on or datetime.datetime(1960, 1, 1)
                try:
                    if(d.get('last_verified_on', datetime.datetime(1950, 1, 1)) > last_verified):
                        if(has_contact_obj):
                            contact_dict['id'] = existing_entry.contact_id
                            contact_objs_updates.append(contact_dict)
                        d['id'] = existing_entry.id
                        table_objs_updates.append(d)
                except:
                    pass
            else:
                if(has_contact_obj):
                    contact_objs_inserts.append(contact_dict)
                table_objs_inserts.append(d)

    with get_session() as session:
        try:
            if(has_contact_obj):
                session.bulk_update_mappings(Contact,contact_objs_updates)
            session.bulk_update_mappings(table,table_objs_updates)
            session.commit()
        except Exception as e:
            print("Error while saving: " + str(e))


    with get_session() as session:
        try:
            if(has_contact_obj):
                session.bulk_insert_mappings(Contact,contact_objs_inserts,return_defaults=True)
                for (i,d) in enumerate(table_objs_inserts):
                    d['contact_id'] = contact_objs_inserts[i]['id']
            session.bulk_insert_mappings(table,table_objs_inserts)
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
