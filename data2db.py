import requests
from data_diff import get_diff
from data_extractor import modify
import json
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from credentials import USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME
import datetime
import pandas
from db import Supply, Demand, get_session


def get_updates_and_inserts(new_data,data_source,table):
    updates = []
    inserts = []
    with get_session() as session:
        for d in new_data:
            if(data_source=="coronasafe"):
                existing_entry = session.query(table).filter_by(external_uuid=d['external_uuid']).first()
            elif(data_source=="nlp_supply"):
                existing_entry = session.query(table).filter_by(row_num=d['row_num']).first()
            if(existing_entry):
                # if last_verified_on is newer, update data - otherwise do nothing
                last_verified = existing_entry.last_verified_on or datetime.datetime(1960, 1, 1)
                if d.get('last_verified_on', datetime.datetime(1950, 1, 1)) > last_verified:
                    updates.append(d)
            else:
                inserts.append(d)
    return (updates, inserts)


def data2db(table,new_data,data_source):
    return
    updates, inserts = get_updates_and_inserts(new_data, data_source,table)
    if(updates):
        try:
            with get_session() as session:
                session.bulk_update_mappings(table,updates)
                session.commit()
        except Exception as e:
            print("Error while updating: " + str(e))
    if(inserts):
        try:
            with get_session() as session:
                session.bulk_insert_mappings(table,inserts)
                session.commit()
        except Exception as e:
            print("Error in inserting: " + str(e))


def extract_transform_load(data_source):
    if(data_source=="coronasafe"):
        new_data = get_diff(data_source,["oxygen_v2","ambulance_v2","medicine_v2","hospital_v2"])
        print("Adding %d CORONASAFE records to database" % len(new_data))
        data2db(Supply,new_data,data_source)
    elif(data_source=="nlp_supply"):
        new_data = get_diff(data_source)
        print("Adding %d NLP SUPPLY records to database" % len(new_data))
        data2db(Supply,new_data,data_source)
