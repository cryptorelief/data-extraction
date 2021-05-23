import requests
from data_diff import get_diff
from data_extractor import modify
import json
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from credentials import USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME
import datetime
import pandas

pengine = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME))
Base = declarative_base()
metadata = sqlalchemy.MetaData(pengine)
metadata.reflect()

# All the tables in the database (probably would be renamed in the future)

class Supply(Base):
    __table__ = sqlalchemy.Table("Supply", metadata)

class Demand(Base):
    __table__ = sqlalchemy.Table("Demand", metadata)

# TODO: Add Error handling table

# session object to talk with the db
Session = sqlalchemy.orm.sessionmaker(pengine)
session = Session()

def get_updates_and_inserts(new_data,data_source,table):
    updates = []
    inserts = []
    for d in new_data:
        if(data_source=="coronasafe"):
            existing_entry = session.query(table).filter_by(external_uuid=d['external_uuid']).first()
        elif(data_source=="nlp_supply"):
            existing_entry = session.query(table).filter_by(row_num=d['row_num']).first()
        if(existing_entry):
            last_verified = str(existing_entry.last_verified_on or datetime.datetime(1960, 1, 1))
            if d.get('last_verified_on', str(datetime.datetime(1950, 1, 1))) > last_verified:
                updates.append(d)
        else:
            inserts.append(d)
    return updates,inserts

def data2db(table,new_data,data_source):
    updates,inserts = get_updates_and_inserts(new_data,data_source,table)
    if(len(updates)!=0):
        try:
            session.bulk_update_mappings(table,updates)
            session.commit()
        except Exception as e:
            print("Error in Updating. " + str(e))
    if(len(inserts)!=0):
        try:
            session.bulk_insert_mappings(table,inserts)
            session.commit()
        except Exception as e:
            print("Error in Inserting. " + str(e))


def extract_transform_load(data_source):
    if(data_source=="coronasafe"):
        new_data = get_diff(data_source,["oxygen_v2","ambulance_v2","medicine_v2","hospital_v2"])
        print("Adding %d CORONASAFE records to database" % len(new_data))
        data2db(Supply,new_data,data_source)
    elif(data_source=="nlp_supply"):
        new_data = get_diff(data_source)
        print("Adding %d NLP SUPPLY records to database" % len(new_data))
        data2db(Supply,new_data,data_source)
