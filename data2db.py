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

def data2db(data_dict,table,data_source):
    # check if a table with this external_id already exists
    if(data_source=="coronasafe"):
        existing_entry = session.query(table).filter_by(external_uuid=data_dict['external_uuid']).first()
    elif(data_source=="nlp_supply"):
        existing_entry = session.query(table).filter_by(row_num=data_dict['row_num']).first()
    if existing_entry: # Check if external_id exists
        last_verified = str(existing_entry.last_verified_on or datetime.datetime(1960, 1, 1))
        if data_dict.get('last_verified_on', str(datetime.datetime(1950, 1, 1))) > last_verified:
            # update database with new verification date
            session.query(table).update({table.last_verified_on: data_dict['last_verified_on']})
            session.commit()
    else:
        try:
            d = table(**data_dict)
            session.add(d)
            session.commit()
        except Exception as e:
            print(str(e))

def extract_transform_load(data_source):
    if(data_source=="coronasafe"):
        new_data = get_diff(data_source,["oxygen_v2","ambulance_v2","medicine_v2","hospital_v2"])
        print("Adding %d CORONASAFE records to database" % len(new_data))
        for d in new_data:
            data2db(d,Supply,"coronasafe")
    elif(data_source=="nlp_supply"):
        new_data = get_diff(data_source)
        print("Adding %d NLP SUPPLY records to database" % len(new_data))
        for d in new_data:
            data2db(d,Supply,"nlp_supply")
