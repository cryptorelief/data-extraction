import requests
from data_diff import get_diff
from data_extractor import modify
import json
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from credentials import USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME
import datetime

apis = ["oxygen_v2","ambulance_v2","medicine_v2","hospital_v2"]

pengine = sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(USERNAME,PASSWORD,HOSTNAME,PORT,DB_NAME))
Base = declarative_base()
metadata = sqlalchemy.MetaData(pengine)
metadata.reflect()

# All the tables in the database (probably would be renamed in the future)

class Supply(Base):
    __table__ = sqlalchemy.Table("Supply", metadata)

# TODO: Add Error handling table

# session object to talk with the db
Session = sqlalchemy.orm.sessionmaker(pengine)
session = Session()

def data2db(data_dict):
    # check if a Supply with this external_id already exists
    existing_entry = session.query(Supply).filter_by(external_uuid=data_dict['external_uuid']).first()
    if existing_entry: # Check if external_id exists
        last_verified = existing_entry.last_verified_on or datetime.datetime(1960, 1, 1)
        if data_dict.get('last_verified_on', datetime.datetime(1950, 1, 1)) > last_verified:
            # update database with new verification date
            session.query(Supply).update({Supply.last_verified_on: data_dict['last_verified_on']})
            session.commit()
    else:
        try:
            d = Supply(**data_dict)
            session.add(d)
            session.commit()
        except Exception as e:
            print(str(e))

def extract_transform_load():
    for api in apis:
        new_data = get_diff(api)
        print("Adding %d records to database" % len(new_data))
        for d in new_data:
            data2db(d)
