from time import sleep
from data2db.py import extract_transform_load

while condition == True:
    sleep(60*15)  # Wait for 15 minutes
    extract_transform_load()

    
