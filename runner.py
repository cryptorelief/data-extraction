from time import sleep
from data2db import extract_transform_load

while True:
    extract_transform_load("coronasafe")
    extract_transform_load("nlp_supply")
    extract_transform_load("nlp_demand")
    sleep(60*10)  # Wait for 15 minutes
