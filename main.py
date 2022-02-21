import os
import psycopg
import json

def read_json_file(file_path):
    file = open(file_path)
    data = json.load(file)
    print(data)

data_folder = './raw data'
files = os.listdir(data_folder)

for file in files:
    if file.endswith('.json'):
        read_json_file("{}/{}".format(data_folder, file))

# Connect to an existing database
with psycopg.connect("dbname=doctolib_data user=louis") as conn:
    print('connecxted to DB')