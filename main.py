import os
import psycopg
import json

def read_json_file(file_path):
    file = open(file_path)
    data = json.load(file)
    return data[0]['metadata']

data_folder = './raw data'
files = os.listdir(data_folder)
data = {}

for file in files:
    if file.endswith('.json'):
        json_file_path = "{}/{}".format(data_folder, file)
        json_file = read_json_file(json_file_path)
        data[json_file_path] = json_file

print(data)

# Connect to an existing database
with psycopg.connect("dbname=doctolib_data user=louis") as conn:
    print('connecxted to DB')