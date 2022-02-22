import os
import string
import psycopg
import json

with psycopg.connect("dbname=doctolib_data user=louis") as db_conn:
    print('connected to db')
# Open a cursor to perform database operations
    with db_conn.cursor() as cur:
         cur.execute("""
            DROP TABLE doctolib_centers;
         """)
         print('table dropped')
        
        # Execute a command: this creates a new table
    with db_conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE doctolib_centers (
                id serial PRIMARY KEY,
                is_directory boolean,
                address text,
                city varchar(64),
                zipcode varchar(8),
                link varchar(256),
                cloudinary_public_id varchar(36),
                profile_id varchar(64),
                exact_match boolean,
                priority_speciality boolean,
                first_name varchar(128),
                last_name varchar(128),
                name_with_title varchar(128),
                speciality varchar(64),
                organization_status varchar(64),
                place_id varchar(64),
                telehealth boolean
                )
            """)
    print('table created')

def read_json_file(file_path):
    file = open(file_path)
    data = json.load(file)
    return data

data_folder = './raw data'
files = os.listdir(data_folder)
data = {}

for file in files:
    if file.endswith('.json'):
        json_file_path = "{}/{}".format(data_folder, file)
        json_file = read_json_file(json_file_path)
        data[json_file_path] = json_file

def insert_metadata_in_db(metadata):
    with psycopg.connect("dbname=doctolib_data user=louis") as db_conn:
        with db_conn.cursor() as cur:
            cur.execute("""
            INSERT INTO doctolib_centers (id, is_directory, address, city, zipcode, link , cloudinary_public_id, profile_id , exact_match, priority_speciality, first_name, last_name, name_with_title , speciality, organization_status, place_id, telehealth)
            VALUES (%(id)s, %(is_directory)s, %(address)s, %(city)s, %(zipcode)s, %(link)s,  %(cloudinary_public_id)s, %(profile_id)s,  %(exact_match)s, %(priority_speciality)s, %(first_name)s, %(last_name)s, %(name_with_title)s,  %(speciality)s, %(organization_status)s, %(place_id)s, %(telehealth)s);
            """,
            metadata)

for data_key, centers in data.items():
    for center in centers:
        metadata = center['metadata']
        print(metadata)
        insert_metadata_in_db(metadata)    