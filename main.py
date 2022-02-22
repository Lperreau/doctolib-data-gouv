import os
import string
import psycopg
import json

pg_conn_string = "dbname=doctolib_data user=louis"

with psycopg.connect(pg_conn_string) as db_conn:
    print('connected to db')
# Open a cursor to perform database operations
    with db_conn.cursor() as cur:
         cur.execute("""
            DROP TABLE doctolib_centers_metadata;
         """)
         print('table dropped')
        
        # Execute a command: this creates a new table
    with db_conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE doctolib_centers_metadata (
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
    print('table metadata created')

    with db_conn.cursor() as cur:
         cur.execute("""
            DROP TABLE doctolib_centers_profile;
         """)
         print('table dropped')
        
        # Execute a command: this creates a new table
    with db_conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE doctolib_centers_profile (
                id INTEGER,
                name_with_title_and_determiner VARCHAR(128),
                name_with_title VARCHAR(128),
                speciality TEXT,
                organization VARCHAR(64),
                redirect_url VARCHAR(256),
                language_list VARCHAR(128)
                )
            """)
    print('table profile created')

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
    with psycopg.connect(pg_conn_string) as db_conn:
        with db_conn.cursor() as cur:
            cur.execute("""
            INSERT INTO doctolib_centers_metadata (id, is_directory, address, city, zipcode, link , cloudinary_public_id, profile_id , exact_match, priority_speciality, first_name, last_name, name_with_title , speciality, organization_status, place_id, telehealth)
            VALUES (%(id)s, %(is_directory)s, %(address)s, %(city)s, %(zipcode)s, %(link)s,  %(cloudinary_public_id)s, %(profile_id)s,  %(exact_match)s, %(priority_speciality)s, %(first_name)s, %(last_name)s, %(name_with_title)s,  %(speciality)s, %(organization_status)s, %(place_id)s, %(telehealth)s);
            """,
            metadata)

def insert_profile_in_db(profile):
    with psycopg.connect(pg_conn_string) as db_conn:
        with db_conn.cursor() as cur:
            # we serialize `speciality` key, because it is a dict,
            # and postgres will now expect a json string
            profile['speciality'] = json.dumps(profile['speciality'])
            cur.execute("""
            INSERT INTO doctolib_centers_profile( id, name_with_title_and_determiner, name_with_title, speciality, organization, redirect_url, language_list)
            VALUES ( %(id)s, %(name_with_title_and_determiner)s, %(name_with_title)s, %(speciality)s, %(organization)s, %(redirect_url)s, %(language_list)s);
            """,
            profile)

for data_key, centers in data.items():
    for center in centers:
        metadata = center['metadata']
        profile = center['data']['metadata']['profile']
        insert_metadata_in_db(metadata)
        print(profile)
        insert_profile_in_db(profile)
        
        