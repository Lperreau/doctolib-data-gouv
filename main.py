import os
import psycopg
import json
import csv

SCRIPT_NAME = 'doctolib-data-gouv'
PG_CONN_STRING = "dbname=doctolib_data user=louis"
DATA_FOLDER = './raw_data'

# Tables creation
def init_db_tables(pg_conn_string):
    with psycopg.connect(pg_conn_string) as db_conn:
        print('connected to db')
        with db_conn.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS src_stocks, allocation_vs_appointment, doctolib_centers_metadata, doctolib_centers_profile;
                """)
            cur.execute("""
                CREATE TABLE src_stocks (
                    code_departement VARCHAR(8),
                    departement VARCHAR(64),
                    raison_sociale VARCHAR(128),
                    libelle_pui VARCHAR(128),
                    finess VARCHAR(16),
                    type_de_vaccin VARCHAR(16),
                    nb_ucd INTEGER,
                    nb_doses INTEGER,
                    date DATE)
                """)
            cur.execute('''
                CREATE TABLE allocation_vs_appointment (
                code_region varchar(8),
                region varchar(64),
                departement varchar(64),
                id_centre varchar(64),
                nom_centre text,
                rang_vaccinal integer,
                date_debut_semaine date,
                nb integer,
                nb_rdv_cnam integer,
                nb_rdv_rappel integer
            )
            ''')
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

# Get data from raw data files - Json and CSV - and return python Dict
def get_data(data_folder_path):
    files = os.listdir(data_folder_path)
    data = {}
    for file in files:
        file_path = "{}/{}".format(data_folder_path, file)
        if file.endswith('.json'):
            json_file = read_json_file(file_path)
            data[file_path] = json_file
        if file.endswith('.csv'):
            csv_file = read_csv_file(file_path)
            data[file_path] = csv_file
    return data

def read_json_file(file_path):
    file = open(file_path)
    data = json.load(file)
    return data

def read_csv_file(file_path, encoding = 'windows-1252'):
    file_data = []
    with open(file_path, newline='', encoding=encoding) as csv_file:
        try:
            dialect = csv.Sniffer().sniff(csv_file.read(1024))
            csv_file.seek(0)
            reader = csv.DictReader(csv_file, dialect=dialect)
            for row in reader:
                file_data.append(row)
        except UnicodeDecodeError as e:
            print('unicode error', file_path, e)
        except Exception as e:
            print('error reading CSV', file_path, str(e))
            if 'Could not determine delimiter' == str(e):
                try: 
                    dialect = csv.Sniffer().sniff(csv_file.read(1024))
                    csv_file.seek(0)
                    reader = csv.DictReader(csv_file, dialect=dialect, delimiter=';')
                    for row in reader:
                        file_data.append(row)
                except:
                    print('csv delimiter ; did not work')
        except:
            print('csv error')
    return file_data

def insert_metadata_in_db(pg_conn_string, metadata):
    with psycopg.connect(pg_conn_string) as db_conn:
        with db_conn.cursor() as cur:
            cur.execute("""
            INSERT INTO doctolib_centers_metadata (id, is_directory, address, city, zipcode, link , cloudinary_public_id, profile_id , exact_match, priority_speciality, first_name, last_name, name_with_title , speciality, organization_status, place_id, telehealth)
            VALUES (%(id)s, %(is_directory)s, %(address)s, %(city)s, %(zipcode)s, %(link)s,  %(cloudinary_public_id)s, %(profile_id)s,  %(exact_match)s, %(priority_speciality)s, %(first_name)s, %(last_name)s, %(name_with_title)s,  %(speciality)s, %(organization_status)s, %(place_id)s, %(telehealth)s);
            """,
            metadata)

def insert_profile_in_db(pg_conn_string, profile):
    with psycopg.connect(pg_conn_string) as db_conn:
        with db_conn.cursor() as cur:
            # we serialize `speciality` key, because it is a dict,
            # and postgres will now expect a json string
            profile['speciality'] = json.dumps(profile['speciality'])
            cur.execute("""
            INSERT INTO doctolib_centers_profile(id, name_with_title_and_determiner, name_with_title, speciality, organization, redirect_url, language_list)
            VALUES (%(id)s, %(name_with_title_and_determiner)s, %(name_with_title)s, %(speciality)s, %(organization)s, %(redirect_url)s, %(language_list)s);
            """,
            profile)

def insert_allocation_vs_appointment_in_db(pg_conn_string, data):
    with psycopg.connect(pg_conn_string) as db_conn:
        with db_conn.cursor() as cur:
            for item in data:
                cur.execute('''
                INSERT INTO allocation_vs_appointment(code_region,region,departement,id_centre,nom_centre,rang_vaccinal,date_debut_semaine,nb,nb_rdv_cnam,nb_rdv_rappel) 
                VALUES (%(code_region)s,%(region)s,%(departement)s,%(id_centre)s,%(nom_centre)s,%(rang_vaccinal)s,%(date_debut_semaine)s,%(nb)s,%(nb_rdv_cnam)s, %(nb_rdv_rappel)s)
                ''', item)
            print('table allocation vs appointment created')

def insert_data_in_db(pg_conn, data):
    for center in data:
        metadata = center['metadata']
        profile = center['data']['metadata']['profile']
        insert_metadata_in_db(pg_conn, metadata)
        insert_profile_in_db(pg_conn, profile)

def main(name, pg_conn, data_folder):
    print(name, "is running")
    init_db_tables(pg_conn)
    print('all tables created') 
    print('starting inserting data in postgresql')
    data = get_data(data_folder)
    for key in data.keys():
        if key.endswith('.json'):
            json_data = data[key]
            insert_data_in_db(pg_conn, json_data)
        if key == './raw_data/vaccinations_vs_appointments.csv':
            local_data = data['./raw_data/vaccinations_vs_appointments.csv']
            # insert_allocation_vs_appointment_in_db(pg_conn, local_data)
        if key == './raw_data/allocations-vs-rdv.csv':
            local_data = data['./raw_data/allocations-vs-rdv.csv']
            # print(local_data, "allocation-vs-rdv")
        if key == './raw_data/stocks.csv':
            local_data = data['./raw_data/stocks.csv']
            # print(local_data, 'stock data')
        if key == './raw_data/vaccination_centers.csv':
            local_data = data['./raw_data/vaccination_centers.csv']
            # print(local_data, 'vaccination_centers')
        print('finished inserting data in postgresql')

main(SCRIPT_NAME, PG_CONN_STRING, DATA_FOLDER)
