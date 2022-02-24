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
                DROP TABLE IF EXISTS stocks, allocation_vs_appointment, vaccination_centers, allocation_vs_rdv, doctolib_centers_metadata, doctolib_centers_profile;
                """)
            cur.execute("""
                CREATE TABLE stocks (
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
            cur.execute("""
                CREATE TABLE vaccination_centers (
                    gid SERIAL PRIMARY KEY,
                    nom VARCHAR(256),
                    arrete_pref_numero VARCHAR(64),
                    xy_precis VARCHAR(64),
                    id_adr VARCHAR(32),
                    adr_num VARCHAR(64),
                    adr_voie VARCHAR(256),
                    com_cp INTEGER,
                    com_insee VARCHAR(32),
                    com_nom VARCHAR(64),
                    lat_coor1 REAL,
                    long_coor1 REAL,
                    structure_siren VARCHAR(16),
                    structure_type VARCHAR(8),
                    structure_rais VARCHAR(256),
                    structure_num VARCHAR(32),
                    structure_voie VARCHAR(32),
                    structure_cp VARCHAR(8),
                    structure_insee VARCHAR(32),
                    structure_com VARCHAR(16),
                    _userid_creation VARCHAR(64),
                    _userid_modification VARCHAR(64),
                    _edit_datemaj VARCHAR(32),
                    lieu_accessibilite TEXT,
                    rdv_lundi VARCHAR(128),
                    rdv_mardi VARCHAR(128),
                    rdv_mercredi VARCHAR(128),
                    rdv_jeudi VARCHAR(128),
                    rdv_vendredi VARCHAR(128),
                    rdv_samedi VARCHAR(128),
                    rdv_dimanche VARCHAR(128),
                    rdv VARCHAR(8),
                    date_fermeture VARCHAR(64),
                    date_ouverture VARCHAR(64),
                    rdv_site_web TEXT,
                    rdv_tel VARCHAR(32),
                    rdv_tel2 VARCHAR(32),
                    rdv_modalites TEXT,
                    rdv_consultation_prevaccination VARCHAR(8),
                    centre_svi_repondeur VARCHAR(8),
                    centre_fermeture VARCHAR(8),
                    reserve_professionels_sante VARCHAR(8),
                    centre_type VARCHAR(128))
                """)
            cur.execute("""
                CREATE TABLE allocation_vs_appointment (
                    id_centre INTEGER,
                    date_debut_semaine DATE,
                    code_region INTEGER,
                    nom_region VARCHAR(8),
                    code_departement VARCHAR(8),
                    nom_departement VARCHAR(64),
                    commune_insee VARCHAR(8),
                    nom_centre VARCHAR(256),
                    nombre_ucd INTEGER,
                    doses_allouees INTEGER,
                    rdv_pris INTEGER)
                """)
            cur.execute('''
                CREATE TABLE vaccination_vs_appointment (
                    code_region INTEGER,
                    region VARCHAR(8),
                    departement VARCHAR(8),
                    id_centre VARCHAR(32),
                    nom_centre VARCHAR(128),
                    rang_vaccinal INTEGER,
                    date_debut_semaine DATE,
                    nb INTEGER,
                    nb_rdv_cnam INTEGER,
                    nb_rdv_rappel INTEGER
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
                INSERT INTO allocation_vs_appointment(id_centre, date_debut_semaine, code_region, nom_region, code_departement, nom_departement, commune_insee, nom_centre, nombre_ucd, doses_allouees, rdv_pris) 
                VALUES ( %(id_centre)s, %(date_debut_semaine)s, %(code_region)s, %(nom_region)s, %(code_departement)s, %(nom_departement)s, %(commune_insee)s, %(nom_centre)s, %(nombre_ucd)s, %(doses_allouees)s, %(rdv_pris)s)
                ''', item)
            print('table allocation vs appointment created')

def insert_stocks_in_db(pg_conn_string, data):
    with psycopg.connect(pg_conn_string) as db_conn:
        with db_conn.cursor() as cur:
            for item in data:
                cur.execute('''
                INSERT INTO stocks(code_departement, departement, raison_sociale, libelle_pui, finess, type_de_vaccin, nb_ucd, nb_doses, date) 
                VALUES (%(code_departement)s, %(departement)s, %(raison_sociale)s, %(libelle_pui)s, %(finess)s, %(type_de_vaccin)s, %(nb_ucd)s, %(nb_doses)s, %(date)s)
                ''', item)
            print('table stocks created')

def insert_vaccination_centers_in_db(pg_conn_string, data):
    with psycopg.connect(pg_conn_string) as db_conn:
        with db_conn.cursor() as cur:
            for item in data:
                cur.execute('''
                INSERT INTO vaccination_centers(gid,nom,arrete_pref_numero,xy_precis,id_adr,adr_num,adr_voie,com_cp,com_insee,com_nom,lat_coor1,long_coor1,structure_siren,structure_type,structure_rais,structure_num,structure_voie,structure_cp,structure_insee,structure_com,_userid_creation,_userid_modification,_edit_datemaj,lieu_accessibilite,rdv_lundi,rdv_mardi,rdv_mercredi,rdv_jeudi,rdv_vendredi,rdv_samedi,rdv_dimanche,rdv,date_fermeture,date_ouverture,rdv_site_web,rdv_tel,rdv_tel2,rdv_modalites,rdv_consultation_prevaccination,centre_svi_repondeur,centre_fermeture,reserve_professionels_sante,centre_type) 
                VALUES ( %(gid)s, %(nom)s, %(arrete_pref_numero)s, %(xy_precis)s, %(id_adr)s, %(adr_num)s, %(adr_voie)s, %(com_cp)s, %(com_insee)s, %(com_nom)s, %(lat_coor1)s, %(long_coor1)s, %(structure_siren)s, %(structure_type)s, %(structure_rais)s, %(structure_num)s, %(structure_voie)s, %(structure_cp)s, %(structure_insee)s, %(structure_com)s, %(_userid_creation)s, %(_userid_modification)s, %(_edit_datemaj)s, %(lieu_accessibilite)s, %(rdv_lundi)s, %(rdv_mardi)s, %(rdv_mercredi)s, %(rdv_jeudi)s, %(rdv_vendredi)s, %(rdv_samedi)s, %(rdv_dimanche)s, %(rdv)s, %(date_fermeture)s, %(date_ouverture)s, %(rdv_site_web)s, %(rdv_tel)s, %(rdv_tel2)s, %(rdv_modalites)s, %(rdv_consultation_prevaccination)s, %(centre_svi_repondeur)s, %(centre_fermeture)s, %(reserve_professionels_sante)s, %(centre_type)s)
                ''', item)
            print('table vaccination_centers created')

def insert_vaccination_vs_appointment_in_db(pg_conn_string, data):
    with psycopg.connect(pg_conn_string) as db_conn:
        with db_conn.cursor() as cur:
            for item in data:
                cur.execute('''
                INSERT INTO vaccination_vs_appointment(code_region,region,departement,id_centre,nom_centre,rang_vaccinal,date_debut_semaine,nb,nb_rdv_cnam,nb_rdv_rappel) 
                VALUES (%(code_region)s,%(region)s,%(departement)s,%(id_centre)s,%(nom_centre)s,%(rang_vaccinal)s,%(date_debut_semaine)s,%(nb)s,%(nb_rdv_cnam)s, %(nb_rdv_rappel)s)
                ''', item)
            print('table vaccination_vs_appointment created')


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
            insert_vaccination_vs_appointment_in_db(pg_conn, local_data)
        if key == './raw_data/allocations-vs-rdv.csv':
            local_data = data['./raw_data/allocations-vs-rdv.csv']
            insert_allocation_vs_appointment_in_db(pg_conn, local_data)
        if key == './raw_data/stocks.csv':
            local_data = data['./raw_data/stocks.csv']
            insert_stocks_in_db(pg_conn, local_data)
        if key == './raw_data/vaccination_centers.csv':
            local_data = data['./raw_data/vaccination_centers.csv']
            insert_vaccination_centers_in_db(pg_conn, local_data)
        print('finished inserting data in postgresql')

main(SCRIPT_NAME, PG_CONN_STRING, DATA_FOLDER)
