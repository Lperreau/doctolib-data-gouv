# How to run the project
## Initial requirements
1. Run command `python3 -m venv venv`
1. Run command `. venv/bin/activate` to activate the virtual environment
1. Run `pip install --upgrade pip`
1. Run `sudo apt install python3-dev libpq-dev`
1. Run command `pip install -r requirements.txt` to install packages installed in the virtual environment

## Install all requirements 
Run command `pip install -r requirements.txt`

## Log in to the PostgreSQL database server using psql
1. Run command `sudo -i -u postgres`
2. Run `psql` to start the database server

## Create a new database, a user and grant accesses
1. Run `create database doctolib_data;`
2. Run `create user louis with encrypted password 'doctolib';`
 1. Make sure the user name is the same as your linux user name
 1. If you don't you might get the following error: "psycopg.OperationalError: connection failed: FATAL:  Peer authentication failed for user "robot" "
3. Run `grant all privileges on database doctolib_data to robot;`

## Find CSVs files encoding
1. run command `file -bi raw_data/allocations-vs-rdv.csv `
 . In our case, allocations-vs-rdv has encoding `charset=unknown-8bit` which breaks our function reader to read the csv.