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
2. Run `create user robot with encrypted password 'doctolib';`
3. Run `grant all privileges on database doctolib_data to robot;`