drop database;
drop user;
create database doctolib_data;
create user robot with encrypted password 'doctolib';
grant all privileges on database doctolib_data to robot;

