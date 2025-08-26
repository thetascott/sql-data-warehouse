/*
======================================================================================================================
Create Database
======================================================================================================================
Script Purpose:
This script creates a new database named 'DataWarehouse'. If the database exists, it is dropped and recreated.

WARNING:
This script will drop the entire 'DataWarehouse' database if it exists. All data in the database will be permanently deleted. Proceed with caution!!!
*/

-- Drop database if it exists
DROP DATABASE IF EXISTS "DataWarehouse";

-- Create database
CREATE DATABASE "DataWarehouse";
