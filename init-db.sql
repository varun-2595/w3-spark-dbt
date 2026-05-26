-- Create the dbt warehouse database if it doesn't exist
SELECT 'CREATE DATABASE warehouse_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'warehouse_db')\gexec
