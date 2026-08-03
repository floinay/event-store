-- Run as a PostgreSQL cluster administrator. Credentials are supplied by deployment secrets.
CREATE ROLE event_store_owner NOLOGIN;
CREATE ROLE event_store_migrator LOGIN NOINHERIT;
CREATE ROLE event_store_app LOGIN NOINHERIT;
CREATE ROLE event_store_cdc LOGIN REPLICATION NOINHERIT;
CREATE ROLE projection_worker LOGIN NOINHERIT;
GRANT event_store_owner TO event_store_migrator;
