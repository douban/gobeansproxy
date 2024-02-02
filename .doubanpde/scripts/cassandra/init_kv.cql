-- DEFAULT USER: cassandra
-- default pass: cassandra
-- change cassandra password:
--   ALTER USER cassandra WITH PASSWORD 'verysecretpass';

-- create a user for doubandb
CREATE USER IF NOT EXISTS doubandb_test WITH PASSWORD 'doubandb_test';
CREATE ROLE IF NOT EXISTS doubandb_admin;
-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS doubandb WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '3' };
GRANT ALL ON KEYSPACE doubandb TO doubandb_admin;
GRANT doubandb_admin TO doubandb_test;

CREATE TYPE IF NOT EXISTS doubandb.bdbvalue (
        rtime timestamp,
        flag int,
        exptime int,
        cas int,
        body blob
);
-- Create a table
CREATE TABLE IF NOT EXISTS doubandb.kvstore (
        key blob PRIMARY KEY,
        value doubandb.bdbvalue,
) WITH compression = {'class': 'ZstdCompressor'};

-- insert a @ value for test
--INSERT INTO doubandb.kvstore (key, value)
--        VALUES ('@', {rtime: '2023-06-21 08:01:14.247000+0000', flag: 0, exptime: 0, cas: 0, body: null});
