/* contrib/oblivpg_fdw/oblivpg_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION oblivpg_fdw" to load this file. \quit



CREATE FUNCTION oblivpg_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION oblivpg_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION init_soe(int4)
RETURNS int4
AS 'MODULE_PATHNAME', 'init_soe'
LANGUAGE C STRICT;


CREATE FUNCTION log_special_pointer()
RETURNS int4
AS 'MODULE_PATHNAME', 'log_special_pointer'
LANGUAGE C STRICT;


DROP SERVER IF EXISTS obliv;
DROP FOREIGN TABLE IF EXISTS ftw_users;

CREATE FOREIGN DATA WRAPPER oblivpg_fdw
  HANDLER oblivpg_fdw_handler
  VALIDATOR oblivpg_fdw_validator;

CREATE SERVER obliv FOREIGN DATA WRAPPER oblivpg_fdw;


create table users(
	id integer,
	name varchar(50),
	age integer,
	gender smallint,
	email varchar(35)
);

create index user_email on users using hash (email);


create table obl_ftw(
	ftw_table_oid Oid,
	mirror_table_oid Oid,
	mirror_index_oid Oid,
	ftw_table_nblocks integer,
	ftw_index_nblocks integer,
	init 	boolean
);

CREATE FOREIGN TABLE ftw_users(
	id integer,
	name varchar(50),
	age integer,
	gender smallint,
	email varchar(35)
) SERVER obliv;