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

CREATE FOREIGN DATA WRAPPER oblivpg_fdw
  HANDLER oblivpg_fdw_handler
  VALIDATOR oblivpg_fdw_validator;



drop server obliv;
DROP EXTENSION oblivpg_fdw CASCADE;
DROP FOREIGN TABLE obliv_users;