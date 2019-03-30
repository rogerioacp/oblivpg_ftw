/* contrib/oblivpg_fdw/oblivpg_fdw--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "Extension has to be installed on postgres" to load this file. \quit

DROP FUNCTION setup_obliv_tables(int4) 
CREATE FUNCTION setup_obliv_tables(int4)
RETURNS int4
AS 'MODULE_PATHNAME', 'setup_obliv_tables'
LANGUAGE C STRICT;

DROP FUNCTION init_soe(int4) 
CREATE FUNCTION init_soe(int4)
RETURNS int4
AS 'MODULE_PATHNAME', 'init_soe'
LANGUAGE C STRICT;
