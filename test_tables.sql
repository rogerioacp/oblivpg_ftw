
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
	mirror_index_am Oid,
	ftw_table_nblocks integer,
	ftw_index_nblocks integer,
	ftw_heap_table_oid Oid,
	ftw_index_oid Oid,
	init 	boolean
);

CREATE EXTENSION oblivpg_fdw;
CREATE SERVER obliv FOREIGN DATA WRAPPER oblivpg_fdw;
CREATE FOREIGN DATA WRAPPER oblivpg_fdw OPTIONS (debug 'true');
CREATE FOREIGN TABLE ftw_users(
	id integer,
	name varchar(50),
	age integer,
	gender smallint,
	email varchar(35)
) SERVER obliv;

DROP extension oblivpg_fdw CASCADE;
DROP FOREIGN TABLE obliv_users;

INSERT INTO obliv_users (id, name, age, gender, email) values (1, 'teste', 20, 1, 'teste');

select pg_backend_pid();

#get oid of foreign table 
select Oid from pg_class where relname  = 'ftw_users';

select Oid from pg_class where relname = 'users';

# oid of the index relation.
select Oid from pg_class where relname  = 'user_email';

select relam from pg_class where oid = ?;

# generate debug symbols without optimization
make CFLAGS='-Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -Wno-unused-command-line-argument -g -O0'


insert into obl_ftw (ftw_table_oid, mirror_table_oid, mirror_index_oid, mirror_index_am, ftw_table_nblocks, ftw_index_nblocks, ftw_heap_table_oid, ftw_index_oid, init) values(16401, 16385,   16388, 405, 100, 100, NULL, NULL, false);


CREATE FUNCTION setup_obliv_tables(int4) RETURNS int4
    AS 'oblivpg_fdw', 'setup_obliv_tables'
    LANGUAGE C STRICT;

CREATE FUNCTION init_soe(int4) RETURNS int4
    AS 'oblivpg_fdw', 'init_soe'
    LANGUAGE C STRICT;

select setup_obliv_tables(16401);