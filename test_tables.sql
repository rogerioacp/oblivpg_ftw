
create table users(
	id integer,
	name varchar(50),
	age integer,
	gender smallint,
	email varchar(35)
);

create index user_email on users using hash (email);

create table obl_ftw(
	ftw_oid Oid,
	mirror_table_oid Oid,
	mirror_index_oid Oid,
	mirror_index_am Oid,
	ftw_index_oid Oid
);

CREATE EXTENSION oblivpg_fdw;
CREATE SERVER obliv FOREIGN DATA WRAPPER oblivpg_fdw;
CREATE FOREIGN DATA WRAPPER oblivpg_fdw
    OPTIONS (debug 'true');
CREATE FOREIGN TABLE obliv_users(
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

select Oid from pg_class where relname = 'users';

# oid of the index relation.
select Oid from pg_class where relname  = 'user_email';

select relam from pg_class where oid = ?;


# indrelid is the oid of the table that is indexed.
# The OID of the pg_class entry for the table this index is for
select * from pg_index where indrelid = ?;


insert into obl_ftw (ftw_oid, table_oid, index_oid) values(?, ?, ?);
