
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
	table_oid Oid,
	index_oid Oid
);

select Oid from pg_class where relname = 'users';

insert into obl_ftw (ftw_oid, table_oid, index_oid) values(?, ?, ?);

# oid of the index relation.
select Oid from pg_class where relname = 'user_email';

# indrelid is the oid of the table that is indexed.
# The OID of the pg_class entry for the table this index is for
select * from pg_index where indrelid = ?;
