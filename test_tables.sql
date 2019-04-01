CREATE EXTENSION oblivpg_fdw;

INSERT INTO ftw_users (id, name, age, gender, email) values (1, 'teste', 20, 1, 'teste');

select pg_backend_pid();

#get oid of foreign table 
select Oid from pg_class where relname  = 'ftw_users';

select Oid from pg_class where relname = 'users';
# oid of the index relation.
select Oid from pg_class where relname  = 'user_email';

select relam from pg_class where oid = ?;

insert into obl_ftw (ftw_table_oid, mirror_table_oid, mirror_index_oid, ftw_table_nblocks, ftw_index_nblocks, init) values(16399, 16392, 16395, 100, 100, false);

select init_soe(16399);

select log_special_pointer();

# generate debug symbols without optimization
make CFLAGS='-Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -Wno-unused-command-line-argument -g -O0'

