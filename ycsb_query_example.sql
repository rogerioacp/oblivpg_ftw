alter table mirror_usertable set unlogged;
CREATE FUNCTION update_mapping3() RETURNS void AS
$$
        DECLARE
                ftw_users_oid oid;
                user_oid oid;
                user_email_oid oid;
        BEGIN
                ftw_users_oid := 0;
                user_oid := 0;
                user_email_oid := 0;
                select Oid from pg_class into ftw_users_oid  where relname  = 'ftw_usertable';

                select Oid from pg_class into user_oid where relname = 'mirror_usertable';

                select Oid from pg_class into user_email_oid  where relname  = 'mirror_usertable_key';
                delete from obl_ftw where ftw_table_oid = ftw_users_oid;
                
                insert into obl_ftw (ftw_table_oid, mirror_table_oid, mirror_index_oid, ftw_table_nblocks, ftw_index_nblocks, init) values(ftw_users_oid, user_oid, user_email_oid, 176000, 6000, false);

        END;
$$ LANGUAGE plpgsql;

select update_mapping3();


\set autocommit off;
BEGIN;
BEGIN;
select open_enclave();
select init_soe(0, CAST( get_ftw_oid() as INTEGER), 1, CAST (get_original_index_oid() as INTEGER));
select load_blocks(CAST (get_original_index_oid() as INTEGER), CAST (get_original_heap_oid() as INTEGER));
declare tcursor CURSOR FOR select YCSB_KEY from ftw_usertable;
select set_nextterm('277476164');
FETCH NEXT tcursor;
END;
select close_enclave();
