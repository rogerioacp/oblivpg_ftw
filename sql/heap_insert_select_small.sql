/*This script tests the sequential insertion and later retrieval of records using only the oblivious heap. The entire scripts tests the integration of the oblivpg_fdw.c with the SOE lib, uses the enclave to store and access tuple which will trigger multiple ORAM reads and writes. However, all access are done to the oblivious heap and do not use the oblivious index. Furthermore, this test verifies if the SOE correctly keeps track how many tuples are per page and if it expands the in-memory abstraction of the heap by assigning new buffers. In reality the oblivious heap has a predetermined number of blocks that can not expand. However, for the hash and heap accesses to the file, this is abstracted by the soe_bufmgr that creates virtual pages.*/

CREATE EXTENSION oblivpg_fdw;

/*the extension creates an example table ftw_users.*/

CREATE FUNCTION update_mapping() RETURNS void AS 
$$
	DECLARE
		ftw_users_oid oid;
		user_oid oid;
		user_email_oid oid;
	BEGIN
		ftw_users_oid := 0;
		user_oid := 0;
		user_email_oid :=0;
		select Oid from pg_class into ftw_users_oid  where relname  = 'ftw_users';

		select Oid from pg_class into user_oid where relname = 'users';

		select Oid from pg_class into user_email_oid  where relname  = 'user_email';

		insert into obl_ftw (ftw_table_oid, mirror_table_oid, mirror_index_oid, ftw_table_nblocks, ftw_index_nblocks, init) values(ftw_users_oid, user_oid, user_email_oid, 100, 100, false);

	END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION get_ftw_oid() RETURNS oid AS
$$
	DECLARE
		ftw_users_oid oid;
	BEGIN
		select Oid from pg_class into ftw_users_oid  where relname  = 'ftw_users';
		RETURN ftw_users_oid;
	END;
$$ LANGUAGE plpgsql;

select update_mapping();

/*start enclave*/

select open_enclave();


/*initiate soe*/
select init_soe(CAST (get_ftw_oid() as INTEGER), 1);


/*insert into oblivious table*/

INSERT INTO ftw_users (id, name, age, gender, email) values (1,'Florrie',31,0,'fhundy0@tinypic.com');
INSERT INTO ftw_users (id, name, age, gender, email) values (2,'Eustace',27,1,'edack1@over-blog.com');
INSERT INTO ftw_users (id, name, age, gender, email) values (3,'Bethena',47,1,'brattray2@freewebs.com');
INSERT INTO ftw_users (id, name, age, gender, email) values (4,'Cristina',44,1,'cboissieux3@si.edu');
INSERT INTO ftw_users (id, name, age, gender, email) values (5,'Berne',24,0,'britson4@desdev.cn');
INSERT INTO ftw_users (id, name, age, gender, email) values (6,'Helenka',97,0,'hseiler5@unicef.org');
INSERT INTO ftw_users (id, name, age, gender, email) values (7,'Elwyn',15,1,'erogister6@e-recht24.de');
INSERT INTO ftw_users (id, name, age, gender, email) values (8,'Hayward',12,0,'hchurly7@ucla.edu');
INSERT INTO ftw_users (id, name, age, gender, email) values (9,'Tiena',82,1,'tjozwiak8@wordpress.org');
INSERT INTO ftw_users (id, name, age, gender, email) values (10,'Frederik',94,0,'fkeough9@weather.com');
INSERT INTO ftw_users (id, name, age, gender, email) values (11,'Alexandr',89,1,'ajordena@amazon.co.jp');
INSERT INTO ftw_users (id, name, age, gender, email) values (12,'Derron',62,1,'dlauderdaleb@mozilla.com');
INSERT INTO ftw_users (id, name, age, gender, email) values (13,'Berkley',12,0,'basipenkoc@usda.gov');
INSERT INTO ftw_users (id, name, age, gender, email) values (14,'Saundra',63,1,'schampleyd@narod.ru');
INSERT INTO ftw_users (id, name, age, gender, email) values (15,'Tess',98,0,'tmitrovice@virginia.edu');
INSERT INTO ftw_users (id, name, age, gender, email) values (16,'Orlan',52,1,'obottenf@wordpress.org');
INSERT INTO ftw_users (id, name, age, gender, email) values (17,'Drusilla',61,0,'dhucknallg@cisco.com');
INSERT INTO ftw_users (id, name, age, gender, email) values (18,'Georgie',56,1,'gadmansh@so-net.ne.jp');
INSERT INTO ftw_users (id, name, age, gender, email) values (1999,'Tanhya',4,0,'thallerrq@eventbrite.com');
INSERT INTO ftw_users (id, name, age, gender, email) values (2000,'Corney',69,0,'cabraminorr@ow.ly');


select set_next();

select * from ftw_users;

select close_enclave();