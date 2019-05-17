# How to compile & install

- The makefile assumes the projects is under the postgres' contrib folder on the source code.

- Define the environment variable to link the generated library with the SGX libs on the untrusted side.

	- The following example compiles the library to run the enclave in simulation mode.
	Use different flags to run in the hardware.

```bash 

export SGX_LIB="-L/opt/intel/sgxsdk/lib64 -lsgx_urts_sim -lpthread -lsgx_uae_service_sim"

```

- Define the environment variables containing the location of signed enclave lib.
 
 ```bash

 export ENCLAVE_LIB="-L/usr/local/lib/soe"

 ```


- Run Make with the include directory for SGX and SOE dependencies.

```bash

 make CFLAGS="-Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -g -O0 -fPIC -I. -I./ -I/usr/local/pgsql/include/server -I/usr/local/pgsql/include/internal -I/usr/local/include/soe -I/opt/intel/sgxsdk/include"

```

- Install the library.

```bash

sudo "PATH=$PATH" make install

```

# How to create test with a simple example

- Create the database data folder

```shell

initdb -D data

```

- Update the configurations to write logs to a file

- Set the linker lookup library path to the location of the libsoe and SGX SDK

```shell

export LD_LIBRARY_PATH="/usr/local/lib/soe:/opt/intel/sgxsdk/lib64/"

```

- Start the database on the data folder

```shell

postgres -D data

```

- Create the database


```shell

createdb teste

```

- Login to the the database

```shell

psql teste

```


- Follow the following example script

```sql

CREATE EXTENSION oblivpg_fdw;

# the extension creates an example table ftw_users.


select pg_backend_pid();

# the extension creates an example table ftw_users.
#get oid of foreign table 
select Oid from pg_class where relname  = 'ftw_users';


# the extension creates an example table users.
select Oid from pg_class where relname = 'users';

# the example table users has an index 
select Oid from pg_class where relname  = 'user_email';

#select relam from pg_class where oid = ?;

#Replace with correct oids.
insert into obl_ftw (ftw_table_oid, mirror_table_oid, mirror_index_oid, ftw_table_nblocks, ftw_index_nblocks, init) values(16399, 16392, 16395, 100, 100, false);


#start enclave

select open_enclave();

#initiate soe
select init_soe(16399);

select log_special_pointer();

#insert into oblivious table



INSERT INTO ftw_users (id, name, age, gender, email) values (1, 'teste', 20, 1, 'teste');

#block offset, page item offset
select set_row(0,1);

select * from ftw_users;

```
