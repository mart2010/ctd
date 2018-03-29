
-- As superuser, create role crt
	create role ctd with login password 'ctd';
	alter role ctd CREATEROLE;
	create database ctd owner= ctd;

-- As superuser, switch to new db and revoke privileges to other users */
	\c ctd
	revoke connect on database ctd from public;
	revoke all on schema public from public;
	grant all on schema public to ctd;


-- used to backup database
pg_dump -f ctd_20180727.sql  --schema=staging --schema=integration -U crt  -p 54355 crt

