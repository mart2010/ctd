-- author = 'mart2010'
-- copyright = "Copyright 2018, The CTD Project"


-- As superuser, create role crt
	create role crt with login password 'crt';
	alter role crt CREATEROLE;
	create database crt owner= crt;

-- As superuser, switch to new db and revoke privileges to other users */
	\c crt
	revoke connect on database crt from public;
	revoke all on schema public from public;
	grant all on schema public to crt;


-- used to backup database
pg_dump -f crt_20180727.sql  --schema=staging --schema=integration -U crt  -p 54355 crt

--used to export table as flat files (for Redshift LOAD import)
-- by default Null values are encoded as '\N' which is also the default used by Redshift LOAD command
\copy presentation.dim_site to '/Users/mart/Temp/dimsite_20160811.txt' with (format text, delimiter '|', encoding 'utf-8')
\copy presentation.dim_language to '/Users/mart/Temp/dimlanguage_20160811.txt' with (format text, delimiter '|', encoding 'utf-8')
\copy presentation.dim_book to '/Users/mart/Temp/dimbook_20160811.txt' with (format text, delimiter '|', encoding 'utf-8')
\copy presentation.dim_date to '/Users/mart/Temp/dimdate_20160811.txt' with (format text, delimiter '|', encoding 'utf-8')
\copy presentation.dim_author to '/Users/mart/Temp/dimauthor_20160811.txt' with (format text, delimiter '|', encoding 'utf-8')
\copy presentation.dim_tag to '/Users/mart/Temp/dimtag_20160811.txt' with (format text, delimiter '|', encoding 'utf-8')
\copy presentation.dim_mds to '/Users/mart/Temp/dimmds_20160811.txt' with (format text, delimiter '|', encoding 'utf-8')
\copy presentation.dim_reviewer to '/Users/mart/Temp/dimreviewer_20160811.txt' with (format text, delimiter '|', encoding 'utf-8')

\copy presentation.rel_author to '/Users/mart/Temp/relauthor_20160811.txt' with (format text, delimiter '|', encoding 'utf-8')
\copy presentation.rel_tag to '/Users/mart/Temp/reltag_20160811.txt' with (format text, delimiter '|', encoding 'utf-8')

\copy presentation.review to '/Users/mart/Temp/review_20160811.txt' with (format text, delimiter '|', encoding 'utf-8')
