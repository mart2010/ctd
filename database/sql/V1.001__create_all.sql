-- author = 'mart2010'
-- copyright = "Copyright 2018, The CTD Project"


-------------------------------------- Schema creation -------------------------------------
create schema stg;
create schema itg;
create schema pres;


------------------------------------------ Staging layer -----------------------------------------------
--------------------------------------------------------------------------------------------------------
-- Goals:   - Layer where raw data is bulk loaded straight from source. ... 
--
--------------------------------------------------------------------------------------------------------
create table itg.load_audit (
    id serial primary key,
    batch_job text,
    step_name text,
    step_no integer,
    status text,
    run_dts timestamp,
    elapse_sec integer,
    rows_impacted integer,
    output text
);
comment on table stg.load_audit is 'Metadata to report on running batch_job/steps';
comment on column stg.load_audit.status is 'Status of step';
comment on column stg.load_audit.run_dts is 'Timestamp when step run';
comment on column stg.load_audit.output is 'Output produced by a step like error msg when failure or additional info';


-- ? needed 
create table stg.txoutraw (
	...
	...
    foreign key (load_audit_id) references stg.load_audit(id)
);

--.........

-- staging table for various initial files 
-- ex. btc kaggle file (https://www.kaggle.com/smitad/bitcoin-trading-strategy-simulation/data)
create table stg.exchange_import(
	begin_txt varchar(50),
	begin_timestmp timestamp,
	begin_epoch long,
	end_timestmp timestamp,
	pair varchar(30),
	period_txt varchar(30),
	price_open real,
	price_high real,
	price_low real,
	price_close real,
	weigthed_price real,
	volume_from real,
	volume_to real,
	source_file varchar(50),
	source_url varchar(100),
	time_standard varchar(30)
);



------------------------------------------ Integration layer -------------------------------------------
--------------------------------------------------------------------------------------------------------
-- Two sub-layers:
--     - 1 Raw sub-layer: untransformed data from source without applying business rules
--     - 2 Business sub-layer: apply some transformation to help preparing for presentation layer
--             2.1 de-duplication (same_as,  etc...)
--             2.2 any sort of standardization/harmonization...
--
--------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------
-------------------------------------- Raw Sub-layer -------------------------------------
------------------------------------------------------------------------------------------

create table itg.period (
	period_id bigserial primary key,
	begin_period timestamp not null, 
	end_period timestamp not null, --> = next(open)
	begin_epoch integer, -- unix time in sec since epoch
	end_epoch integer,
	timeframe_code varchar(5) not null,  -- ex. M1, M5, M15, H1, H4, D1, W1, MN (1-month)...
	timeframe_value varchar(100), -- ex. 01:15:00, 01:45, etc.
	duration_sec integer not null,
	unique (begin_period, end_period)
);

create table itg.currency (
	currency_id smallint primary key,
	ticker varchar(10) unique not null,
	name varchar(50),
	type varchar(50)   -- fiat, crypto..
);

create table itg.currency_pair (
	pair_id integer primary key,
	base_currency_id smallint,
	quote_currency_id smallint,
	pair_ticker varchar(10) unique not null,  -- EUR/USD  (EUR=base, USD=quote, EUR/USD = 1.25, i.e. 1 EUR = 1.25 USD)
	unique (base_currency_id, quote_currency_id)
);

-- to convert unix timestamp (since epoch) to postgres timestamp : select to_timestamp(1195374767);
-- To convert back to unix timestamp : select date_part('epoch',CURRENT_TIMESTAMP)::integer 

-- source exchange used as source (ex. bitstamp, or exchanges with avg price index -coindesk, or website for historical data)
create table itg.exchange_source (
	source_id integer primary key,
	name varchar(30),
	description varchar(200)
);

create table itg.exchange_rate (
	pair_id integer,
	period_id bigint,
	source_id integer,
	open real,   -- OK: data normalized downstream.. (6 decimal precision, ok for fiat as well)
	high real,
	low real,
	close real,
	volume real, -- in diff. unit depending on source (to be normalized anyway)
	primary key (pair_id, period_id, source_id),
	foreign key (pair_id) references itg.currency_pair(pair_id),
	foreign key (period_id) references itg.period(period_id),
	foreign key (source_id) references itg.exchange_source(source_id)
);


-- other useful metrics from bitstamp....













-- BLOCKCHAIN-related data

/*
create table itg.block (
    block_id bigint primary key,
    hash char(64) unique not null,
    version integer not null
    coinbase text not null,
    price integer,
    size integer not null,
    difficulty float not null,
    block_time timestamp,
    fees integer,
    height integer,
    prev_block_id bigint,
    load_audit_id integer,
    foreign key (prev_block_id) references itg.block(block_id) on delete cascade
);
comment on table itg.block is '...';
comment on column itg.site.coinbase is 'Field used as the sole input for coinbase trx to claim block reward (up to 100 bytes for arbitrary data)';


create table itg.tx (
    tx_id bigint primary key,
    hash char(64) unique not null,
    block_id bigint not null,
    version integer,
    locktime integer,
    size integer not null,
    fee bigint,
    nb_inputs integer,
    nb_outputs integer,
    load_audit_id integer,
    foreign key (block_id) references itg.block(block_id) on delete cascade
);
comment on table itg.tx is '......';
comment on column itg.size is '...';

create table itg.txin (
    txin_id bigint primary key,
    tx_id bigint not null,
    tx_pos integer not null,
    txout_id bigint not null,
    load_audit_id integer,
    unique (tx_id, tx_pos),
    foreign key (tx_id) references itg.tx(tx_id) on delete cascade,
    foreign key (txout_id) references itg.txout(txout_id) on delete cascade
);
comment on table itg.txin is '...';
comment on column itg.txin.tx_pos is '...';

create table itg.txout (
    txout_id bigint primary key,
    tx_id bigint not null,
    tx_pos integer no not null,
    address_id bigint not null,
    value_btc bigint not null,
    load_audit_id integer,
    unique (tx_id, tx_pos),
    foreign key (tx_id) references itg.tx(tx_id) on delete cascade,
    foreign key (address_id) references itg.address(address_id) on delete cascade
);
comment on table itg.txout is '...';
comment on column itg.txout.tx_pos is '...';

create table itg.address (
    address_id bigint primary key,
    address_base58 char(58) not null, --address prefix '0', Pay-To-ScriptHash prefix '3'
    --maybe store the compr/uncompr publickey as a way to indicate whether the address corresponds to which form...
	publickey char(130),  --full uncompressed (X,Y data +prefix 04, 130Hex)...useful to recognize 2
	diff address from same PubKey..not sure it seems that other pk format is used for the other Address
    load_audit_id int
);
comment on table itg.address is '....';


*/










-----------------------------------------------------------------------------------------------
-------------------------------------- Business Sub-layer -------------------------------------
-----------------------------------------------------------------------------------------------
create table itg.cluster_addr_heuristic (
	heuristic_code varchar(20) primary key,
	heuristic_desc varchar(100)
);
comment on table itg.merge_addr_heuristic is 'Heuristic rules used to link addresses into same cluster (entity: whether a single user, an exchange, etc..)';


create table itg.batch_cluster_address (
    address_id bigint,
    sameas_id bigint,
    heuritisic varchar(20),
    load_audit_id integer,
    primary key (address_id, sameas_id),
    foreign key (address_id) references itg.address(address_id),
    foreign key (sameas_id) references itg.address(address_id)
);
comment on table itg.batch_cluster is 'Flatten adjency list of addresses considered part of same cluster WITHIN the same transaction batch ';
comment on column itg.address_sameas.address_id is 'Base address using arbitrarily one of the clustered addresses'; 
comment on column itg.address_sameas.address_id is 'The paired address within the cluster part of the same cluster';



--1st heuristic: both compressed and uncompressed Address-forms belong to SAME privateKey, entity (not sure this should be one...)
--2nd Multi-input Linkage : inputs Address belong to same entity except for case like CoinJoin operations,  different address-types, ??
--3rd Change Linkage : change Address belong to the same Address as the input Address(es) (payee entity).  Change identification rule (before certain block use bug, after use various rules ... never seen Address (iif there's one exist among all output Addresses), output Address of same type as input Address(es) iif  the output is didfferent from the rest of the output)
--4th Multi-hop linkage: self-churn identification (see 3.4 BlockSci paper) 
--5th Public disclose:  feeding from website letting user publish their own Address 





--------------------------------------------------------------------------

--each elt run, we add new rows independtly from previous... no trying to have a complete graph view --> this is done downstream using table
-- in fact, a downstream process will collapse these cluster transitively (ex. if A and B are the same in batch-1 and B and C in batch-2 then A,B,C are the same )


-- Multi-input heuristic 
-- probably do not have to verify address-type as the payee must control all private keys so should be same entity (TO check that there are no case of mix-match address-type among inputs !!!! )
insert into batch_cluster_address
with many_ins as
	(select ti.tx_id
			, a.address_id 
			, count() over (partition by ti.tx_id) as nb_inputs
			, sum(case when substr(a.address_base58,1,1) = '1' then 1 else 0 end) as nb_normal_address
			, sum(case when substr(a.address_base58,1,1) = '3' then 1 else 0 end) as nb_multisig_address
	from txin ti 
	join txout tou on (ti.txout_id = tou.txout_id)
	join address a on (a.address_id = tou.address_id)
	where 
	--TODO:  filter out CoinJoin inputs
	-- .....
	and load_id > #last_load_id#   -- only process transactions after previous processed batch
	)
select	min(address_id) over (partition by tx_id) as address_id  -- arbitrarily choose smallest as main address
		, address_id as sameas_id
		, 'Multi-input linkage'
		, 1111111
from many_ins
where nb_normal_address = nb_inputs 
or nb_multisig_address = nb_inputs 
;


-- Change heuristic 

-- 1) Single input cases

-- 1.1. bug of bitcoin case ---only done on initial load  TODO.......



-- 1.2 normal linkage after that bug correction date...

-- these are the rules of Fistfull paper....
-- Rules: Adress is change if 1) never appeared on network before (no other output address qualifies on this criteria), 2) not output adress same as one input (this would be the change), 3) no Coin generation tx  

-- Add rules on conflicting input-output type




