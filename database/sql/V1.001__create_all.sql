-- author = 'mart2010'
-- copyright = "Copyright 2018, The CTD Project"


-------------------------------------- Schema creation -------------------------------------
create schema staging;
create schema integration;
create schema presentation;


------------------------------------------ Staging layer -----------------------------------------------
--------------------------------------------------------------------------------------------------------
-- Goals:   - Layer where raw data is bulk loaded straight from source. ... 
--
--------------------------------------------------------------------------------------------------------
create table integration.load_audit (
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
comment on table staging.load_audit is 'Metadata to report on running batch_job/steps';
comment on column staging.load_audit.status is 'Status of step';
comment on column staging.load_audit.run_dts is 'Timestamp when step run (useful for things like limiting harvest period)';
comment on column staging.load_audit.output is 'Output produced by a step like error msg when failure or additional info';


-- ? needed 
create table staging.txoutraw (
	...
	...
    foreign key (load_audit_id) references staging.load_audit(id)
);


--.........



-- staging table for the btc kaggle file (https://www.kaggle.com/smitad/bitcoin-trading-strategy-simulation/data)
create table staging.bitstamp_oneminute(
	epoch_timestp bigint,
	open_price decimal(10,2),
	high_rate decimal(10,2),
	low_rate decimal(10,2),
	close_rate decimal(10,2),
	volume_btc float,
	volume_usd float,
	weigthed_price decimal(10,2)
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

create table integration.currency (
	currency_id smallint primary key,
	ticker varchar(10) unique not null,
	name varchar(50),
	type varchar(50)   -- fiat, crypto..
);


create table integration.currency_pair (
	pair_id smallint primary_key,
	pair_ticker varchar(10) unique not null,  -- ex. EUR-USD (Euro against US dollar)
	pair_desc varchar(50)
);


create table integration.period (
	period_id bigserial primary key,
	open timestamp not null, 
	close timestamp not null, --> = next(open)
	open_epoch integer, -- unix time in sec since epoch
	close_epoch integer,
	time_code varchar(10) not null,  -- ex. M15 (15min)
	duration_sec integer not null,
	unique (open, close)
);


-- to convert unix timestamp (since epoch) to postgres timestamp
-- select to_timestamp(1195374767);

-- To convert back to unix timestamp :
-- select date_part('epoch',CURRENT_TIMESTAMP)::integer 


-- source exchange used as source (ex. bitstamp, or many exchanges with price index such as the one from coindesk)
-- or website for historical data 
create table integration.exchange_source (
	source_id integer primary key,
	name varchar(30),
	description varchar(200),
	volume_unit varchar(30)
);


create table integration.exchange_rate (
	period_id bigint,
	pair_id integer,
	source_id integer,
	open_rate real,   -- float OK: data normalized downstream.. (6 decimal precision, ok for fiat as well)
	high_rate real,
	low_rate real,
	close_rate real,
	volume real, -- in diff. unit depending on source (to be normalized anyway)
	primary key (exchange_id, period_id, coin_id),
	foreign key (period_id) references integration.period(period_id),
	foreign key (pair_id) references integration.currency_pair(currency_pair_id)
);


-- other useful metrics from bitstamp....













-- BLOCKCHAIN-related data

/*
create table integration.block (
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
    foreign key (prev_block_id) references integration.block(block_id) on delete cascade
);
comment on table integration.block is '...';
comment on column integration.site.coinbase is 'Field used as the sole input for coinbase trx to claim block reward (up to 100 bytes for arbitrary data)';


create table integration.tx (
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
    foreign key (block_id) references integration.block(block_id) on delete cascade
);
comment on table integration.tx is '......';
comment on column integration.size is '...';

create table integration.txin (
    txin_id bigint primary key,
    tx_id bigint not null,
    tx_pos integer not null,
    txout_id bigint not null,
    load_audit_id integer,
    unique (tx_id, tx_pos),
    foreign key (tx_id) references integration.tx(tx_id) on delete cascade,
    foreign key (txout_id) references integration.txout(txout_id) on delete cascade
);
comment on table integration.txin is '...';
comment on column integration.txin.tx_pos is '...';

create table integration.txout (
    txout_id bigint primary key,
    tx_id bigint not null,
    tx_pos integer no not null,
    address_id bigint not null,
    value_btc bigint not null,
    load_audit_id integer,
    unique (tx_id, tx_pos),
    foreign key (tx_id) references integration.tx(tx_id) on delete cascade,
    foreign key (address_id) references integration.address(address_id) on delete cascade
);
comment on table integration.txout is '...';
comment on column integration.txout.tx_pos is '...';

create table integration.address (
    address_id bigint primary key,
    address_base58 char(58) not null, --address prefix '0', Pay-To-ScriptHash prefix '3'
    --maybe store the compr/uncompr publickey as a way to indicate whether the address corresponds to which form...
	publickey char(130),  --full uncompressed (X,Y data +prefix 04, 130Hex)...useful to recognize 2
	diff address from same PubKey..not sure it seems that other pk format is used for the other Address
    load_audit_id int
);
comment on table integration.address is '....';


*/










-----------------------------------------------------------------------------------------------
-------------------------------------- Business Sub-layer -------------------------------------
-----------------------------------------------------------------------------------------------
create table integration.cluster_addr_heuristic (
	heuristic_code varchar(20) primary key,
	heuristic_desc varchar(100)
);
comment on table integration.merge_addr_heuristic is 'Heuristic rules used to link addresses into same cluster (entity: whether a single user, an exchange, etc..)';


create table integration.batch_cluster_address (
    address_id bigint,
    sameas_id bigint,
    heuritisic varchar(20),
    load_audit_id integer,
    primary key (address_id, sameas_id),
    foreign key (address_id) references integration.address(address_id),
    foreign key (sameas_id) references integration.address(address_id)
);
comment on table integration.batch_cluster is 'Flatten adjency list of addresses considered part of same cluster WITHIN the same transaction batch ';
comment on column integration.address_sameas.address_id is 'Base address using arbitrarily one of the clustered addresses'; 
comment on column integration.address_sameas.address_id is 'The paired address within the cluster part of the same cluster';



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




