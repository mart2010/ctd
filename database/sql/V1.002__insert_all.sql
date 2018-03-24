-- author = 'mart2010'
-- copyright = "Copyright 2018, The CRT Project"


-------------------------------------- Data insertion -------------------------------------

-- the goal here is to pre-load all reference data and static content....

-------------------------------------------------------------------------------------------


insert into itg.currency 
values
(1, 'USD', 'US Dollar', 'fiat')
(2, 'EUR', 'Euro', 'fiat')
(3, 'CHF', 'Swiss Franc', 'fiat')
(4, 'CDN', 'Canadian Dollar', 'fiat')
(100, 'BTC', 'Bitcoin', 'crypto')
(101, 'ETH', 'Ether', 'crypto')
(102, 'LTC', 'Litecoin', 'crypto')
;


insert into itg.currency_pair
select base.currency_id + quote.currency_id
		, base.currency_id
		, quote.currency_id
		, concat(base.ticker, '/', quote.ticker)
from itg.currency base 
cross join itg.currency quote
where quote.ticker = 'USD'
;



insert into itg.exchange_source
values
(1, 'Bitstamp', 'Single Bitstamp source', 'http://')
(2, 'Kanggle?'
(3, 'Dukascopy', 'Swiss market watch', 'http://')
;



insert into itg.period(code, open, close, open_epoch, close_epoch, duration_sec, )
select foo.period_code
		, foo.open
		, lead(foo.open) over (partition by foo.period_code order by foo.open)
		, date_part('epoch',foo.open)::int 
		, date_part('epoch',lead(foo.open) over (partition by foo.period_code order by foo.open))::int
		, foo.duration
from 
(select p.c as period_code
		, generate_series(cast('2012-01-01' as date), cast('2012-01-02' as date), p.i) as open
		, p.d as duration
from 
	( 	select 'M15' as c, '15 minutes'::interval as i, 900 as d
		union
		select 'H1', '1 hour'::interval, 3600
		union
		select 'H4', '4 hours'::interval, 14400
		union
		select 'D1', '1 day'::interval, 14400
		
	) as p
) as foo
;


-- copy initial file 

-- as postgres connected to ctd
\c ctd postgres
copy stg.exchange_import(begin_txt,price_open,price_high,price_low,price_close,volume)
from 'c:\Users\d7loz9\dev\ctd\data_static\EURUSD_15m_BID_01.01.2010-31.12.2016.csv'
csv header;

\c ctd ctd
update stg.exchange_import 
	set pair = 'EUR/USD', 
		begin_timestmp = to_timestamp(begin_txt, 'YYYY-MM-DD HH24:MI'),
		source_file = 'EURUSD_15m_BID_01.01.2010-31.12.2016.csv',
		source_url = 'https://www.kaggle.com/meehau/EURUSD',
		time_standard = 'GMT',
		volume_unit = 'EUR?'
where source_file IS NULL;


\c postgres ctd
copy itg.bitstamp_oneminute(epoch_timestp,open_price,high_rate,low_rate,close_rate,volume_btc,volume_usd,weighted_price)
from 'c:\Users\d7loz9\dev\ctd\data_static\...'
with delimiter ',', header, ;

bitstamp_oneminute(epoch_timestp,open_price,high_rate,low_rate,close_rate,volume_btc,volume_usd,weighted_price)



insert into itg.exchange_rate(pair_id,period_id,source_id,open,high,low,close,volume)

(
select cp.pair_id 
		, p.period_id
		, s.source_id
		, i.price_open
		, i.price_high
		, i.price_low
		, i.price_close
		, i.volume
from stg.exchange_import i 
join itg.period p on (b.begin_timestp = i.begin_timestmp and b.end_timestp = lead(i.begin_timestmp) over (order by i.begin_timestmp))
join itg.currency_pair cp on (i.pair = cp.pair_ticker)
join itg.source_exchange s on (s.)



	begin_timestmp varchar(50),
	begin_epoch bigint,
	price_open real,
	price_high real,
	price_low real,
	price_close real,
	price_unit varchar(30),
	pair varchar(30),
	weigthed_price real,
	volume real,
	volume_unit varchar(30),
	source_file varchar(50),
	source_url varchar(100),
	time_standard varchar(30)




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




insert into itg.cluster_addr_heuristic(heuristic_code, heuristic_desc, create_date)
values
('', '...', now())
,('', '...', now())
;





