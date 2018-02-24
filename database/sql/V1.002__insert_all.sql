-- author = 'mart2010'
-- copyright = "Copyright 2018, The CRT Project"


-------------------------------------- Data insertion -------------------------------------

-- the goal here is to pre-load all reference data and static content....

-------------------------------------------------------------------------------------------

copy staging.bitstamp_oneminute(epoch_timestp,open_price,high_rate,low_rate,close_rate,volume_btc,volume_usd,weighted_price)
from '/Users/mart/Google Drive/brd/ref_data/Iso_639_and_Marc_code - ISO-639-2_utf-8.tsv'
with delimiter ',', header, ;


insert into integration.currency 
values
(1, 'BTC', 'Bitcoin', 'crypto')
(2, 'ETH', 'Ether', 'crypto')
(3, 'LTC', 'Litecoin', 'crypto')
(4, 'USD', 'US Dollar', 'fiat')
(5, 'EUR', 'Euro', 'fiat')
(6, 'CHF', 'Swiss franc', 'fiat')
;


insert into integration.currency_pair
values
(1, 'BTC-USD', 'Bitcoin against US dollar')
(2, 'EUR-USD', 'Euro against US dollar')



insert into integration.exchange 
values
(1, 'Bitstamp', 'Single Bitstamp source')
;



insert into integration.period(code, open, close, open_epoch, close_epoch, duration_sec, )
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
		select 'H1', '1 hours'::interval, 3600
	) as p
) as foo
;


insert into integration.exchange_rate

(
select epoch_timestp
		, first(open_price) over (partition by p.)
		, 
from staging.bitstamp_oneminute b 
join integration.period p on (b.epoch_timestp >= p.open_epoch and b.epoch_timestp < p.close_epoch )




bitstamp_oneminute(epoch_timestp,open_price,high_rate,low_rate,close_rate,volume_btc,volume_usd,weighted_price)



insert into integration.cluster_addr_heuristic(heuristic_code, heuristic_desc, create_date)
values
('', '...', now())
,('', '...', now())
;



