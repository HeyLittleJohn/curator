
-- view into data pull success
select a.type, count(distinct a.ticker) as stock_tickers, count(b.id) as options_tickers, count(c.id) as options_price_points from stock_tickers a
left join options_tickers b
on a.id = b.underlying_ticker_id
left join option_prices c
on b.id = c.options_ticker_id
group by a.type