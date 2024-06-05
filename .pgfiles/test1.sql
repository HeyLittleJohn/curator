select a.ticker, count(b.id) as prices from stock_tickers as a
left join options_tickers on a.id = options_tickers.underlying_ticker_id
left join option_prices as b on options_tickers.id = b.options_ticker_id
group by 1
order by 2 desc
limit 10