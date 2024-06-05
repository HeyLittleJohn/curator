-- Temporary table for faster processing
CREATE TEMP TABLE quotes (
    ticker TEXT,
    ask_exchange TEXT,       
    ask_price NUMERIC,
    ask_size INTEGER,
    bid_exchange INTEGER,
    bid_price NUMERIC,
    bid_size INTEGER,
    sequence_number BIGINT,
    sip_timestamp TIMESTAMP
);

-- Load data using COPY (adjust path)
COPY quotes FROM stdin 
WITH (FORMAT CSV, DELIMITER ',');


CREATE INDEX ON quotes (ticker);  
ANALYZE quotes;


-- Aggregate into candlestick bars
SELECT 
    ticker,
    -- Open Prices
    (FIRST_VALUE(bid_price) OVER w + FIRST_VALUE(ask_price) OVER w) / 2 AS open_midpoint,
    FIRST_VALUE(bid_price) OVER w AS open_bid,
    FIRST_VALUE(ask_price) OVER w AS open_ask,

    -- Close Prices
    (LAST_VALUE(bid_price) OVER w + LAST_VALUE(ask_price) OVER w) / 2 AS close_midpoint,
    LAST_VALUE(bid_price) OVER w AS close_bid,
    LAST_VALUE(ask_price) OVER w AS close_ask,

    -- Averages and Spread
    AVG((ask_price - bid_price) / 2) OVER w AS average_spread,
    AVG(bid_price) OVER w AS average_bid,
    AVG(ask_price) OVER w AS average_ask,
    AVG((ask_price + bid_price) / 2) OVER w AS average_midpoint,

    -- Percentage Above/Below Open/Close
    COUNT(*) FILTER (WHERE (ask_price + bid_price) / 2 > open_midpoint) * 100.0 / COUNT(*) OVER w AS pct_above_open,
    COUNT(*) FILTER (WHERE (ask_price + bid_price) / 2 < open_midpoint) * 100.0 / COUNT(*) OVER w AS pct_below_open,
    COUNT(*) FILTER (WHERE (ask_price + bid_price) / 2 > close_midpoint) * 100.0 / COUNT(*) OVER w AS pct_above_close,
    COUNT(*) FILTER (WHERE (ask_price + bid_price) / 2 < close_midpoint) * 100.0 / COUNT(*) OVER w AS pct_below_close,
    
    COUNT(*) OVER w AS count_quotes

FROM quotes
WINDOW w AS (PARTITION BY ticker ORDER BY sip_timestamp)
GROUP BY ticker;

-- Drop temporary table
DROP TABLE quotes;
