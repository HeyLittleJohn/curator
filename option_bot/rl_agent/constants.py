import pandas as pd
from pathlib import Path

# Search Boundaries  #####
CONTRACT_COUNT = 4
DAY_TRADE_LIMIT = 3
COLLATERAL = 2000
DAYS_TIL_EXP = 45
CALENDAR_SPREAD_ABLE = False
ANNUAL_TRADING_DAYS = 252
ACTIONS = ["HOLD", "CLOSE POSITION"]
RISK_FREE = pd.read_csv(str(Path("~").expanduser()) + "/option_bot/risk-free-rate.csv")
RISK_FREE["as_of_date"] = pd.to_datetime(RISK_FREE["as_of_date"])
RISK_FREE["risk_free_rate"] = RISK_FREE["risk_free_rate"].astype(float)
