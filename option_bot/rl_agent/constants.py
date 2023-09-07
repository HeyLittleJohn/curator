from pathlib import Path

import pandas as pd
from torch.cuda import is_available

DEVICE = "gpu" if is_available() else "cpu"

# Search Boundaries  #####
CONTRACT_COUNT = 4
DAY_TRADE_LIMIT = 3
COLLATERAL = 2000
DAYS_TIL_EXP = 45
CALENDAR_SPREAD_ABLE = False
ANNUAL_TRADING_DAYS = 252
ACTIONS = ["CLOSE POSITION", "HOLD"]
RISK_FREE = pd.read_csv(str(Path("~").expanduser()) + "/option_bot/risk-free-rate.csv")
RISK_FREE["as_of_date"] = pd.to_datetime(RISK_FREE["as_of_date"])
RISK_FREE["risk_free_rate"] = RISK_FREE["risk_free_rate"].astype(float)

# Training Parameters #####
EPISODES = 1000
BATCH_SIZE = 100
MEMORY_MAX = 100000
SUCCESS_THRESHOLD = 0.5  # 50% of return kept. NOTE: Will change for spreads and long positions

# Model Parameters #####
HIDDEN_SIZE_LG = 512  # can tune these
HIDDEN_SIZE_SM = 256
EPSILON = 1.0
ALPHA = 0.001
GAMMA = 0.99
