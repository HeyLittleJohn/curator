import torch

device = "gpu" if torch.cuda.is_available() else "cpu"

print(device)


# Search Boundaries  #####
CONTRACT_COUNT = 4
DAY_TRADE_LIMIT = 3
COLLATERAL = 2000
DAYS_TIL_EXP = 45
CALENDAR_SPREAD_ABLE = False
