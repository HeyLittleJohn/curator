from proj_constants import RH_USERNAME, RH_PASSWORD, QR
from pyrh import Robinhood


rh = Robinhood(username=RH_USERNAME, password=RH_PASSWORD, mfa=QR)
rh.login()

("Portfolio Data:")
print(rh.portfolio())
print()
("Positions Data:")
print(rh.positions())
print()
print("Account Data:")
print(rh.get_account())
