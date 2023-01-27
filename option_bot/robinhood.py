import os

from pyrh import Robinhood

USERNAME = os.getenv("RH_USERNAME")
PASSWORD = os.getenv("RH_PASSWORD")
QR = os.getenv("RH_QR")


rh = Robinhood(username=USERNAME, password=PASSWORD, mfa=QR)
