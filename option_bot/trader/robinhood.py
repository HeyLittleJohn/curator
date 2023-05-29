from proj_constants import RH_USERNAME, RH_PASSWORD, QR, TRAD_IRA, ROTH_IRA
from pyrh import Robinhood, urls
from yarl import URL
from pyrh.models import PortfolioSchema


# rh = Robinhood(username=RH_USERNAME, password=RH_PASSWORD, mfa=QR)
# rh.login()


class RH_Controller(Robinhood):
    def setup(self):
        self.login()
        self.ira_accts = [TRAD_IRA, ROTH_IRA]
        self.portfolios = self.portfolio_data()

    def _retirment_url_generator(self, url: URL):
        accts = self.ira_accts
        accts.insert(0, "")
        for acct in accts:
            yield url / (str(acct) + "/") if acct != "" else url

    def _clean_results(self, result):
        return result["results"][0] if "results" in result else result

    def portfolio_data(self):
        return [self._clean_results(self.get(url)) for url in self._retirment_url_generator(urls.PORTFOLIOS)]


# ("Portfolio Data:")
# print(rh.portfolio())
# print()
# ("Positions Data:")
# print(rh.positions())
# print()
# print("Account Data:")
# print(rh.get_account())

rh = RH_Controller(username=RH_USERNAME, password=RH_PASSWORD, mfa=QR)
rh.setup()
print(rh.portfolios)
