import argparse
from datetime import datetime

from dateutil.relativedelta import relativedelta
from orchestrator import add_tickers_to_universe


DEFAULT_DAYS = 120
DEFAULT_START_DATE = datetime.now() - relativedelta(months=24)


def add_ticker(args):
    kwargs_list = []
    for ticker in args.tickers:
        a = {
            "ticker": ticker,
            "start_date": datetime.strptime(args.startdate) if args.startdate else DEFAULT_START_DATE,
            "price_days": args.pricedays if args.pricedays else DEFAULT_DAYS,
        }
        kwargs_list.append(a)
    add_tickers_to_universe(kwargs_list)


def remove_ticker(args):
    # remove from tickers table, allow cascading to remove everything else
    return


def main():
    """primary CLI for adding underlying stocks to our data universe, and specifying how far back to pull data"""

    parser = argparse.ArgumentParser(
        description="CLI for adding stocks to the data pull process and for refreshing stock/options pricing data"
    )

    parser.add_argument(
        "tickers",
        type=str,
        nargs="*",
        metavar="underlying tickers",
        default=None,
        help="Adds underlying tickers to our options data pull universe",
    )

    parser.add_argument(
        "-d",
        "--startdate",
        type=str,
        nargs=1,
        metavar="YYYY-MM",
        help="YYYY-MM formatted date str indicating start of data pull for s",
    )

    parser.add_argument(
        "-p",
        "--pricedays",
        type=int,
        nargs=1,
        default=DEFAULT_DAYS,
        metavar="num_days",
        help="Int specifying number of price days wanted for each options contract",
    )

    parser.add_argument(
        "-r",
        "--remove",
        help="Removes the specified underlying ticker(s) from our options data pull universe",
    )

    args = parser.parse_args()

    if args.remove is None:
        add_ticker(args)

    else:
        remove_ticker(args)


if __name__ == "__main__":
    main()
