import argparse
from datetime import datetime

from dateutil.relativedelta import relativedelta
from orchestrator import add_tickers_to_universe, remove_ticker_from_universe


DEFAULT_DAYS = 500
DEFAULT_MONTHS_HIST = 24
DEFAULT_START_DATE = datetime.now() - relativedelta(months=DEFAULT_MONTHS_HIST)


async def add_ticker(args):
    await add_tickers_to_universe(
        [
            {
                "ticker": ticker,
                "start_date": datetime.strptime(args.startdate),
                "opt_price_days": args.pricedays,
                "months_hist": args.monthhist,
            }
            for ticker in args.tickers
        ]
    )


def remove_tickers(args):
    tickers = list(args.tickers) if type(args.tickers) != list else args.tickers
    remove_ticker_from_universe(tickers)


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
        help="Adds underlying tickers to our options data pull universe",
    )

    parser.add_argument(
        "-d",
        "--startdate",
        type=str,
        nargs=1,
        default=DEFAULT_START_DATE.strftime("%Y-%m"),
        metavar="YYYY-MM",
        help="YYYY-MM formatted date str indicating start of data pull for s",
    )

    parser.add_argument(
        "-m",
        "--monthhist",
        type=str,
        nargs=1,
        default=DEFAULT_MONTHS_HIST,
        metavar="int: Months of historical data",
        help="The number of months of historical options contracts you are going to pull",
    )

    parser.add_argument(
        "-op",
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
        action="store_true",
        help="Removes the specified underlying ticker(s) from our options data pull universe",
    )

    parser.add_argument(
        "-aa",
        "--add-all",
        default=False,
        action="store_true",
        help="Adds all stocks tickers to the command. This works with refresh, or adding tickers",
    )

    parser.add_argument(
        "-ref",
        "--refresh",
        default=False,
        action="store_true",
        help="Removes the specified underlying ticker(s) from our options data pull universe",
    )

    args = parser.parse_args()

    if not args.remove:
        add_ticker(args)

    else:
        remove_ticker(args)


if __name__ == "__main__":
    main()
