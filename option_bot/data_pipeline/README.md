# Data Pipeline

## Overview

This package contains the tools, scripts, and clients needed to download data from the Polygon API, clean it, and upload to . Downloaded data is stored in raw `.json` in the `option_bot/data/` directory.

## Organization

- `polygon_utils.py` contains clients customized to download data from specific endpoints. Additionally they house the functions to clean the results the API returns, despite the cleaning not occuring during the download step. Note: The `download_data()` co-routine in the PolygonPaginator clients are the target functions of the process pools in download.py.

- `download.py` contains the code that creates the process pools which enable asyncronous network requests scaled across the number of cores on the machine. "Download" vernacular includes querying the api and writing the results to disk.

- `uploader.py` contains the `Uploader` object and the process pools to read data from disk, clean it, and upload asyncronously to the database. Like `download.py` this module is dependent on polygon_utils.py's methods to clean the data according to asset type.

- `orchestrator.py` is the file that orchestrates the diffent components of the data pipeline including downloading, cleaning, and upload to db. Functions in this file are called by `main.py`, and are organized according to asset type and scope of the data to be acquired.

- `main.py` is the entrypoint for the data pipeline. It contains an CLI that triggers specific functions from `orchestrator.py`. `main.py` will be scheduled to run.

## Notes

`all_` as a boolean argument indicates whether dealing with all tickers or just a list of tickers. This is its consistent use. It never indicates downloading "all components" (i.e. metadata, stock prices, options contracts, options prices)
