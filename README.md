# Little John

This project contains the code to backfill data (from [Polygon](https://polygon.io), [Robinhood with the unofficial API](https://github.com/robinhood-unofficial/pyrh)), build a training environment for a trading agent, and then to train/test a DQN algorithm.

The end goal is to teach a bot to be able to successfully manage a portfolio of options, primarily in the short position, to consistently generate portfolio cash flow with the objective of maximizing reward relative to the amount of collateral required. By selling OTM spreads, a trader can consistently generate such cash flows. This project is aimed at giving a bot the same learned experience of a veteran trader on wall street.

Randomness in market movements is assumed, and this project in no way attempts to predict prices of securities. Rather, we will generate a deterministic value policy of when to hold/close/roll our options prices given the characteristics of state.

We can then use the policy and the bot to identify options spread positions that would be of high-likelihood of generating profit, while also determining ideal actions in the instance that stock prices move close to our spread boundaries.

## Getting Started

This is a python project, and will utilize pyenv, poetry, pre-commit, and will be greatly benefitted by an Nvidia GPU for training. This project will build on the default `dev` branch and push to `prod` via "Squash Merge" PR only. At which time, we will cut a new release / tag for the project.

### PyEnv

Install [pyenv](https://github.com/pyenv/pyenv) and use it to create a virtualenv for this project with a version of python that is >=3.11

### Poetry

We use [Poetry](https://python-poetry.org/) for dependency management. Once poetry is installed on your machine and you've cloned this repo, go into the project root `option_bot` directory and run `poetry install` to install all dependencies indicated in the pyTOML.

### Data Sources

We will utilize Polygon.io and Robinhood as our sources of historical and current data, as well as to programatically call our portoflio data and execute new trades/orders. As such it will be required that you have an API key for the Polygon.io API (available for free, the code is built to backfill taking into account the 5 queries / min limitation of the free tier) and a funded brokerage account with Robinhood. 

Store your Polygon.io API key locally in your `.bashrc` file in a variable called `"POLYGON_API_KEY"`
Likewise, store your username, password, and MFA QR code for Robinhood locally using variable names `"RH_USRNAME"`, `"RH_PASSWORD"`, and `"RH_QR"`.

## Using the App

To begin using the app, navigate to your root `option_bot` folder (after having poetry installed everything), and run the following command to build your local db:
```CLI Command Here```

Then run the below command to view the available CLI for the project:
```CLI command Here```

Finally, utilize the below command to add your first underlying ticker to "your universe" to begin backfilling historical stock and option pricing data necessary to begin training your agent. Depending on whether you are subscribed to the free tier of the Polygon.io API or not, this could take a bit of time. Be sure to not interrupt the process until it is done.
