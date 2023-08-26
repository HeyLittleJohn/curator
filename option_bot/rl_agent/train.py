from rl_agent.game_environment import GameEnvironment


def train_agent(ticker: str, start_date: str, num_positions: int):
    """
    Script to train the DQN network and the learning agent

    Args:
        ticker (str): ticker symbol
        start_date (str): start date for training data. Will use all price data from start date to present
        num_positions (int): number of positions to hold in the portfolio
    """
    env = GameEnvironment(ticker, start_date, num_positions=num_positions)
    env.prepare_state_data()


def optimize_with_replay():
    pass
