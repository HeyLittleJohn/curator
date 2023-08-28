from torch.optim import Adam

from rl_agent.game_environment import GameEnvironment
from rl_agent.agent import DQN_Network
from rl_agent.constants import EPISODES, BATCH_SIZE, MEMORY_MAX, DEVICE


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
    model = DQN_Network(actions_dim=len(env.actions), state_dim=len(env.feature_cols))
    sgd = Adam(model.parameters(), lr=model.alpha)

    for i in range(EPISODES):
        state = env.reset()
        reward = 0
        while not env.end:
            action = model.choose_action(state)
            next_state, reward = env.step(action)
            model.rewards.append(reward)
            state = next_state


def optimize_with_replay():
    pass
