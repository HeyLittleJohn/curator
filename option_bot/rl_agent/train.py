from torch.optim import Adam
from torch.nn import SmoothL1Loss
import torch
import numpy as np

from rl_agent.game_environment import GameEnvironment, calc_port_return_from_positions
from rl_agent.agent import DQN_Network, Memories, transition
from rl_agent.constants import EPISODES, BATCH_SIZE, MEMORY_MAX, DEVICE, FEATURE_COLS


async def train_agent(ticker: str, start_date: str, num_positions: int):
    """
    Script to train the DQN network and the learning agent

    Reward is the total return of the portfolio (aiming to exceed the Success Threshold)
    Meanwhile, r is the transition of return from day to day.
    This nuance is important because Reward != sum(r).

    NOTE: in the future, change reward to be a ratio of overall return as a ratio of days spent in the game.

    Args:
        ticker (str): ticker symbol
        start_date (str): start date for training data. Will use all price data from start date to present
        num_positions (int): number of positions to hold in the portfolio
    """
    env = GameEnvironment(ticker, start_date, num_positions=num_positions)
    await env.prepare_state_data()
    model = DQN_Network(actions_dim=len(env.actions_labels), state_dim=len(FEATURE_COLS))
    model.apply(weights_init)
    model.to(DEVICE)
    sgd = Adam(model.parameters(), lr=model.alpha)
    memory = Memories(MEMORY_MAX)

    done = False
    episode = 0
    new_mem_length = 0
    while episode <= EPISODES:
        state, game_positions = env.reset()
        reward = 0
        while not env.end:
            actions = model.choose_action(state, game_positions)
            next_state, new_game_positions, game_rewards = env.step(actions, current_state=state)

            if not (
                sum(actions.values()) == 0 and env.start_days_to_exp - env.days_to_exp <= 1
            ):  # add to memory if it didn't close the position on the first day
                for tkr in env.opt_tkrs:  # NOTE: may need to ignore closed positions
                    memory.add_transition(
                        transition(
                            state.loc[state["options_ticker"] == tkr].copy(),
                            actions[tkr],
                            game_rewards[tkr][-1],
                            next_state.loc[next_state["options_ticker"] == tkr].copy(),
                            env.end,
                            game_positions[tkr],
                            new_game_positions[tkr],
                        )
                    )
            reward = calc_port_return_from_positions(game_positions)
            # NOTE: "reward" here is only used to check progress, not for optimization

            state = next_state
            game_positions = new_game_positions

        # backward pass
        if len(memory.replay_memory) >= BATCH_SIZE and (
            new_mem_length != len(memory.replay_memory) or len(memory.replay_memory) == MEMORY_MAX
        ):  # if memory is maxed but no new transitions were added, may need to end the training
            print(f"Episode: {episode}, Reward: {reward}, Memories: {len(memory.replay_memory)}")
            optimize_with_replay(model, memory, sgd, BATCH_SIZE)
            model.decay_epsilon()
            model.gamma_update(episode)
            done = model.check_progress(reward, episode)
            episode += 1
            new_mem_length = len(memory.replay_memory)
        if done:
            break
    return model


def optimize_with_replay(model, memories, optimizer, batch_size):
    batch = memories.sample_memories(batch_size)
    batch = transition(*zip(*batch))
    a = torch.tensor(batch.a).to(DEVICE)
    r = torch.tensor(batch.r).to(DEVICE)
    # calculates and gathers the values based on the states + actions with the current model
    pred = model(batch.s, memories_positions=batch.game_positions)
    values = torch.gather(pred, 1, a.reshape(batch_size, 1))  # gathers the value of the best action

    target_values = torch.zeros(batch_size, device=DEVICE)  # starting tensor for actual target values
    non_terminal_s = []
    non_terminal_pos = []
    for i in range(batch_size):
        if not batch.end[i]:
            non_terminal_s.append(batch.s_prime[i])
            non_terminal_pos.append(batch.game_positions_prime[i])

    mask = torch.tensor(~np.array(batch.end), device=DEVICE, dtype=torch.bool)
    target_values[mask] = model(non_terminal_s, memories_positions=non_terminal_pos).max(1)[0].detach()
    target_values = (target_values * model.gamma) + r

    loss_criteria = SmoothL1Loss()
    loss = loss_criteria(values.float(), target_values.unsqueeze(1).float())
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()
    return


@torch.no_grad()
def weights_init(model):
    classname = model.__class__.__name__
    if classname.find("Linear") != -1:
        # get the number of the inputs
        n = model.in_features
        y = 1.0 / np.sqrt(n)
        model.weight.normal_(0.0, 0.1)
        model.bias.fill_(0)
