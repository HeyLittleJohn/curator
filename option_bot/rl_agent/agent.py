import torch
import numpy as np
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F

device = "gpu" if torch.cuda.is_available() else "cpu"

SUCCESS_THRESHOLD = 200  # TODO: need to tune these


class DQN_Network(nn.Module):
    def __init__(self, actions_dim, state_dim):
        super().__init__()
        self.actions_dim = actions_dim
        self.hidden_size_lg = 512  # can tune these
        self.hidden_size_sm = 256
        self.epsilon = 1.0
        self.alpha = 0.001
        self.gamma = 0.99
        self.rewards = []
        self.best = (-1000, 0)  # initialized at Nothing

        # layers
        self.fc1 = nn.Linear(state_dim, self.hidden_size_lg)
        self.fc2 = nn.Linear(self.hidden_size_lg, self.hidden_size_sm)
        self.fc3 = nn.Linear(self.hidden_size_sm, actions_dim)

    def forward(self, x):
        if not type(x) == torch.Tensor:
            x = torch.tensor(x).to(device)
        x = F.leaky_relu(self.fc1(x))
        x = F.leaky_relu(self.fc2(x))
        x = F.leaky_relu(self.fc3(x))
        # x = torch.sigmoid(self.fc3(x)) # an alternative to leaky_relu
        return x

    def choose_action(self, state):
        if np.random.rand() <= self.epsilon:
            action = np.random.randint(self.actions_dim)
        else:  # need to use the model to predict
            with torch.no_grad():
                pred = self.forward(state)
                action = torch.argmax(pred).item()  # gets it off the gpu
        return action

    def decay_epsilon(self):
        if self.epsilon > 0.001:
            self.epsilon *= 0.9

    def gamma_update(self, episode):
        if episode % 20 == 0 and self.gamma < 0.95:
            self.gamma += 0.05

    def check_progress(self, reward, episode, done=False):
        """function to evaluate the reward being earned per episode, compared with recent averages.
        If reward is above a certain threshold, the agent is considered to have solved the environment"""

        self.rewards.append(reward)
        if reward > self.best[0]:
            print("new best job! Score:{}, Episode:{}".format(reward, episode))
            self.best = (reward, episode)
        recent = np.mean(self.rewards[-100:])
        if recent > SUCCESS_THRESHOLD:
            print("Done! Your last 100 episode average was: ", recent)
            done = True
        if episode % 100 == 0:
            print("Episode:{}, recent average:{}".format(episode, recent))
        return done


class Memories(object):
    pass


class test_agent(object):
    pass
