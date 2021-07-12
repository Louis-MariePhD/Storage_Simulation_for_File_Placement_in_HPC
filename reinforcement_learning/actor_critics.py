import torch
import torch.nn as nn
import torch.nn.functional as F


class Actor(nn.Module):
    """Equivalent to the policy"""
    def forward(self, state):
        raise NotImplementedError()


class Critic(nn.Module):
    """Q-Fonction equivalent"""
    def forward(self, state, action):
        raise NotImplementedError()


class ExampleActor(nn.Module):
    def __init__(self, input_size, hidden_size, output_size, learning_rate=3e-4):
        super(ExampleActor, self).__init__()
        self.linear1 = nn.Linear(input_size, hidden_size)
        self.linear2 = nn.Linear(hidden_size, hidden_size)
        self.linear3 = nn.Linear(hidden_size, output_size)

    def forward(self, state):
        x = F.relu(self.linear1(state))
        x = F.relu(self.linear2(x))
        x = torch.tanh(self.linear3(x))
        return x


class ExampleCritic(Critic):
    def __init__(self, input_size, hidden_size, output_size):
        super(ExampleCritic, self).__init__()
        self.linear1 = nn.Linear(input_size, hidden_size)
        self.linear2 = nn.Linear(hidden_size, hidden_size)
        self.linear3 = nn.Linear(hidden_size, output_size)

    def forward(self, state, action):
        x = torch.cat([state, action], 1)
        x = F.relu(self.linear1(x))
        x = F.relu(self.linear2(x))
        x = self.linear3(x)
        return x
