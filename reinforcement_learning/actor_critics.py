import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
from reinforcement_learning import utils


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


class CNNActor(nn.Module):
    def __init__(self, input_string_size, convolution_layers_params, dense_layers_params, output_size, embedding_dim):
        """
        An actor. Given a state, it will produce an action from a continuous action-space. Will receive feedback and
        learn from the output of the critic (Q-fonction).
        :param input_string_size:
        :param convolution_layers_params:
        :param dense_layers_params:
        :param output_size:
        :param embedding_dim:
        """
        super(nn.Module, self).__init__()
        self.input_string_length = input_string_size

        # Embedding layer
        self.embedding = nn.Embedding(num_embeddings=utils.VOCAB_LEN, embedding_dim=embedding_dim).to(utils.DEVICE)
        self.embedding_dim = embedding_dim
        expected_shape = (1, embedding_dim, input_string_size)

        # Convolutional part
        self.pooling = []
        self.batch_norms = []
        self.conv_layers = []
        for params in convolution_layers_params:
            self.conv_layers += [nn.Conv1d(in_channels=expected_shape[1],
                                           out_channels=params["out_channels"],
                                           kernel_size=params["kernel_size"],
                                           stride=params["stride"]).to(utils.DEVICE)]
            self.pooling += [nn.MaxPool1d(params["pooling_kernel"], params["pooling_stride"])]
            self.batch_norms += [nn.BatchNorm1d(params["out_channels"]).to(utils.DEVICE)]
            expected_shape = utils.get_output_shape(self.pooling[-1], utils.get_output_shape(self.conv_layers[-1], expected_shape))
        self.conv_output_shape = np.prod(expected_shape[1:])

        # Dense part
        expected_shape = (1, self.conv_output_shape)
        self.denses = []
        for params in dense_layers_params:
            self.denses += [nn.Linear(expected_shape[1], params["width"]).to(utils.DEVICE)]
            expected_shape = utils.get_output_shape(self.denses[-1], expected_shape)
        self.head = nn.Linear(expected_shape[1], output_size).to(utils.DEVICE)

    def forward(self, state):
        """
        :param state: a path string preprocessed using utils.str2array()
        :return: the output of the network: an action from a continuous action-space. No clamping is done here.
        """
        x = state.to(utils.DEVICE)
        x = self.embedding(x).transpose(0,1).reshape(-1, self.embedding_dim, self.input_string_length)
        for conv_layer, pooling, batch_norm in zip(self.conv_layers, self.pooling, self.batch_norms):
            x = F.relu(batch_norm(pooling(conv_layer(x))))
        x = x.reshape(-1, self.conv_output_shape)
        for dense in self.denses:
            x = F.relu(dense(x))
        return self.head(x)


class CNNCritic(Critic):
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
