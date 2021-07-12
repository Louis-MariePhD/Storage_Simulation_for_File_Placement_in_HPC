import torch
import torch.nn as nn
import torch.optim as optim
from torch.autograd import Variable
from reinforcement_learning.replay_memory import Memory

"""
Deep Deterministic Gradient Descent
Composed of: an Actor network that generate an action-value from a continuous action space
             a Critic network that is basically a Q-Network
It is well described there (https://spinningup.openai.com/en/latest/algorithms/ddpg.html) and
there (https://towardsdatascience.com/deep-deterministic-policy-gradients-explained-2d94655a9b7b)
"""


class DDGD:
    def __init__(self, actor, actor_target, critic, critic_target,
                 actor_learning_rate=1e-4,
                 critic_learning_rate=1e-3,
                 gamma=0.99,
                 tau=1e-2,
                 max_memory_size=50000):
        self.actor = actor
        self.actor_target = actor_target
        self.critic = critic
        self.critic_target = critic_target
        self.tau = tau
        self.gamma = gamma

        for target_param, param in zip(self.actor_target.parameters(), self.actor.parameters()):
            target_param.data.copy_(param.data)

        for target_param, param in zip(self.critic_target.parameters(), self.critic.parameters()):
            target_param.data.copy_(param.data)

            # Training
        self.memory = Memory(max_memory_size)
        self.critic_criterion = nn.MSELoss()
        self.actor_optimizer = optim.Adam(self.actor.parameters(), lr=actor_learning_rate)
        self.critic_optimizer = optim.Adam(self.critic.parameters(), lr=critic_learning_rate)

    def get_action(self, state):
        state = Variable(torch.from_numpy(state).float().unsqueeze(0))
        action = self.actor.forward(state)
        action = action.detach().numpy()[0, 0]
        return action

    def update(self, batch_size):
        states, actions, rewards, next_states, _ = self.memory.sample(batch_size)
        states = torch.FloatTensor(states)
        actions = torch.FloatTensor(actions)
        rewards = torch.FloatTensor(rewards)
        next_states = torch.FloatTensor(next_states)
        
        # Critic/Q-function updates
        Qvals = self.critic.forward(states, actions)
        next_actions = self.actor_target.forward(next_states)
        next_Q = self.critic_target.forward(next_states, next_actions.detach())
        Qprime = rewards + self.gamma * next_Q

        critic_loss = nn.MSELoss(Qvals, Qprime)
        self.critic_optimizer.zero_grad()
        critic_loss.backward()
        self.critic_optimizer.step()

        # Actor Updates
        policy_loss = -self.critic.forward(states, self.actor.forward(states)).mean()

        self.actor_optimizer.zero_grad()
        policy_loss.backward()
        self.actor_optimizer.step()

        # Target networks updates
        for target_param, param in zip(self.actor_target.parameters(), self.actor.parameters()):
            target_param.data.copy_(self.tau * param.data + (1.0 - self.tau) * target_param.data)

        for target_param, param in zip(self.critic_target.parameters(), self.critic.parameters()):
            target_param.data.copy_(self.tau * param.data + (1.0 - self.tau) * target_param.data)