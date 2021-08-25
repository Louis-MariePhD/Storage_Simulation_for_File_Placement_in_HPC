from simpy.core import Environment
from typing import List


class File:
    def __init__(self, path: str, tier: "Tier", size: int, ctime: float, last_mod: float, last_access: float):
        """
        :param path: full qualified path
        :param tier: host tier
        :param size: octets
        :param ctime: seconds
        :param last_mod: seconds
        :param last_access: seconds
        """
        self.path = path
        self.tier = tier
        self.size = size
        self.creation_time = ctime
        self.last_modification = last_mod
        self.last_access = last_access


class Tier:
    def __init__(self, name: str, max_size: int, latency: float, throughput: float,
                 target_occupation: float = 0.8):
        """
        TODO: increment used size
        TODO: returns real delays

        :param max_size: octets
        :param latency: seconds
        :param throughput: Go/seconds
        :param target_occupation: [0.0, 1.0[, the maximum allowed used capacity ratio until firing a nearly full event
        """
        self.name = name
        self.max_size = max_size
        self.used_size = 0
        self.latency = latency
        self.throughput = throughput
        self.target_occupation = target_occupation
        self.content = dict()  # key: path, value: File
        self.manager = None

    def has_file(self, path):
        return path in self.content.keys()

    def rename_file(self):
        """
        :return: time in seconds until operation completion
        """
        return 0

    def create_file(self, timestamp, path, size : int = 0):
        """
        :return: time in seconds until operation completion
        """
        file = File(path, self.manager.get_default_tier(), size=size, ctime=timestamp, last_mod=timestamp, last_access=timestamp)
        self.used_size += file.size
        self.content[path]=file
        self.manager.fire_event(self.manager.on_file_created_event, self, (file,))  # file, tier
        if self.used_size >= self.max_size*self.target_occupation:
            self.manager.fire_event(self.manager.on_tier_nearly_full_event, self, ())
        return 0

    def open_file(self):
        """
        :return: time in seconds until operation completion
        """
        return 0

    def read_file(self, timestamp, path):
        """
        :return: time in seconds until operation completion
        """
        self.content[path].last_access = timestamp
        self.manager.fire_event(self.manager.on_file_access_event, self, (self.content[path], False))  # file, tier, is_write
        return 0

    def write_file(self, timestamp, path):
        """
        :return: time in seconds until operation completion
        """
        self.content[path].last_access = timestamp
        self.content[path].last_mod = timestamp
        self.manager.fire_event(self.manager.on_file_access_event, self, (self.content[path], True))  # file, tier, is_write
        #if self.used_size >= self.max_size*self.target_occupation:
        #    self.manager.fire_event(self.manager.on_tier_nearly_full_event, self, ())\
        # TODO: update file size, add offset as arg
        return 0

    def close_file(self):
        """
        :return: time in seconds until operation completion
        """
        return 0

    def delete_file(self, path):
        """
        :return: time in seconds until operation completion
        """
        file = self.content.remove(path)
        self.manager.fire_event(self.manager.on_file_deleted_event, self, (file,))  # file, tier
        return 0


class StorageManager:
    def __init__(self, tiers: List[Tier], env: Environment, default_tier_index: int = 0):
        """
        :param tiers: Tiers in performance order. Default tier is 0, and a file tier index will augment as it ages.
        :param env: Simpy env, used to fire events
        :param default_tier_index: 0 by default, most of the time you want file to be created on the performant tier.
        """
        self._env = env
        self.tiers = tiers
        self.default_tier_index = default_tier_index
        self.on_file_created_event = [env.event()]
        self.on_file_deleted_event = [env.event()]
        self.on_file_access_event = [env.event()]
        self.on_tier_nearly_full_event = [env.event()]

        for tier in tiers:
            tier.manager = self  # association linking

    def fire_event(self, event, tier, value=()):
        e = event[0]
        event[0] = self._env.event()
        e.succeed((tier, *value))

    def get_default_tier(self):
        return self.tiers[self.default_tier_index]

    def get_file(self, path):
        """
        :param path: Fully qualified path
        :return: The first file with the corresponding path found
        """
        for tier in self.tiers:
            if tier.has_file(path):
                return tier.content[path]
        return None

    @staticmethod
    def migrate(file: File, target_tier: Tier):
        """
        :return: The time needed until completion of the migration
        """

        if file.tier is target_tier:
            return  # Migration has already been done. Nothing to do?

        # TODO: find migration delay from a paper
        delay = 0.
        delay += target_tier.create_file()
        delay += max(file.tier.read_file(), target_tier.write_file())
        delay += file.tier.delete_file()

        return delay
