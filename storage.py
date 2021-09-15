from os import environ
from simpy.core import Environment
from typing import List


class File:
    def __init__(self, path: str, tier: "Tier", size: int, ctime: float, last_mod: float, last_access: float, user : str = 'default_user'):
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
        self.user = user

        assert self.path not in self.tier.content
        self.tier.content[path] = self


class Tier:
    def __init__(self, name: str, max_size: int, latency: float, throughput: float,
                 target_occupation: float = 0.9):
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
        self.listeners = []
        self.currently_migrating = False

        self.number_of_reads = 0
        self.number_of_write = 0
        self.number_of_eviction_from_this_tier = 0
        self.number_of_eviction_to_this_tier = 0
        self.number_of_prefetching_from_this_tier = 0
        self.number_of_prefetching_to_this_tier = 0

        self.time_spent_reading = 0
        self.time_spent_writing = 0

    def register_listener(self, listener: "Policy"):
        self.listeners += [listener]

    def stats(self):
        return {"number_of_reads": self.number_of_reads,
                "number_of_write": self.number_of_write,
                "number_of_eviction_to_this_tier": self.number_of_eviction_to_this_tier,
                "number_of_prefetching_from_this_tier": self.number_of_prefetching_from_this_tier,
                "number_of_prefetching_to_this_tier": self.number_of_prefetching_to_this_tier,
                "time_spent_reading": self.time_spent_reading,
                "time_spent_writing": self.time_spent_writing}

    def has_file(self, path):
        return path in self.content.keys()

    def rename_file(self):
        """
        :return: time in seconds until operation completion
        """
        return 0

    def create_file(self, timestamp, path, size: int = 0, file: File = None, migration=False):
        """
        :param timestamp: timestamp of the file creation event
        :param path: path of the file being created. No file must exist at this path in this tier
        :param size: init size of the file. this will not be counted as a write
        :param file: optional. a file whose metadata will be copied onto the newly created file.
        :param migration: whether the file creation event was caused by a migration. prevents event loops.

        :return: time in seconds until operation completion
        """
        assert path not in self.content.keys()

        if file is None:
            file = File(path, self, size=size, ctime=timestamp, last_mod=timestamp, last_access=timestamp)
        else:
            assert file.path == path
            assert file.path not in self.content.keys()  # not supposed to happen with our policies
            file = File(path, self, file.size, file.creation_time, file.last_modification, file.last_access)
            assert file.path in self.content.keys()
        self.used_size += file.size
        self.time_spent_writing += self.latency # taille du header faible, on considère uniquement la latence.
        assert path in self.content.keys()
        assert file.path in self.content.keys()
        for listener in self.listeners:
            listener.on_file_created(file)
            if not migration and self.used_size >= self.max_size * self.target_occupation and not self.currently_migrating:
                self.currently_migrating = True
                listener.on_tier_nearly_full()
                self.currently_migrating = False
        return 0

    def open_file(self):
        """
        :return: time in seconds until operation completion
        """
        return 0

    def read_file(self, timestamp, path, update_meta=True, cause=None):
        """
        :return: time in seconds until operation completion
        """
        if path in self.content.keys():
            file = self.content[path]
            if update_meta:
                file.last_access = timestamp
            for listener in self.listeners:
                listener.on_file_access(file, False)
            self.number_of_reads += 1
            self.time_spent_reading += self.latency + file.size/self.throughput
            if cause is not None:
                if cause == "eviction":
                    self.number_of_eviction_from_this_tier += 1
                elif cause == "prefetching":
                    self.number_of_prefetching_from_this_tier += 1
                else:
                    raise RuntimeError(f'Unknown cause {cause}. Expected "eviction", "prefetching" or None')
        else:
            print(f'File {path} should be in tiers {self.name} but it is not. Recheck your trace and policies!')
        return 0

    def write_file(self, timestamp, path, update_meta=True, cause=None):
        """
        :return: time in seconds until operation completion
        """
        if path in self.content.keys():
            file = self.content[path]
            if update_meta:
                file.last_access = timestamp
                file.last_mod = timestamp
            for listener in self.listeners:
                listener.on_file_access(file, True)
            self.number_of_write += 1
            self.time_spent_writing += self.latency + file.size/self.throughput
            if cause is not None:
                if cause == "eviction":
                    self.number_of_eviction_to_this_tier += 1
                elif cause == "prefetching":
                    self.number_of_prefetching_to_this_tier += 1
                else:
                    raise RuntimeError(f'Unknown cause {cause}. Expected "eviction", "prefetching" or None')
            # TODO: update file size, add offset as arg
        else:
            print(f'File {path} should be in tiers {self.name} but it is not. Recheck your trace and policies!')
        return 0

    def close_file(self):
        """
        :return: time in seconds until operation completion
        """
        return 0

    def delete_file(self, path, event_priority=0):
        """
        :return: time in seconds until operation completion
        """
        if path in self.content.keys():
            file = self.content.pop(path)
            self.used_size -= file.size
            for listener in self.listeners:
                listener.on_file_deleted(file)
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

        for tier in tiers:
            tier.manager = self  # association linking

    def delay(self, timeout, cb):
        yield self._env.timeout(timeout)
        cb()

    def get_default_tier(self):
        return self.tiers[self.default_tier_index]

    def get_file(self, path):
        """
        :param path: Fully qualified path
        :return: The first file with the corresponding path found
        """
        for tier in self.tiers:
            if tier.has_file(path):
                file = tier.content[path]
                assert file.path == path
                return file
        return None

    @staticmethod
    def migrate(file: File, target_tier: Tier, timestamp):
        """
        :return: The time needed until completion of the migration
        """

        if file.path in target_tier.content.keys():
            return 0

        is_eviction = file.tier.manager.tiers.index(file.tier) < file.tier.manager.tiers.index(target_tier)
        cause = ["prefetching", "eviction"][is_eviction]

        delay = 0.
        delay += target_tier.create_file(timestamp, file.path, file=file, migration=True)
        assert file.path in target_tier.content.keys()
        delay += max(file.tier.read_file(timestamp, file.path, update_meta=False, cause=cause),
                     target_tier.write_file(timestamp, file.path, update_meta=False, cause=cause))
        delay += file.tier.delete_file(file.path, event_priority=2)

        return delay
