from storage import StorageManager, File, Tier
from simpy.core import Environment


class Policy:
    def __init__(self, storage: StorageManager, env: Environment):
        self.storage = storage
        self.env = env

        for event, callback in [(storage.on_file_created_event, self.on_file_created),
                                (storage.on_file_access_event, self.on_file_access),
                                (storage.on_disk_occupation_increase_event, self.on_disk_occupation_increase)]:
            self.env.process(self._storage_event_listener(target=storage,
                                                          event=event,
                                                          callback=callback))

    @staticmethod
    def _storage_event_listener(target, event, callback):
        while True:
            e = event[0]
            yield e
            callback(*e.value)

    def on_file_created(self, file: File, tier: Tier):
        pass

    def on_file_access(self, file: File, tier: Tier, is_write: bool):
        pass

    def on_disk_occupation_increase(self, tier: Tier):
        pass
