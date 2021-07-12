from storage import StorageManager, File, Tier
from simpy.core import Environment


class Policy:
    def __init__(self, tier: Tier, storage: StorageManager, env: Environment):
        """
        :param storage:
        :param env:
        """
        self.tier = tier
        self.storage = storage
        self.env = env

        for event, callback in [(storage.on_file_created_event, self.on_file_created),
                                (storage.on_file_deleted_event, self.on_file_deleted),
                                (storage.on_file_access_event, self.on_file_access),
                                (storage.on_tier_nearly_full_event, self.on_tier_nearly_full)]:
            self.env.process(self._storage_event_listener(event=event,
                                                          callback=callback))

    def _storage_event_listener(self, event, callback):
        while True:
            e = event[0]
            yield e
            if e.value[0] == self.tier:
                callback(*e.value[1:])

    def on_file_created(self, file: File):
        pass

    def on_file_deleted(self, file: File):
        pass

    def on_file_access(self, file: File, is_write: bool):
        pass

    def on_tier_nearly_full(self):
        pass