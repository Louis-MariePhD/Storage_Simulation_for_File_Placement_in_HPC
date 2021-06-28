
class Policy:
    def __init__(self, storage, env=None):
        self.storage = storage
        if env is not None:
            self.env = env
        else:
            self.env = storage.env

        for event_name, callback in [("on_file_created_event", self.on_file_created),
                                     ("on_file_access_event", self.on_file_access),
                                     ("on_disk_occupation_increase_event", self.on_disk_occupation_increase)]:
            self.env.process(self._storage_event_listener(target=storage,
                                                          event_name=event_name,
                                                          callback=callback))

    @staticmethod
    def _storage_event_listener(target, event_name, callback):
        while True:
            event = getattr(target, event_name)
            yield event
            callback(*event.value)

    def on_file_created(self, path, id, tier_id, size, ctime, last_mod, last_access):
        pass

    def on_file_access(self, path, id, tier_id, size, ctime, last_mod, last_access, is_write):
        pass

    def on_disk_occupation_increase(self, path, id, tier_id, size, ctime, last_mod, last_access, prev_size):
        pass