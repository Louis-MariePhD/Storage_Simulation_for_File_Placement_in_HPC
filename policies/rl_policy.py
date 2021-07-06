from policies.policy import Policy


class RLPolicy(Policy):
    def __init__(self, storage, env=None):
        Policy.__init__(self, storage, env)

    def on_file_created(self, path, file_id, tier_id, size, ctime, last_mod, last_access):
        print(f'File {path} was created in tier {self.storage.tier_names[tier_id]}')

    def on_file_access(self, path, file_id, tier_id, size, ctime, last_mod, last_access, is_write):
        print(f'There was a {["read", "write"][is_write]} in file {path} in tier {self.storage.tier_names[tier_id]}')

    def on_disk_occupation_increase(self, path, file_id, tier_id, size, ctime, last_mod, last_access, prev_size):
        print(f'A write in file {path} informed us of an occupation increase in tier {self.storage.tier_names[tier_id]}')
