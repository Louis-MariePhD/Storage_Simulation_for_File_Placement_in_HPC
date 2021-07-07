from policies.policy import Policy
from storage import File, Tier


class DemoPolicy(Policy):
    def __init__(self, storage, env=None):
        Policy.__init__(self, storage, env)

    def on_file_created(self, file: File, tier: Tier):
        print(f'File {file.path} was created in tier {tier.name}')

    def on_file_access(self, file: File, tier: Tier, is_write: bool):
        print(f'There was a {["read", "write"][is_write]} in file {file.path} in tier {tier.name}')

    def on_disk_occupation_increase(self, tier: Tier):
        print(f'We were notified of a tier occupation increase in tier {tier.name}')
