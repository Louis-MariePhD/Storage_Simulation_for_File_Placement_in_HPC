
class File:
    """Unused class please ignore"""

    def __init__(self, path, id, tier_id, size, ctime, last_mod, last_access):
        self.path = path
        self.file_id = id
        self.tier_id = tier_id
        self.size = size
        self.creation_time = ctime
        self.last_modification = last_mod
        self.last_access = last_access

class Storage:
    """
    A simplistic storage representation. N named storage tier of size S

    We do not emulate read/write speed, and throughput limitations, we consider that there is no concurrent access
    limitations, that a file transfer across tiers is instant, that a file is on a single tier at a given time, etc,
    until some1 gives me some more data.

    As such, the only reliable metric to evaluate a placement politic with such a simplistic design is to count the
    number of reads / write and the time spent on operations (from the input trace) on every tier. A good policy should
    have most reads / write on the performant tier.

    The storage fire simpy events, namely instance.on_file_created_event and instance.on_occupation_increased_event,
    which can be used in our policies

    TODO: better metadata handling using dictionaries ?
    """

    def __init__(self, env, tier_sizes=[], tier_names=[]):
        assert len(tier_names) == len(tier_names)
        self.tier_sizes = tier_sizes
        self.tier_names = tier_names
        self.tier_occupations = [0 for _ in tier_sizes]
        self.tier_contents = [[] for _ in tier_sizes] # "filepath"
        self.tier_contents_metadata = [[] for _ in tier_sizes] # "size", "ctime", "last_mod", "last_access"
        self.env = env

        self.on_file_created_event = env.event()
        self.on_file_access_event = env.event()
        self.on_disk_occupation_increase_event = env.event()

    def create_file(self, path, ctime, size=0, default_tier_id=0):
        """
        Create a file and initialize its metadata. Doesn't check if there is already a file with this name.
        :param path:
        :param ctime:
        :param size:
        :return: a tuple (path, id, tier_id, size, ctime, last_mod, last_access)
        """

        # create file on the default tier
        self.tier_contents[default_tier_id] += [path]
        self.tier_contents_metadata[default_tier_id] += [[size, ctime, ctime, ctime]]

        # trigger event
        file = path, len(self.tier_contents[default_tier_id]) - 1, default_tier_id, size, ctime, ctime, ctime
        e = self.on_file_created_event
        self.on_file_created_event = self.env.event()
        e.succeed(value=file)

        return file

    def sim_read(self, tstart, tend, path, offset, count):
        file = self.get_file_from_path(path)
        if file is None:
            print("[simulation] Reading a file we had no idea of... It seems to have been created before the start "
                  "of the simulation. The simulation will assume it was created at the simulation start time")

            # Artificially creating a file during a read.... This shouldn't occur unless the trace is bad
            file = self.create_file(path, tstart, 0)
            self._update_file_access(*file[1:3], tstart, True)
            self._update_file_size(*file[1:3], offset+count)

        self._update_file_access(*file[1:3], tstart, False)

        return file

    def sim_write(self, tstart, tend, path, offset, count):
        file = self.get_file_from_path(path)
        if file is None:
            file = self.create_file(path, tstart, offset+count)

        self._update_file_access(*file[1:3], tstart, True)
        self._update_file_size(*file[1:4])

        return file

    def _get_file_index(self, path):
        """
        :param path: the path to look for in the memory
        :return: a tuple (file_index, tier_index)
        """
        tier_id = 0
        for tier in self.tier_contents:
            if path in tier:
                index = tier.index(path) # simulation speed will take a hit if there is a lot of different files
                return (index, tier_id)
            tier_id += 1
        return (-1, -1)

    def get_file_from_path(self, path):
        """
        :param path: The path of the file
        :return: a tuple (path, id, tier_id, size, ctime, last_mod, last_access) or None if not found
        """
        id, tier_id = self._get_file_index(path)
        if id >= 0:
            return (path, id, tier_id, *self.tier_contents_metadata[tier_id][id])
        else:
            return None

    def _get_path_from_id(self, file_id, tier_id):
        return self.tier_contents[tier_id][file_id]

    def _update_file_size(self, file_id, tier_id, size):
        """
        If the given size is different than the previous one, update file size metadata. If it was greater than the
        previous one, fire on_disk_occupation_increase_event if disk size augmented
        :param file_id:
        :param tier_id:
        :param size:
        """
        if size is not None:
            prev_size = self.tier_contents_metadata[tier_id][file_id][0]

            if size != prev_size:
                # updates size
                self.tier_contents_metadata[tier_id][file_id][0] = size

                if size-prev_size>0:
                    e = self.on_disk_occupation_increase_event
                    self.on_disk_occupation_increase_event = self.env.event()
                    e.succeed(value=(self._get_path_from_id(file_id, tier_id),
                                     file_id,
                                     tier_id,
                                     *self.tier_contents_metadata[tier_id][id],
                                     prev_size))

    def _update_file_access(self, file_id, tier_id, timestamp, is_write):
        """
        Updates file last_mod / last_access metadata and fire on_file_access_event
        :param file_id:
        :param tier_id:
        :param timestamp:
        :param isWrite:
        """
        if is_write:
            self.tier_contents_metadata[tier_id][file_id][2] = timestamp # last_mod modified
        self.tier_contents_metadata[tier_id][file_id][3] = timestamp # last_access modified

        e = self.on_file_access_event
        self.on_file_access_event = self.env.event()
        e.succeed(value=(self._get_path_from_id(file_id, tier_id),
                         file_id,
                         tier_id,
                         *self.tier_contents_metadata[tier_id][file_id],
                         is_write))