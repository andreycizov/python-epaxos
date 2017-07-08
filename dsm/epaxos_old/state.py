class Command:
    def __init__(self, id):
        self.id = id

    def __repr__(self):
        return f'{self.__class__.__name__}({self.id})'


class Slot:
    def __init__(self, replica_id, instance_id):
        self.replica_id = replica_id
        self.instance_id = instance_id

    def ballot(self, epoch):
        return Ballot(epoch, 0, self.replica_id)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.replica_id},{self.instance_id})'


class Ballot:
    def __init__(self, epoch, b, replica_id):
        self.epoch = epoch
        self.b = b
        self.replica_id = replica_id

    def next(self):
        return Ballot(self.epoch, self.b + 1, self.replica_id)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.epoch},{self.b},{self.replica_id})'

    def tuple(self):
        return self.epoch, self.b, self.replica_id

    def __lt__(self, other):
        return self.tuple() < other.tuple()


