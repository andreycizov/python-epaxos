class AbstractCommand:
    def __init__(self, ident):
        self.ident = ident

    def __repr__(self):
        return f'{self.__class__.__name__}({self.ident})'

    def tuple(self):
        return self.ident,

    def __lt__(self, other):
        return self.tuple() < other.tuple()


class EmptyCommand(AbstractCommand):
    def __init__(self):
        super().__init__(-1)


Noop = EmptyCommand()
