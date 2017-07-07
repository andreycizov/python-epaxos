class AbstractCommand:
    def __init__(self, ident):
        self.ident = ident

    def __repr__(self):
        return f'{self.__class__.__name__}({self.ident})'


class EmptyCommand(AbstractCommand):
    def __init__(self):
        super().__init__(-1)


Noop = EmptyCommand()
