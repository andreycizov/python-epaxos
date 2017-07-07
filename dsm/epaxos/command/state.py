class AbstractCommand:
    def __init__(self, ident):
        self.ident = ident

    def __repr__(self):
        return f'{self.__class__.__name__}({self.peer})'
