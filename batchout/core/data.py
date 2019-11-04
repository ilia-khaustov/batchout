

class Data(object):

    def __init__(self, *columns):
        self._columns = columns
        self._rows = []
        self._len = 0

    @property
    def columns(self):
        return self._columns

    def with_row(self, *values):
        self._rows.append(values[:len(self.columns)])
        self._len += 1

    @property
    def rows(self):
        return self._rows

    def __len__(self):
        return self._len
