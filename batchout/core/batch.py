from collections import OrderedDict
from itertools import chain, product

from batchout.core.data import Data
from batchout.core.registry import Registry
from batchout.inputs import Input
from batchout.indexes import Index
from batchout.columns import Column
from batchout.outputs import Output


class Batch(object):

    @staticmethod
    def from_config(config):
        return (
            Batch()
            .with_inputs(**config.get('inputs', {}))
            .with_indexes(**config.get('indexes', {}))
            .with_columns(**config.get('columns', {}))
            .with_outputs(**config.get('outputs', {}))
        )

    def __init__(self):
        self._inputs = OrderedDict()
        self._indexes = OrderedDict()
        self._columns = OrderedDict()
        self._outputs = OrderedDict()

    def with_inputs(self, **configs):
        for k, c in configs.items():
            self._inputs[k] = Registry.create(Input, c)
        return self

    def with_indexes(self, **configs):
        for k, c in configs.items():
            self._indexes[k] = Registry.create(Index, c)
        return self

    def with_columns(self, **configs):
        for k, c in configs.items():
            self._columns[k] = Registry.create(Column, c)
        return self

    def with_outputs(self, **configs):
        for k, c in configs.items():
            self._outputs[k] = Registry.create(Output, c)
        return self

    def _build_row(self, payload, indexes):
        return [column.value(payload, **indexes) for column in self._columns.values()]

    def run_once(self):
        data = Data(*self._columns.keys())
        for p in chain.from_iterable(self._inputs.values()):
            indexes = OrderedDict()
            for k, i in self._indexes.items():
                indexes[k] = i.values(p)
            ikeys = list(indexes.keys())
            for ivals in product(*indexes.values()):
                data.with_row(*self._build_row(p, dict(zip(ikeys, ivals))))
        for o in self._outputs.values():
            o.ingest(data)
        for o in self._outputs.values():
            o.commit()
        for i in self._inputs.values():
            i.commit()

    def run_forever(self):
        while True:
            self.run_once()
