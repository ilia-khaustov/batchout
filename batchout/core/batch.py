import logging
import random
import time
from collections import OrderedDict
from functools import partial
from itertools import chain, product, takewhile, repeat
from operator import is_not, itemgetter

from batchout.core.data import Data
from batchout.core.registry import Registry
from batchout.inputs import Input
from batchout.extractors import Extractor
from batchout.indexes import Index
from batchout.columns import Column
from batchout.outputs import Output
from batchout.selectors import Selector
from batchout.tasks import Task


log = logging.getLogger(__name__)


class ChangedComponentsAfterFirstRun(Exception):
    pass


class UndefinedComponentReference(Exception):
    pass


def _raise_if_called_after_reset(method):
    def wrapped(self, *args, **kwargs):
        if self._reset_cnt > 0:
            raise ChangedComponentsAfterFirstRun(str(kwargs))
        return method(self, *args, **kwargs)
    return wrapped


class Batch(object):

    @staticmethod
    def from_config(config, defaults=None):
        if defaults is None:
            defaults = {
                'columns': {
                    'extractor': 'jsonpath',
                },
                'indexes': {
                    'extractor': 'jsonpath',
                }
            }
        return (
            Batch(defaults)
            .with_inputs(**config.get('inputs', {}))
            .with_extractors(**config.get('extractors', {}))
            .with_indexes(**config.get('indexes', {}))
            .with_columns(**config.get('columns', {}))
            .with_outputs(**config.get('outputs', {}))
            .with_selectors(**config.get('selectors', {}))
            .with_tasks(**config.get('tasks', {}))
        )

    def __init__(self, defaults):
        self._inputs = OrderedDict()
        self._extractors = OrderedDict()
        self._indexes = OrderedDict()
        self._columns = OrderedDict()
        self._outputs = OrderedDict()
        self._selectors = OrderedDict()
        self._tasks = OrderedDict()
        self._defaults = dict(defaults)
        self._index_extractors = dict()
        self._column_extractors = dict()
        self._last_data = None
        self._full_data = None
        self._reset_cnt = 0
        self._validated = False

    def _create_components(self, ctype, plural_alias, current, configs):
        for k, c in configs.items():
            current[k] = Registry.create(ctype, {**self._defaults.get(plural_alias, {}), **c})

    @_raise_if_called_after_reset
    def with_inputs(self, **configs):
        self._create_components(Input, 'inputs', self._inputs, configs)
        return self

    @_raise_if_called_after_reset
    def with_extractors(self, **configs):
        self._create_components(Extractor, 'extractors', self._extractors, configs)
        return self

    @_raise_if_called_after_reset
    def with_indexes(self, **configs):
        self._create_components(Index, 'indexes', self._indexes, configs)
        for name, config in configs.items():
            self._index_extractors[name] = {**self._defaults.get('indexes', {}), **config}['extractor']
        return self

    @_raise_if_called_after_reset
    def with_columns(self, **configs):
        self._create_components(Column, 'columns', self._columns, configs)
        for name, config in configs.items():
            self._column_extractors[name] = {**self._defaults.get('columns', {}), **config}['extractor']
        return self

    @_raise_if_called_after_reset
    def with_outputs(self, **configs):
        self._create_components(Output, 'outputs', self._outputs, configs)
        return self

    @_raise_if_called_after_reset
    def with_selectors(self, **configs):
        self._create_components(Selector, 'selectors', self._selectors, configs)
        return self

    @_raise_if_called_after_reset
    def with_tasks(self, **configs):
        self._create_components(Task, 'tasks', self._tasks, configs)
        return self

    @property
    def readers(self):
        return {k: f.components() for k, f in self._tasks.items() if f.type() is Task.TYPE_READER}

    @property
    def walkers(self):
        return {k: f.components() for k, f in self._tasks.items() if f.type() is Task.TYPE_WALKER}

    @property
    def writers(self):
        return {k: f.components() for k, f in self._tasks.items() if f.type() is Task.TYPE_WRITER}

    def _get_index_extractor(self, index_name):
        return self._extractors[self._index_extractors[index_name]]

    def _get_column_extractor(self, column_name):
        return self._extractors[self._column_extractors[column_name]]

    @property
    def last(self):
        self._last_data = self._last_data or self._init_data()
        return self._last_data

    @property
    def full(self):
        self._full_data = self._full_data or self._init_data()
        return self._full_data

    def _reset_last(self):
        self._last_data.reset().with_sources(*self._inputs.keys())
        self._reset_cnt += 1

    def _log(self, msg, *args, __level=logging.INFO, **kwargs):
        log.log(__level, f'R{self._reset_cnt:03}: {msg}', *args, **kwargs)

    def _init_data(self):
        col_types = {k: c.bound_name for k, c in self._columns.items()}
        return Data(*self._columns.keys(), **col_types).with_sources(*self._inputs.keys())

    def _validate_components(self):
        if self._validated and self._reset_cnt > 0:
            return
        for task_name, task in self._tasks.items():
            task_components = task.components()
            if task_components.get('selector') is not None and task_components['selector'] not in self._selectors:
                raise UndefinedComponentReference(
                    f"task {task_name} references selector {task_components['selector']} which is undefined"
                )
            for index in (task_components.get('indexes') or []):
                if index not in self._indexes:
                    raise UndefinedComponentReference(
                        f"task {task_name} references index {index} which is undefined"
                    )
            for column in (task_components.get('columns') or []):
                if column not in self._columns:
                    raise UndefinedComponentReference(
                        f"task {task_name} references column {column} which is undefined"
                    )
            for _input in (task_components.get('inputs') or []):
                if _input not in self._inputs:
                    raise UndefinedComponentReference(
                        f"task {task_name} references input {_input} which is undefined"
                    )
            for output in (task_components.get('outputs') or []):
                if output not in self._outputs:
                    raise UndefinedComponentReference(
                        f"task {task_name} references output {output} which is undefined"
                    )
        for index, extractor in self._index_extractors.items():
            if extractor not in self._extractors:
                raise UndefinedComponentReference(
                    f"index {index} references extractor {extractor} which is undefined"
                )
        for column, extractor in self._column_extractors.items():
            if extractor not in self._extractors:
                raise UndefinedComponentReference(
                    f"column {column} references extractor {extractor} which is undefined"
                )
        self._validated = True

    def run_once(self):
        self._validate_components()

        read_selectors = [v['selector'] for v in self.readers.values()]
        write_selectors = [v['selector'] for v in self.writers.values()]

        selections_to_read = self._prepare_selections(self.last, *read_selectors)
        self._reset_last()
        
        for reader_name, reader_components in self.readers.items():
            read_inputs, using_selector = reader_components['inputs'], reader_components['selector']

            pkeys, pvals_set = selections_to_read[using_selector] if using_selector else ([], [[]])
            if pkeys and pvals_set:
                self._log(f"{reader_name}: fetching {len(pvals_set)} tuples of "
                          f"({','.join(pkeys)}) from {', '.join(read_inputs)}")
            else:
                self._log(f"{reader_name}: fetching from {', '.join(read_inputs)}")

            read_total = 0
            for idx, params in enumerate((dict(zip(pkeys, pvals)) for pvals in pvals_set or [[]])):
                read_data = self._init_data()
                for reading_input, payload in self._read_inputs(params, *read_inputs):
                    for row_cols, idx_vals in self._walk(payload, reading_input):
                        row = self._build_row(payload, row_cols, idx_vals)
                        if any(filter(None, row)):
                            read_data.with_row(reading_input, row)
                            self.last.with_row(reading_input, row)
                            self.full.with_row(reading_input, row)
                            read_total += 1

                self._log(
                    f"{reader_name}[{idx+1:04}/{len(pvals_set) or 1:04}]: "
                    f"{''.join(f'({k}={v}) ' for k, v in params.items())}"
                    f"read {len(read_data)} records out of {read_total}"
                )

                selections_to_write = self._prepare_selections(read_data, *write_selectors)
                self._write_outputs(selections_to_write)

                for read_input in read_inputs:
                    self._inputs[read_input].commit()
                    self._inputs[read_input].reset()

        return self

    def _prepare_selections(self, data: Data, *names):
        return {
            name: (selector.columns(), [row for row in selector.apply(data) if any(filter(None, row))])
            for name, selector in self._selectors.items()
            if name in names
        }

    def _read_inputs(self, params, *names):
        yield from chain.from_iterable(
            zip(
                repeat(name),
                takewhile(partial(is_not, None), (i.fetch(**(params or {})) for params in repeat(params)))
            )
            for name, i in self._inputs.items()
            if name in names
        )

    def _walk(self, payload, from_input):
        for walker_name, walker_components in self.walkers.items():
            walk_inputs, walk_indexes, walk_columns = (walker_components[c] for c in ('inputs', 'indexes', 'columns'))
            if from_input not in walk_inputs:
                continue
            indexes = {
                walk_index: self._indexes[walk_index].values(self._get_index_extractor(walk_index), payload)
                for walk_index in walk_indexes
            }
            keys, values_set = tuple(zip(*filter(itemgetter(1), indexes.items()))) or (tuple(), ([{}],))
            yield from ((tuple(walk_columns), dict(zip(keys, vals))) for vals in product(*values_set))

    def _build_row(self, payload, cols, indexes):
        return [
            col.value(self._get_column_extractor(name), payload, **indexes) if name in cols
            else None
            for name, col in self._columns.items()
        ]

    def _write_outputs(self, selections_to_write):
        for writer_name, writer_components in self.writers.items():
            write_outputs, from_selector = writer_components['outputs'], writer_components['selector']
            cols, rows = selections_to_write[from_selector]
            if not rows:
                continue
            for write_output in write_outputs:
                written_cnt = self._outputs[write_output].ingest(cols, rows)
                self._outputs[write_output].commit()
                self._log(f'{writer_name}: written {written_cnt} records from {from_selector} into {write_output}')

    def run_forever(self, min_wait_sec=0, max_wait_sec=1):
        while True:
            self.run_once()
            time.sleep(min_wait_sec + random.random() * max_wait_sec)
