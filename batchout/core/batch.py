from __future__ import annotations

import itertools
import logging
import random
import time
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from itertools import chain, takewhile, repeat
from operator import is_not
from typing import Optional

from .data import Data
from .registry import Registry
from .util import as_list, Map
from ..std import (
    Input,
    Extractor,
    Index,
    Column,
    Output,
    Selector,
    Task,
)


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


class Batch:

    @staticmethod
    def from_config(config: dict, defaults: Optional[dict] = None) -> Batch:
        return (
            Batch(defaults or {})
            .with_inputs(**config.get('inputs', {}))
            .with_extractors(**config.get('extractors', {}))
            .with_indexes(**config.get('indexes', {}))
            .with_columns(**config.get('columns', {}))
            .with_outputs(**config.get('outputs', {}))
            .with_selectors(**config.get('selectors', {}))
            .with_tasks(**config.get('tasks', {}))
            .with_maps(**config.get('maps', {}))
        )

    def __init__(self, defaults):
        self._inputs = OrderedDict()
        self._extractors = OrderedDict()
        self._indexes = OrderedDict()
        self._columns = OrderedDict()
        self._outputs = OrderedDict()
        self._selectors = OrderedDict()
        self._tasks = OrderedDict()
        self._maps = dict()
        self._defaults = dict(defaults)
        self._index_extractors = dict()
        self._column_extractors = dict()
        self._last_data = None
        self._full_data = None
        self._reset_cnt = 0
        self._validated = False
        self._input_configs = {}

    def _create_components(self, ctype, current, configs):
        for k, c in configs.items():
            current[k] = Registry.create(ctype, {**self._defaults.get(ctype.PLURAL_ALIAS, {}), **c})

    @_raise_if_called_after_reset
    def with_inputs(self, **configs):
        self._create_components(Input, self._inputs, configs)
        self._input_configs.update(configs)
        return self

    @_raise_if_called_after_reset
    def with_extractors(self, **configs):
        self._create_components(Extractor, self._extractors, configs)
        return self

    @_raise_if_called_after_reset
    def with_indexes(self, **configs):
        self._create_components(Index, self._indexes, configs)
        for name, config in configs.items():
            self._index_extractors[name] = {**self._defaults.get('indexes', {}), **config}['extractor']
        return self

    @_raise_if_called_after_reset
    def with_columns(self, **configs):
        self._create_components(Column, self._columns, configs)
        for name, config in configs.items():
            self._column_extractors[name] = {**self._defaults.get('columns', {}), **config}['extractor']
        return self

    @_raise_if_called_after_reset
    def with_outputs(self, **configs):
        self._create_components(Output, self._outputs, configs)
        return self

    @_raise_if_called_after_reset
    def with_selectors(self, **configs):
        self._create_components(Selector, self._selectors, configs)
        return self

    @_raise_if_called_after_reset
    def with_tasks(self, **configs):
        self._create_components(Task, self._tasks, configs)
        return self

    @_raise_if_called_after_reset
    def with_maps(self, **configs):
        self._maps = {iname: Map([], *as_list(elements)) for iname, elements in configs.items()}
        return self

    @property
    def readers(self):
        return OrderedDict(sorted(
            [(k, f.components()) for k, f in self._tasks.items() if f.type() is Task.TYPE_READER],
            key=lambda c: c[1]['selector'] or '',
        ))

    @property
    def writers(self):
        return {k: f.components() for k, f in self._tasks.items() if f.type() is Task.TYPE_WRITER}

    def _get_index_extractor(self, index_name):
        return self._extractors[self._index_extractors[index_name]]

    def _get_column_extractor(self, column_name):
        return self._extractors[self._column_extractors[column_name]]

    def _clone_inputs(self, *input_names):
        cloned_inputs = {}
        self._create_components(
            Input, cloned_inputs,
            {name: dict(config) for name, config in self._input_configs.items() if name in input_names}
        )
        return cloned_inputs

    @property
    def last(self):
        self._last_data = self._last_data or self._init_data()
        return self._last_data

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

        read_selectors = [v['selector'] for v in list(self.readers.values()) if v['selector']]
        write_selectors = [v['selector'] for v in self.writers.values() if v['selector']]

        selections_to_read = self._prepare_selections(self.last, *read_selectors)
        self._reset_last()

        for reader_name, reader_components in self.readers.items():
            read_inputs, using_selector, max_threads = (
                reader_components['inputs'], reader_components['selector'], reader_components['threads']
            )
            if using_selector and self.last.count() > 0:
                selections_to_read.update(self._prepare_selections(self.last, using_selector))

            pkeys, pvals_set = selections_to_read[using_selector] if using_selector else ([], [[]])
            if pkeys and pvals_set:
                self._log(f"{reader_name}: fetching {len(pvals_set)} tuples of "
                          f"({','.join(pkeys)}) from {', '.join(read_inputs)}")
            else:
                self._log(f"{reader_name}: fetching from {', '.join(read_inputs)}")

            self._read_all(reader_name, read_inputs, max_threads, pkeys, pvals_set)

            for read_input in read_inputs:
                self._inputs[read_input].reset()

        selections_to_write = self._prepare_selections(self.last, *write_selectors)
        self._write_outputs(selections_to_write)

        for each_input in tuple(self._inputs.values()):
            each_input.commit()

        return self

    def _prepare_selections(self, data: Data, *names):
        return {
            name: (selector.columns(), [row for row in selector.apply(data) if any(filter(None, row))])
            for name, selector in self._selectors.items()
            if name in names
        }

    def _read_all(self, reader_name, read_inputs, max_threads, pkeys, pvals_set):
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            fetch_tasks = [
                executor.submit(self._read_one, read_inputs, params)
                for idx, params
                in enumerate((
                    dict(zip(pkeys, pvals))
                    for pvals
                    in pvals_set or [[]]
                ))
            ]
            for idx, fut in enumerate(as_completed(fetch_tasks)):
                params, rows_set = fut.result()
                for source, rows in zip(read_inputs, rows_set):
                    self.last.with_row(source, *rows)
                    self._log(
                        f"{reader_name}[{idx + 1:04}/{len(fetch_tasks):04}]: "
                        f"{''.join(f'({k}={v}) ' for k, v in params.items())}"
                        f"read {len(rows)} records from {source}, new total is {self.last.count(source)}"
                    )

    def _read_one(self, read_inputs, params):
        cloned_inputs = self._clone_inputs(*read_inputs)
        latest = self._init_data()
        for reading_input, payload in self._fetch_from_inputs(params, **cloned_inputs):
            for row_cols, idx_vals in self._parse(payload, reading_input):
                row = self._build_row(payload, row_cols, idx_vals)
                if any(filter(None, row)):
                    latest.with_row(reading_input, row)
        for cloned_input in cloned_inputs.values():
            cloned_input.commit()
        return params, [latest.rows(src) for src in read_inputs]

    @staticmethod
    def _fetch_from_inputs(params, **inputs):
        yield from chain.from_iterable(
            zip(
                repeat(name),
                takewhile(partial(is_not, None), (i.fetch(**(params or {})) for params in repeat(params)))
            )
            for name, i in inputs.items()
        )

    def _parse(self, payload, from_input):
        if from_input not in self._maps:
            return
        for branch in self._maps[from_input]:
            context = defaultdict(dict)
            columns = []
            for path, deps in branch:
                if path in self._indexes:
                    if not deps:
                        context[path] = {
                            v: defaultdict(dict)
                            for v in self._indexes[path].values(self._get_index_extractor(path), payload)
                        }
                        continue
                    for indexes in self._build_indexes(context):
                        first_dep, *other_deps = deps
                        outer = context[first_dep]
                        inner = outer[indexes[first_dep]]
                        for last_dep in other_deps:
                            outer = inner.setdefault(last_dep, defaultdict(dict))
                            inner = outer.setdefault(indexes[last_dep], defaultdict(dict))
                        inner[path] = {
                            v: defaultdict(dict)
                            for v in self._indexes[path].values(self._get_index_extractor(path), payload, **indexes)
                        }
                elif path in self._columns:
                    columns.append(path)
            if not context:
                yield tuple(columns), {}
            else:
                yield from zip(repeat(tuple(columns)), self._build_indexes(context))

    @staticmethod
    def _build_indexes(context):
        if not context:
            yield {}
        all_vals = []
        all_deps = []
        for dep, dep_ctx in context.items():
            all_vals.append(list(dep_ctx.keys()))
            all_deps.append(dep)
        for combo in itertools.product(*all_vals):
            res = dict(zip(all_deps, combo))
            for dep, val in res.items():
                val_ctx = context[dep][val]
                for kv in Batch._build_indexes(val_ctx):
                    yield {**res, **kv}

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
                self._log(f'{writer_name}: written {written_cnt} records from {from_selector} into {write_output}')
            for write_output in write_outputs:
                self._outputs[write_output].commit()

    def run_forever(self, max_runs=-1, min_wait_sec=0, max_wait_sec=1):
        while True:
            if max_runs == 0:
                break
            self.run_once()
            max_runs = max(max_runs - 1, -1)
            time.sleep(max(0.0, min_wait_sec + random.random() * max_wait_sec))
