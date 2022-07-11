from itertools import chain
from operator import itemgetter
from typing import Iterable, TypeVar, Union

T = TypeVar('T')


def as_iter(obj: Union[T, Iterable[T]]) -> Iterable[T]:
    try:
        return iter(obj)
    except TypeError:
        return obj,


def as_list(x: Union[T, Iterable[T]]) -> list[T]:
    return list(as_iter(x))


class Map:

    def __init__(self, deps, *elements):
        root = {}
        branches = [{}]
        for el in elements:
            if isinstance(el, str):
                root[el] = deps
            elif isinstance(el, dict):
                new_deps = [
                    (dep, Map([*deps, dep], *as_list(dep_elements)).context)
                    for dep, dep_elements in el.items()
                ]
                new_branches = []
                for bra in branches:
                    for dep, ctx in new_deps:
                        new_branches.extend([bra | subel | {dep: deps} for subel in ctx])
                branches = new_branches

        self._ctx = [root | b for b in branches]

    @property
    def context(self):
        return list(self._ctx)

    def __iter__(self):
        def depsort(dep_to_deps):
            all_deps = list(chain.from_iterable(dep_to_deps.values()))
            yield from map(
                itemgetter('dep', 'deps'),
                sorted(
                    (
                        {
                            'dep': dep,
                            'deps': deps,
                            'in': len(deps),
                            'out': all_deps.count(dep),
                        }
                        for dep, deps in dep_to_deps.items()
                    ),
                    key=lambda e: e['in']-e['out']
                )
            )
        yield from (depsort(dep_to_deps) for dep_to_deps in self.context)
