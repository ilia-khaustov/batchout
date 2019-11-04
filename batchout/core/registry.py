from typing import ClassVar, Dict, Any

from batchout.core.config import ConfigInvalid, update_from_env


class ClassInvalid(Exception):
    pass


class ClassAlreadyBound(Exception):
    pass


class Registry(object):

    BOUND = {}

    @staticmethod
    def bind(cls: ClassVar, name: str):
        def do_bind(subcls):
            if not issubclass(subcls, cls):
                raise ClassInvalid(f'{subcls.__name__} is not a subclass of {cls.__name__}')
            if name in Registry.BOUND[cls]:
                bound_cls = Registry.BOUND[cls][name]
                raise ClassAlreadyBound(f'type {name} of {cls.__name__} is already bound to {bound_cls.__name__}')
            Registry.BOUND[cls][name] = subcls
            setattr(subcls, 'bound_name', name)
            return subcls
        Registry.BOUND.setdefault(cls, {})
        return do_bind

    @staticmethod
    def create(cls: ClassVar, config: Dict[str, Any]):
        if 'type' not in config:
            raise ConfigInvalid(f'type of {cls.__name__} is not specified')
        Registry.BOUND.setdefault(cls, {})
        subcls = Registry.BOUND[cls].get(config['type'])
        if not subcls:
            raise ConfigInvalid(f'type {config["type"]} of {cls.__name__} is not bound to any class')
        return subcls(update_from_env(config))
