import os
from typing import Dict, Any
from itertools import chain

from batchout.core.util import to_iter


class ConfigFileNotFound(Exception):
    pass


class ConfigFileNoReadAccess(Exception):
    pass


class ConfigInvalid(Exception):
    pass


class EnvironmentVariableMissing(Exception):
    pass


def update_from_env(config):
    config = dict(config)
    env_vars = config.pop('from_env', {})
    if not isinstance(env_vars, dict):
        raise ConfigInvalid('from_env is not a mapping')
    for var_name, env_name in env_vars.items():
        if env_name not in os.environ:
            raise EnvironmentVariableMissing(env_name)
        config[var_name] = os.environ[env_name]
    return config


def with_config_key(key, raise_exc=None, default=None, **sets):
    def set_from_config(ctx, config: Dict[str, Any]):
        setattr(ctx, f'_{key}', None)
        if key in config:
            val = config[key]
            if all_choices and val not in all_choices:
                raise (raise_exc or ValueError)(f'invalid {key}: {val}')
            setattr(ctx, f'_{key}', val)
        elif default is not None:
            setattr(ctx, f'_{key}', default)
        elif raise_exc is not None:
            raise raise_exc(f'{key} is missing')
        if raise_exc is not None and getattr(ctx, f'_{key}') is None:
            raise raise_exc(f'{key} is null')

    def bind(ctx):
        setattr(ctx, f'set_{key}', set_from_config)
        setattr(ctx, f'key_{key}', key)
        for set_name, set_elements in sets.items():
            for v in set_elements:
                setattr(ctx, f'{key}_{v}', v)
            setattr(ctx, set_name, tuple(set_elements))
        return ctx

    all_choices = set(chain.from_iterable(map(to_iter, sets.values())))
    return bind
