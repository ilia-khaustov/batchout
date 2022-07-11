import os
import logging
from typing import Mapping, Any, Optional, Collection, Callable, TypeVar, Type


logger = logging.getLogger(__name__)


class ConfigFileNotFound(Exception):
    pass


class ConfigFileNoReadAccess(Exception):
    pass


class ConfigInvalid(Exception):
    pass


class EnvironmentVariableMissing(Exception):
    pass


class ChoiceNotSupported(Exception):
    pass


def update_from_env(config: Mapping[str, Any]) -> dict:
    config = dict(config)
    env_vars = config.pop('from_env', {})
    if not isinstance(env_vars, Mapping):
        raise ConfigInvalid('from_env is not a mapping')
    for var_name, env_name in env_vars.items():
        if env_name not in os.environ:
            if not config.get(var_name):
                raise EnvironmentVariableMissing(env_name)
            continue
        config[var_name] = os.environ[env_name]
    return config


T = TypeVar('T')


def with_config_key(
    key: str,
    raise_exc: Optional[Type[Exception]] = None,
    default: Optional[Any] = None,
    default_factory: Optional[Callable[[], Any]] = None,
    doc: Optional[str] = None,
    choices: Optional[Collection[Any]] = None,
) -> Callable[[T], T]:
    def set_from_config(ctx: T, config: Mapping[str, Any]):
        setattr(ctx, f'_{key}', None)
        if key in config:
            val = config[key]
            if choices and val not in choices:
                raise (raise_exc or ChoiceNotSupported)(
                    f'"{val}" is not supported for "{key}"; choose one of: {", ".join(choices)}'
                )
            setattr(ctx, f'_{key}', val)
        elif default is not None or default_factory is not None:
            setattr(ctx, f'_{key}', default_factory() if default is None else default)
        elif raise_exc is not None:
            raise raise_exc(f'{key} is missing')
        if raise_exc is not None and getattr(ctx, f'_{key}') is None:
            raise raise_exc(f'{key} is null')

    def bind(ctx: T) -> T:
        setattr(
            ctx,
            f'spec.{ctx.__name__}',
            getattr(ctx, f'spec.{ctx.__name__}', {}) | {
                key: {
                    'doc': doc,
                    'required': raise_exc is not None,
                    'default': default_factory and default_factory() if default is None else default,
                    'choices': choices and tuple(choices),
                }
            }
        )
        setattr(ctx, 'spec.*', getattr(ctx, 'spec.*', []) + [ctx.__name__])
        setattr(ctx, f'set_{key}', set_from_config)
        setattr(ctx, f'key_{key}', key)
        setattr(ctx, f'doc_{key}', doc)
        if choices:
            for choice in choices:
                setattr(ctx, f'{key}_{choice}', choice)
            setattr(ctx, f'{key}_choices', tuple(choices))
        return ctx
    return bind
