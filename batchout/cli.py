import os
import sys
import logging
import argparse
import importlib.util

try:
    import yaml
except ImportError:
    sys.exit('pyyaml is required for Batchout CLI; have you run `pip install batchout[cli]`?')

from . import Batch


def readable_file(path: str) -> str:
    if os.path.isfile(path) and os.access(path, os.R_OK):
        return path
    else:
        raise argparse.ArgumentTypeError('file not exists or not readable: %s', path)


def available_python_module(name: str) -> str:
    if importlib.util.find_spec(name):
        return name
    else:
        raise argparse.ArgumentTypeError('module not found: %s', name)


def main() -> None:
    argparser = argparse.ArgumentParser(description='Run Batchout from a config file (YAML)')
    argparser.add_argument(
        '-c', '--config', required=True, type=readable_file,
        help='Path to YAML config file',
    )
    argparser.add_argument(
        '-I', '--import-from', type=available_python_module, nargs='*',
        help='Import Python modules containing custom Batchout components',
    )
    argparser.add_argument(
        '-n', '--num-batches', default=-1, type=int,
        help='Stop after N batches (never stop if -1 or less)',
    )
    argparser.add_argument(
        '-w', '--min-wait-sec', default=0, type=int,
        help='Minimum seconds to wait between batches',
    )
    argparser.add_argument(
        '-W', '--max-wait-sec', default=1, type=int,
        help='Maximum seconds to wait between batches',
    )
    argparser.add_argument(
        '-l', '--log-level', default=logging.INFO, type=int,
        help=f"Choose logging level between {logging.DEBUG} (DEBUG) and {logging.FATAL} (FATAL)",
    )
    args = argparser.parse_args()
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().handlers[-1].setFormatter(
        logging.Formatter('%(asctime)s %(levelname)-5s %(name)-30s %(message)s')
    )
    logging.getLogger().setLevel(args.log_level)
    for target in args.import_from:
        importlib.import_module(target)
    config = yaml.load(open(args.config), yaml.Loader)
    defaults = config.pop('defaults') if 'defaults' in config else {}
    batch = Batch.from_config(config, defaults)
    batch.run_forever(max_runs=args.num_batches, min_wait_sec=args.min_wait_sec, max_wait_sec=args.max_wait_sec)
