import logging
from datetime import timedelta

import arrow
import yaml

from batchout import Batch


def main():
    config = yaml.load(open('examples/soundcloud_likes/config.yml'), yaml.Loader)
    defaults = config.pop('defaults') if 'defaults' in config else {}
    same_minute_fails = 0
    while same_minute_fails < 3:
        started_at = arrow.get()
        try:
            Batch.from_config(config, defaults).run_forever(min_wait_sec=5, max_wait_sec=10)
        except Exception as exc:
            if arrow.get() < (started_at + timedelta(minutes=1)):
                same_minute_fails += 1
            else:
                same_minute_fails = 0
            if same_minute_fails >= 3:
                raise exc


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().handlers[-1].setFormatter(
        logging.Formatter('%(asctime)s %(levelname)-5s %(name)-30s %(message)s')
    )
    main()
