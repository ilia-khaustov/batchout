import logging

import yaml

from batchout.core.batch import Batch


def main():
    config = yaml.load(open('config.yml'), yaml.Loader)
    defaults = yaml.load(open('defaults.yml'), yaml.Loader)
    b = Batch.from_config(config, defaults)
    b.run_forever()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().handlers[-1].setFormatter(
        logging.Formatter('%(asctime)s %(levelname)-5s %(name)-30s %(message)s')
    )
    main()
