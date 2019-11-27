import logging

import kafka

from batchout.core.util import to_iter
from batchout.core.config import with_config_key
from batchout.core.registry import Registry
from batchout.inputs import Input


log = logging.getLogger(__name__)


class KafkaInputConfigInvalid(Exception):
    pass


@with_config_key('bootstrap_servers', raise_exc=KafkaInputConfigInvalid)
@with_config_key('consumer_group', raise_exc=KafkaInputConfigInvalid)
@with_config_key('topic', raise_exc=KafkaInputConfigInvalid)
@with_config_key('timeout_sec', default=1)
@with_config_key('max_batch_size', default=-1)
@Registry.bind(Input, 'kafka')
class KafkaInput(Input):

    def __init__(self, config):
        self.set_bootstrap_servers(config)
        self.set_consumer_group(config)
        self.set_topic(config)
        self._bootstrap_servers = list(to_iter(self._bootstrap_servers))
        self.set_timeout_sec(config)
        if not isinstance(self._timeout_sec, int):
            raise KafkaInputConfigInvalid('integer expected for timeout_sec')
        self.set_max_batch_size(config)
        if not isinstance(self._max_batch_size, int):
            raise KafkaInputConfigInvalid('integer expected for max_batch_size')
        self._batch_size = 0
        self._consumer = None

    @property
    def consumer(self):
        if self._consumer is None:
            self._consumer = kafka.KafkaConsumer(self._topic,
                                                 bootstrap_servers=self._bootstrap_servers,
                                                 group_id=self._consumer_group,
                                                 consumer_timeout_ms=self._timeout_sec*1000,
                                                 enable_auto_commit=False)
        return self._consumer

    def fetch(self):
        if 0 <= self._max_batch_size <= self._batch_size:
            log.info(f'Fetch stops on batch_size={self._batch_size}')
            return
        message = next(self.consumer, None)
        if message:
            self._batch_size += 1
            return message.value
        else:
            log.info(f'Fetch timeout on batch_size={self._batch_size}')

    def commit(self):
        if self._consumer is None:
            return
        log.info(f'Input commits batch_size={self._batch_size}')
        self._consumer.commit()
        self._consumer.close()
        self._consumer = None
        self._batch_size = 0

    def reset(self):
        self._consumer.close()
        self._consumer = None
        self._batch_size = 0
