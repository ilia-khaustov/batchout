import random

import pytest

from batchout.core.batch import Batch
from batchout.core.registry import Registry
from batchout.outputs import Output


@pytest.fixture
def products_data_gen():
    def gen(n):
        for i in range(n):
            yield {
                'customer': {
                    'id': i,
                },
                'order': {
                    'id': i+1,
                },
                'cart': [
                    {
                        'id': f'cart{i+1+j}',
                        'price': (i+1+j)*10,
                        'name': (' ' * random.randint(0, 5)) + 'foo bar' + (' ' * random.randint(0, 5)),
                    }
                    for j in range(i+1)
                ],
                'model': [
                    {
                        'id': f'model{i+1+j}',
                        'price': (i+1+j)*100,
                        'name': (' ' * random.randint(0, 5)) + 'bar   foo' + (' ' * random.randint(0, 5)),
                    }
                    for j in range(i+1)
                ],
            }
    return gen


@Registry.bind(Output, 'recorder')
class OutputRecorder(Output):

    def __init__(self, _):
        self.data = None

    def ingest(self, data):
        self.data = data

    def commit(self):
        pass


def test_batch_run(products_data_gen):
    d = products_data_gen(3)
    b = Batch.from_config(dict(
        inputs=dict(
            dummy=dict(
                type='dummy',
                data=list(d)
            ),
        ),
        indexes=dict(
            cart_idx=dict(
                type='for_list',
                path='cart'
            ),
            model_idx=dict(
                type='for_list',
                path='model'
            ),
        ),
        columns=dict(
            customer_id=dict(
                type='extracted',
                cast='integer',
                path='customer.id'
            ),
            order_id=dict(
                type='extracted',
                cast='integer',
                path='order.id'
            ),
            cart_product_id=dict(
                type='extracted',
                cast='string',
                path='cart[{cart_idx}].id'
            ),
            cart_product_price=dict(
                type='extracted',
                cast='float',
                path='cart[{cart_idx}].price'
            ),
            cart_product_name=dict(
                type='extracted',
                cast='string',
                path='cart[{cart_idx}].name',
                processors=[
                    dict(type='replace', old=' ', new='')
                ]
            ),
            model_product_id=dict(
                type='extracted',
                cast='string',
                path='model[{model_idx}].id'
            ),
            model_product_price=dict(
                type='extracted',
                cast='float',
                path='model[{model_idx}].price'
            ),
            model_product_name=dict(
                type='extracted',
                cast='string',
                path='model[{model_idx}].name',
                processors=[
                    dict(type='replace', old=' ', new='')
                ]
            ),
        ),
        outputs=dict(
            recorder=dict(
                type='recorder'
            ),
            dummy=dict(
                type='dummy'
            ),
        ),
    ))
    b.run_once()
    recorded = b._outputs['recorder'].data
    assert list(recorded.columns) == [
        'customer_id',
        'order_id',
        'cart_product_id',
        'cart_product_price',
        'cart_product_name',
        'model_product_id',
        'model_product_price',
        'model_product_name',
    ]
    assert list(recorded.rows) == [
        (0, 1, 'cart1', 10., 'foobar', 'model1', 100., 'barfoo'),

        (1, 2, 'cart2', 20., 'foobar', 'model2', 200., 'barfoo'),
        (1, 2, 'cart2', 20., 'foobar', 'model3', 300., 'barfoo'),
        (1, 2, 'cart3', 30., 'foobar', 'model2', 200., 'barfoo'),
        (1, 2, 'cart3', 30., 'foobar', 'model3', 300., 'barfoo'),

        (2, 3, 'cart3', 30., 'foobar', 'model3', 300., 'barfoo'),
        (2, 3, 'cart3', 30., 'foobar', 'model4', 400., 'barfoo'),
        (2, 3, 'cart3', 30., 'foobar', 'model5', 500., 'barfoo'),
        (2, 3, 'cart4', 40., 'foobar', 'model3', 300., 'barfoo'),
        (2, 3, 'cart4', 40., 'foobar', 'model4', 400., 'barfoo'),
        (2, 3, 'cart4', 40., 'foobar', 'model5', 500., 'barfoo'),
        (2, 3, 'cart5', 50., 'foobar', 'model3', 300., 'barfoo'),
        (2, 3, 'cart5', 50., 'foobar', 'model4', 400., 'barfoo'),
        (2, 3, 'cart5', 50., 'foobar', 'model5', 500., 'barfoo'),
    ]
