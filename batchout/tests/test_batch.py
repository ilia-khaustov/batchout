import json
import random

import pytest

from batchout.core.batch import Batch
from batchout.core.registry import Registry
from batchout.outputs import Output


@Registry.bind(Output, 'recorder')
class OutputRecorder(Output):

    def __init__(self, _):
        self.data = None

    def ingest(self, data):
        self.data = data

    def commit(self):
        pass


@pytest.fixture
def json_orders():
    def g(n):
        for i in range(n):
            yield json.dumps({
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
            })
    return g


def test_run_for_json(json_orders):
    b = Batch.from_config(dict(
        inputs=dict(
            dummy=dict(
                type='dummy',
                data=list(json_orders(3))
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
                cast='integer',
                path='customer.id'
            ),
            order_id=dict(
                cast='integer',
                path='order.id'
            ),
            cart_product_id=dict(
                cast='string',
                path='cart[{cart_idx}].id'
            ),
            cart_product_price=dict(
                cast='float',
                path='cart[{cart_idx}].price'
            ),
            cart_product_name=dict(
                cast='string',
                path='cart[{cart_idx}].name',
                processors=[
                    dict(type='replace', old=' ', new='')
                ]
            ),
            model_product_id=dict(
                cast='string',
                path='model[{model_idx}].id'
            ),
            model_product_price=dict(
                cast='float',
                path='model[{model_idx}].price'
            ),
            model_product_name=dict(
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
    ), defaults={
        'columns': {
            'type': 'extracted',
            'extractor': 'jsonpath',
            'strategy': 'take_first',
        },
        'indexes': {
            'extractor': 'jsonpath',
            'strategy': 'take_first',
        }
    })
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


@pytest.fixture
def xml_orders():

    def foobar(foo, bar):
        def ooba(n): return " " * random.randint(0, n)
        return ooba(5) + foo + ooba(5) + bar + ooba(5)

    def g(n):
        for i in range(n):
            cart_products = '\n'.join(f'<product id="cart{i+1+j}">'
                                      f'<price>{(i+1+j)*10.}</price>'
                                      f'<name>{foobar("foo", "bar")}</name>'
                                      f'</product>' for j in range(i+1))

            model_products = '\n'.join(f'<product id="model{i+1+j}">'
                                       f'<price>{(i+1+j)*100.}</price>'
                                       f'<name>{foobar("bar", "foo")}</name>'
                                       f'</product>' for j in range(i+1))
            yield f'''
                <order id="{i+1}">
                    <customer id="{i}" />
                    <cart>{cart_products}</cart>
                    <model>{model_products}</model>
                </order>
            '''
    return g


def test_run_for_xml(xml_orders):
    b = Batch.from_config(dict(
        inputs=dict(
            dummy=dict(
                type='dummy',
                data=list(xml_orders(3))
            ),
        ),
        indexes=dict(
            cart_idx=dict(
                type='for_list',
                path='/order/cart/product'
            ),
            model_idx=dict(
                type='for_list',
                path='/order/model/product'
            ),
        ),
        columns=dict(
            customer_id=dict(
                cast='integer',
                path='/order/customer/@id'
            ),
            order_id=dict(
                cast='integer',
                path='/order/@id'
            ),
            cart_product_id=dict(
                cast='string',
                path='/order/cart/product[{cart_idx}]/@id'
            ),
            cart_product_price=dict(
                cast='float',
                path='/order/cart/product[{cart_idx}]/price/text()'
            ),
            cart_product_name=dict(
                cast='string',
                path='/order/cart/product[{cart_idx}]/name/text()',
                processors=[
                    dict(type='replace', old=' ', new='')
                ]
            ),
            model_product_id=dict(
                cast='string',
                path='/order/model/product[{model_idx}]/@id'
            ),
            model_product_price=dict(
                cast='float',
                path='/order/model/product[{model_idx}]/price/text()',
            ),
            model_product_name=dict(
                cast='string',
                path='/order/model/product[{model_idx}]/name/text()',
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
    ), defaults={
        'columns': {
            'type': 'extracted',
            'extractor': 'xpath',
            'strategy': 'take_first',
        },
        'indexes': {
            'extractor': 'xpath',
            'strategy': 'take_all',
        }
    })
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
