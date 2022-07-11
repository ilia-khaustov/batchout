import json
import logging
import random
from typing import Any
import os.path

import pytest

from batchout import Batch, Output
from batchout.core.registry import Registry
from batchout.core.config import with_config_key

tests_dir = os.path.dirname(__file__)


@pytest.fixture(scope='session', autouse=True)
def configure_logging():
    logging.getLogger().setLevel(logging.ERROR)
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().handlers[-1].setFormatter(
        logging.Formatter('\n%(asctime)s %(levelname)-5s %(name)-30s %(message)s')
    )


@Registry.bind(Output, 'recorder')
class OutputRecorder(Output):

    def __init__(self, _):
        self.cols = None
        self.rows = None

    def ingest(self, cols, rows):
        self.cols = cols
        self.rows = list(map(tuple, rows))

    def commit(self):
        pass


@with_config_key('rows')
@with_config_key('columns')
@Registry.bind(Output, 'assert')
class OutputAssert(Output):

    def __init__(self, config: dict[str, Any]):
        self.set_columns(config)
        self.set_rows(config)

    def ingest(self, cols, rows):
        assert list(self._columns) == list(cols)
        assert list(map(tuple, self._rows)) == list(map(tuple, rows))

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
            json_orders=dict(
                type='const',
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
        extractors=dict(
            first_match_in_json=dict(
                type='jsonpath',
            ),
        ),
        columns=dict(
            customer_id=dict(
                type='integer',
                path='customer.id'
            ),
            order_id=dict(
                type='integer',
                path='order.id'
            ),
            cart_product_id=dict(
                type='string',
                path='cart[{cart_idx}].id'
            ),
            cart_product_price=dict(
                type='float',
                path='cart[{cart_idx}].price'
            ),
            cart_product_name=dict(
                type='string',
                path='cart[{cart_idx}].name',
                processors=[
                    dict(type='replace', old=' ', new='')
                ]
            ),
            model_product_id=dict(
                type='string',
                path='model[{model_idx}].id'
            ),
            model_product_price=dict(
                type='float',
                path='model[{model_idx}].price'
            ),
            model_product_name=dict(
                type='string',
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
            logger=dict(
                type='logger'
            ),
        ),
        selectors=dict(
            all=dict(
                type='sql',
                query='select * from json_orders',
                columns=[
                    'customer_id',
                    'order_id',
                    'cart_product_id',
                    'cart_product_price',
                    'cart_product_name',
                    'model_product_id',
                    'model_product_price',
                    'model_product_name',
                ],
            ),
        ),
        maps=dict(
            json_orders=[
                'customer_id',
                'order_id',
                dict(model_idx=[
                    'model_product_id',
                    'model_product_price',
                    'model_product_name',
                ]),
                dict(cart_idx=[
                    'cart_product_id',
                    'cart_product_price',
                    'cart_product_name',
                ])
            ]
        ),
        tasks=dict(
            read_orders=dict(
                type='reader',
                inputs=['json_orders'],
            ),
            record_and_log=dict(
                type='writer',
                selector='all',
                outputs=['recorder', 'logger'],
            ),
        )
    ), defaults={
        'columns': {
            'extractor': 'first_match_in_json',
        },
        'indexes': {
            'extractor': 'first_match_in_json',
        }
    })
    b.run_once()
    recorded_cols, recorded_rows = b._outputs['recorder'].cols, b._outputs['recorder'].rows
    assert list(recorded_cols) == [
        'customer_id',
        'order_id',
        'cart_product_id',
        'cart_product_price',
        'cart_product_name',
        'model_product_id',
        'model_product_price',
        'model_product_name',
    ]
    assert set(recorded_rows) == {
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
    }


@pytest.fixture
def xml_orders():

    def foobar(foo, bar):
        def oo(n): return " " * random.randint(0, n)
        return oo(5) + foo + oo(5) + bar + oo(5)

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
            xml_orders=dict(
                type='const',
                data=list(xml_orders(3))
            ),
        ),
        extractors=dict(
            first_match_in_xml=dict(
                type='xpath',
            ),
            all_matches_in_xml=dict(
                type='xpath',
                strategy='take_all',
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
                type='integer',
                path='/order/customer/@id'
            ),
            order_id=dict(
                type='integer',
                path='/order/@id'
            ),
            cart_product_id=dict(
                type='string',
                path='/order/cart/product[{cart_idx}]/@id'
            ),
            cart_product_price=dict(
                type='float',
                path='/order/cart/product[{cart_idx}]/price/text()'
            ),
            cart_product_name=dict(
                type='string',
                path='/order/cart/product[{cart_idx}]/name/text()',
                processors=[
                    dict(type='replace', old=' ', new='')
                ]
            ),
            model_product_id=dict(
                type='string',
                path='/order/model/product[{model_idx}]/@id'
            ),
            model_product_price=dict(
                type='float',
                path='/order/model/product[{model_idx}]/price/text()',
            ),
            model_product_name=dict(
                type='string',
                path='/order/model/product[{model_idx}]/name/text()',
                processors=[
                    dict(type='replace', old=' ', new='')
                ]
            ),
        ),
        maps=dict(
            xml_orders=[
                'customer_id',
                'order_id',
                dict(model_idx=[
                    'model_product_id',
                    'model_product_price',
                    'model_product_name',
                ]),
                dict(cart_idx=[
                    'cart_product_id',
                    'cart_product_price',
                    'cart_product_name',
                ]),
            ]
        ),
        outputs=dict(
            recorder=dict(
                type='recorder'
            ),
            logger=dict(
                type='logger'
            ),
        ),
        selectors=dict(
            all=dict(
                type='sql',
                query='select * from xml_orders',
                columns=[
                    'customer_id',
                    'order_id',
                    'cart_product_id',
                    'cart_product_price',
                    'cart_product_name',
                    'model_product_id',
                    'model_product_price',
                    'model_product_name',
                ],
            )
        ),
        tasks=dict(
            read_orders=dict(
                type='reader',
                inputs=['xml_orders'],
            ),
            record_and_log=dict(
                type='writer',
                selector='all',
                outputs=['recorder', 'logger'],
            ),
        )
    ), defaults={
        'columns': {
            'extractor': 'first_match_in_xml',
        },
        'indexes': {
            'extractor': 'all_matches_in_xml',
        }
    })
    b.run_once()
    recorded_cols, recorded_rows = b._outputs['recorder'].cols, b._outputs['recorder'].rows
    assert list(recorded_cols) == [
        'customer_id',
        'order_id',
        'cart_product_id',
        'cart_product_price',
        'cart_product_name',
        'model_product_id',
        'model_product_price',
        'model_product_name',
    ]
    assert set(recorded_rows) == {
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
    }


def test_run_for_nested_indexing():
    config_path = os.path.join(tests_dir, 'config/nested_indexing.json')
    with open(config_path) as f:
        config = json.loads(f.read())
    defaults = config.pop('defaults') if 'defaults' in config else {}
    Batch.from_config(config, defaults).run_once()
