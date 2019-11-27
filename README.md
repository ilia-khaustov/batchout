# Batchout

Framework for building data pipelines that: 

1. Extract batch of payloads with hierarchical structure like JSON or XML;
2. Transform batch of payloads to batch of rows for a table defined by columns;
3. Load batch of table rows to persistent storage.

## Install

`pip install batchout`

## Usage

It is better explained by example.

Let's say we periodically fetch a JSON message from some REST API:

```json
{
  "user": {
    "id": "someuserid",
    "last_seen": "2019-11-01T00:00:00"
  },
  "sessions": [
     {
        "id": "somesessionid",
        "created_at": "2019-10-01T00:00:00",
        "device": {
           "useragent": "chrome"
        }
     },
     {
        "id": "othersessionid",
        "created_at": "2019-11-01T00:00:00",
        "device": {
           "useragent": "firefox"
        }
     }
  ]
}
```

Fetched data has to be put into database table `user_session` like this:

```
user_id       user_last_seen        session_id      session_created_at    session_useragent
-------------------------------------------------------------------------------------------
someuserid    2019-11-01T00:00:00   somesessionid   2019-10-01T00:00:00   chrome
someuserid    2019-11-01T00:00:00   othersessionid  2019-11-01T00:00:00   firefox
```

With Batchout, you don't need to write boilerplate code.

Just use `batchout.Batch` for configuring and running your pipeline.

```python
from batchout import Batch


batch = Batch.from_config(
    dict(
        inputs=dict(
            some_api=dict(
                type='http',
                method='get',
                uri='https://some.api/my/user/sessions',
            ),
        ),
        indexes=dict(
            session_idx=dict(
                type='for_list',
                path='sessions',
            )
        ),
        columns=dict(
            user_id=dict(
                cast='string',
                path='user.id',
            ),
            user_last_seen=dict(
                cast='string',
                path='user.last_seen',
            ),
            session_id=dict(
                cast='string',
                path='sessions[{session_idx}].id',  # notice usage of session_idx defined as index above
            ),
            session_created_at=dict(
                cast='timestamp',
                path='sessions[{session_idx}].created_at',
            ),
            session_useragent=dict(
                cast='timestamp',
                path='sessions[{session_idx}].device.useragent',
            ),
        ),
        outputs=dict(
            local=dict(
                type='postgres',
                mode='upsert',
                keys=['user_id', 'session_id'],
                host='localhost',
                port=5432,
                dbname='somedb',
                table='user_session',
                from_env=dict(
                    user='DB_USER',          # DB_USER and
                    password='DB_PASSWORD',  # DB_PASSWORD are read from environment
                ),
            ),
        ),
    ),
    defaults=dict(
        columns=dict(
            type='extracted',
            extractor='jsonpath',
        ),
        indexes=dict(
            extractor='jsonpath',
        ),
    ),
)
batch.run_once()
```

`Batch.run_once()` processes exactly one batch of payloads from each input.

`Batch.run_forever()` does the same in infinite loop.

## Integrations

### Extractors

#### jsonpath

JSON parser built on top of jsonpath_rw.

#### xpath

XML/HTML parser built on top of lxml.

### Inputs

#### http

Request your data via HTTP API using this wrapper for requests.

#### kafka

Simple consumer built on top of kafka-python.

### Outputs

#### postgres

Postgres table writer built on top of psycopg2.
