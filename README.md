# Batchout

Framework for building data pipelines that: 

1. Extract batch of payloads with hierarchical structure like JSON or XML;
2. Transform batch of payloads to batch of rows for a table defined by columns;
3. Load batch of table rows to persistent storage.

## Install

`pip install batchout`

## Usage

It is better explained by example.

Let's say we encounter a stream of JSON messages coming from Kafka topic `auth.sessions`:

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

Stream data has to be put into database table `user_session` like this:

```
user_id       user_last_seen        session_id      session_created_at    session_useragent
-------------------------------------------------------------------------------------------
someuserid    2019-11-01T00:00:00   somesessionid   2019-10-01T00:00:00   chrome
someuserid    2019-11-01T00:00:00   othersessionid  2019-11-01T00:00:00   firefox
```

With Batchout, you don't need to write boilerplate code.

Just use `batchout.core.Batch` for configuring and running your pipeline.

```python
from batchout.core import Batch


batch = Batch.from_config(
    inputs=dict(
        kafka=dict(
            type='kafka',
            bootstrap_servers=['kafka:9092'],
            consumer_group='batchout',
            topic='auth.sessions',
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
            type='extracted',
            cast='string',
            path='user.id',
        ),
        user_last_seen=dict(
            type='extracted',
            cast='string',
            path='user.last_seen',
        ),
        session_id=dict(
            type='extracted',
            cast='string',
            path='sessions[{session_idx}].id',  # notice usage of session_idx defined as index above
        ),
        session_created_at=dict(
            type='extracted',
            cast='timestamp',
            path='sessions[{session_idx}].created_at',
        ),
        session_useragent=dict(
            type='extracted',
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
            dbname='user_session',
            from_env=dict(
                user='PG_USER',          # PG_USER and
                password='PG_PASSWORD',  # PG_PASSWORD are read from environment
            ),
        ),
    ),
)
batch.run_once()
```

`Batch.run_once()` processes exactly one batch of payloads from each input.

`Batch.run_forever()` does the same in infinite loop.

## Integrations

### Extractors

#### jsonpath_rw

JSON parser built on top of jsonpath_rw.

### Inputs

#### kafka

Simple consumer built on top of kafka-python.

### Outputs

#### postgres

Postgres table writer built on top of psycopg2.
