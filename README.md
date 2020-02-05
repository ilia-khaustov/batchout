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
        extractors=dict(
            first_match_in_json=dict(
                type='jsonpath',
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
                type='string',
                path='user.id',
            ),
            user_last_seen=dict(
                type='string',
                path='user.last_seen',
            ),
            session_id=dict(
                type='string',
                path='sessions[{session_idx}].id',  # notice usage of session_idx defined as index above
            ),
            session_created_at=dict(
                type='timestamp',
                path='sessions[{session_idx}].created_at',
            ),
            session_useragent=dict(
                type='timestamp',
                path='sessions[{session_idx}].device.useragent',
            ),
        ),
        selectors=dict(
            all_sessions=dict(
                type='sql',
                query='select * from some_api',
                columns=[
                    'user_id',
                    'user_last_seen',
                    'session_id',
                    'session_created_at',
                    'session_useragent',
                ]
            )
        ),
        outputs=dict(
            local_db=dict(
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
        tasks=dict(
            read_sessions=dict(
                type='reader',
                inputs=['some_api'],
            ),
            walk_sessions=dict(
                type='walker',
                inputs=['some_api'],
                indexes=['session_idx'],
                columns=[
                    'user_id',
                    'user_last_seen',
                    'session_id',
                    'session_created_at',
                    'session_useragent',
                ],
            ),
            write_sessions_to_local_db=dict(
                type='writer',
                selector='all_sessions',
                outputs=['local_db']
            )
        )
    ),
    defaults=dict(
        columns=dict(
            extractor='first_match_in_json',
        ),
        indexes=dict(
            extractor='first_match_in_json',
        ),
    ),
)
batch.run_once()
```

`Batch.run_once()` processes exactly one batch of payloads from each input.

`Batch.run_forever()` does the same in infinite loop.
