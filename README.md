# Batchout

Framework for building data pipelines that: 

1. Extract batch of payloads of any structure like JSON, XML or just text/bytes;
2. Transform batch of payloads to batch of rows for a table defined by columns;
3. Load batch of table rows to persistent storage.

## Install

`pip install batchout` without dependencies except Python 3.9+

`pip install batchout[cli]` with a basic CLI if you avoid coding in Python

`pip install batchout[postgres]` for Postgres reading/writing

`pip install batchout[jsonpath]` to extract data from JSON documents

`pip install batchout[xpath]` to extract data from XML/HTML

## Use in Python

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
                type='datetime',
                path='sessions[{session_idx}].created_at',
            ),
            session_useragent=dict(
                type='datetime',
                path='sessions[{session_idx}].device.useragent',
            ),
        ),
        maps=dict(
            some_api=[
                'user_id',
                'user_last_seen',
                dict(
                    session_idx=[
                        'session_id',
                        'session_created_at',
                        'session_useragent',
                    ]
                ),
            ],
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

## Use in terminal

> Requires `pip install batchout[cli]`

    $ batchout --help
    usage: batchout [-h] -c CONFIG [-n NUM_BATCHES] [-w MIN_WAIT_SEC] [-W MAX_WAIT_SEC] [-l LOG_LEVEL]
    
    Run Batchout from a config file (YAML)
    
    optional arguments:
      -h, --help            show this help message and exit
      -c CONFIG, --config CONFIG
                            Path to YAML config file
      -I [IMPORT_FROM ...], --import-from [IMPORT_FROM ...]
                            Import Python modules containing custom Batchout components
      -n NUM_BATCHES, --num-batches NUM_BATCHES
                            Stop after N batches (never stop if -1 or less)
      -w MIN_WAIT_SEC, --min-wait-sec MIN_WAIT_SEC
                            Minimum seconds to wait between batches
      -W MAX_WAIT_SEC, --max-wait-sec MAX_WAIT_SEC
                            Maximum seconds to wait between batches
      -l LOG_LEVEL, --log-level LOG_LEVEL
                            Choose logging level between 10 (DEBUG) and 50 (FATAL)


## Read documentation

First time? Proceed to [Batchout documentation](docs/index.md).
