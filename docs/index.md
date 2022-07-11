# Batchout documentation starts here

## The paragraph to justify hours spent on this project

Let's define data pipeline as an automation of data processing operations 
that update information we have with information we receive. This automation can take multiple forms:
from a bash script importing CSV files into a database to a multi-step process running in a distributed 
environment.

Sometimes information can be received in the same format that we store it. For example,
when we query a relational database, we receive a table as a result. In many cases, it is possible 
to write a query that will produce a table exactly matching one of tables in target database. 
So, in this case, a pipeline can be designed in declarative style - as no extra logic is needed.

However, when it comes to receiving data in completely different format, like XML or JSON document, 
we need to do all necessary transformations on our side. In this case, automation can be done by developers, 
who write and maintain code implementing these transformations. As alternative, one can use specialized 
software, that provides graphical interface for ETL design. However, the most feature-rich graphical 
designer for data processing would essentially require a certain expertise from a user. Also, maintaining 
complex data pipelines via graphical interface takes more time if software doesn't have features 
like version control or find-and-replace that are available for free when you work with text.

As a solution to a specific set of problems related to data ingestion one can suggest an engine/framework that allows 
building of customized yet simple to configure data pipelines. Syntax of configuration files for such a framework 
could follow few rules to avoid creating unnecessary complexity:

1. Prefer multiple but simpler elements to complex nested objects.
2. Compensate lack of configuration by ease of extensibility.
3. Stick to widely recognised formats and conventions.

## About Batchout

Batchout is built based on these 3 rules and aims to fill in the gap between manually written scripts and complicated 
GUIs for ETL/ELT development. It doesn't target enterprise or high-performance setups but focuses on simpler yet commonly
occurring data extraction use-cases.

The nature of Batchout is similar to a constructor where every part has a specific role.

0. [Tasks](components/00_overview.md#tasks) define where and what to read and what and where to write;
1. [Inputs](components/00_overview.md#inputs) deliver raw data;
2. [Extractors](components/00_overview.md#extractors) process bytes into values;
3. values are mapped to [Indexes](components/00_overview.md#indexes) and [Columns](components/00_overview.md#columns);
4. [Processors](components/00_overview.md#processors) transform [Columns](components/00_overview.md#columns)' values in-place;
5. [Columns](components/00_overview.md#columns) are read by [Selectors](components/00_overview.md#selectors) to create datatables;
6. [Outputs](components/00_overview.md#outputs) read datatables from [Selectors](components/00_overview.md#selectors) into files, 
   databases or log in the terminal.

All together, these components form a system that retrieves, transforms and writes data batch by batch.

## Discover Batchout components

Read about components and how they work in the [components overview](components/00_overview.md).

## Create your own components

### Disclaimer

Batchout is all about extensibility. It is possible to implement a custom component type if you:

1. want to communicate with a specific system;
2. need an extra authentication layer like Kerberos or OAuth;
3. would like to do a specialized processing on a column e.g. math operations.

To put it simple, everything related to data retrieval, processing and ingestion can be customized. However, if you

1. need to change the way components are executed
2. or want to add another component role
3. or want to customize component creation and configuration

then it would require a lot more effort to dive deep into [batchout.core](../batchout/core) package 
and specifically [batchout.core.batch](../batchout/core/batch.py) module which unfortunately concentrates all complexity
that otherwise had to be implemented elsewhere. Refactoring of core package is very much desired yet not planned.

### Example

Let's say there is a need to parse time from a `hh:mm` format to a number of minutes since day started.
Implementing a Processor for such an operation could look like this:

```python3
from typing import Optional

from batchout import Processor                    # base class for every component with role "Processor"
from batchout.core.registry import Registry       # Registry is a singleton class for storing all registered types
from batchout.core.config import with_config_key  # decorator for adding a field to component config


@with_config_key('max_hour', doc='Expect 12 or 24 hour clock', default=24, choices=(12, 24))
@Registry.bind(Processor, 'hhmm_to_minutes')  # 'hhmm_to_minutes' is a type of Processor that has to be unique
class HhmmToMinutesProcessor(Processor):

    def __init__(self, config: dict):
        self.set_max_hour(config)  # this auto-generated method is called to set self._max_hour from config

    def process(self, value: Optional[str]) -> Optional[int]:
        if value is None or ':' not in value:
            return None
        hours, minutes = value.split(':', maxsplit=1)
        try:
           hours = int(hours)
           minutes = int(minutes[:2])
        except ValueError:
           return None
        if self._max_hour == 24:
            return hours * 60 + minutes
        elif 'a' in value.lower():
            return (hours % 12) * 60 + minutes
        elif 'p' in value.lower():
            return (12 + hours % 12) * 60 + minutes
        else:
            return None
```

Save custom component in a file `hhmm_to_minutes.py`.
Then, Batchout CLI can import it as a module, making our new Processor available in configuration:

```bash
$ export PYTHONPATH=.
$ batchout --import-from hhmm_to_minutes -c config.yml
```

Using it in YAML config is as simple as any other Processor:

```yaml
columns:
  # ...
  reported_at_minute:
    type: string
    path: '$[{entry_idx}].reported_at'
    processors:
      - type: hhmm_to_minutes
        max_hour: 12
  # ...
```
