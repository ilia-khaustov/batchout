
[Index](../index.md) : [Components](00_overview.md) : __Batchout Outputs__



# LoggerOutput

Source: [batchout.std.outputs.logger](../../batchout/std/outputs/logger.py)

## Use

In Python:

```python
from batchout.std.outputs.logger import LoggerOutput
```

In YAML config:

```YAML
outputs:
    type: logger
```

## Configuration


### width

_Default_: `80`


# CsvOutput

Source: [batchout.std.outputs.csv](../../batchout/std/outputs/csv.py)

## Use

In Python:

```python
from batchout.std.outputs.csv import CsvOutput
```

In YAML config:

```YAML
outputs:
    type: csv
```

## Configuration


### path

_Required_


### delimiter

_Default_: `,`


### mode

_Default_: `append`

_Choices_: One of `append`, `overwrite`
