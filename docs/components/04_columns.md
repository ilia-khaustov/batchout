
[Index](../index.md) : [Components](00_overview.md) : __Batchout Columns__



# DatetimeColumn

Source: [batchout.std.columns.scalar](../../batchout/std/columns/scalar.py)

## Use

In Python:

```python
from batchout.std.columns.scalar import DatetimeColumn
```

In YAML config:

```YAML
columns:
    type: datetime
```

## Configuration


### extractor

_Required_

Connect to one of [Extractors](02_extractors.md).


### path

_Required_

Expression used to search for a value by Extractor.


### format

Datetime format string compatible with `datetime.strptime()`.


### parser

_Default_: `iso`

_Choices_: One of `iso`, `unix`, `custom`

`iso` to parse ISO format, `unix` to convert from Unix epoch, `custom` to use `format` from config and parse with `datetime.strptime()`.


### processors

Apply sequence of Processors to each value from column.


### timezone

_Default_: `UTC`

Convert timezone-aware datetime or set for timezone-naive datetime.


# DateColumn

Source: [batchout.std.columns.scalar](../../batchout/std/columns/scalar.py)

## Use

In Python:

```python
from batchout.std.columns.scalar import DateColumn
```

In YAML config:

```YAML
columns:
    type: date
```

## Configuration


### extractor

_Required_

Connect to one of [Extractors](02_extractors.md).


### path

_Required_

Expression used to search for a value by Extractor.


### format

Datetime format string compatible with `datetime.strptime()`.


### parser

_Default_: `iso`

_Choices_: One of `iso`, `unix`, `custom`

`iso` to parse ISO format, `unix` to convert from Unix epoch, `custom` to use `format` from config and parse with `datetime.strptime()`.


### processors

Apply sequence of Processors to each value from column.


### timezone

_Default_: `UTC`

Convert timezone-aware datetime parsed from custom format before extraction of date.


# FloatColumn

Source: [batchout.std.columns.scalar](../../batchout/std/columns/scalar.py)

## Use

In Python:

```python
from batchout.std.columns.scalar import FloatColumn
```

In YAML config:

```YAML
columns:
    type: float
```

## Configuration


### extractor

_Required_

Connect to one of [Extractors](02_extractors.md).


### path

_Required_

Expression used to search for a value by Extractor.


### processors

Apply sequence of Processors to each value from column.


# IntegerColumn

Source: [batchout.std.columns.scalar](../../batchout/std/columns/scalar.py)

## Use

In Python:

```python
from batchout.std.columns.scalar import IntegerColumn
```

In YAML config:

```YAML
columns:
    type: integer
```

## Configuration


### extractor

_Required_

Connect to one of [Extractors](02_extractors.md).


### path

_Required_

Expression used to search for a value by Extractor.


### processors

Apply sequence of Processors to each value from column.


# StringColumn

Source: [batchout.std.columns.scalar](../../batchout/std/columns/scalar.py)

## Use

In Python:

```python
from batchout.std.columns.scalar import StringColumn
```

In YAML config:

```YAML
columns:
    type: string
```

## Configuration


### extractor

_Required_

Connect to one of [Extractors](02_extractors.md).


### path

_Required_

Expression used to search for a value by Extractor.


### processors

Apply sequence of Processors to each value from column.


# BooleanColumn

Source: [batchout.std.columns.scalar](../../batchout/std/columns/scalar.py)

## Use

In Python:

```python
from batchout.std.columns.scalar import BooleanColumn
```

In YAML config:

```YAML
columns:
    type: boolean
```

## Configuration


### extractor

_Required_

Connect to one of [Extractors](02_extractors.md).


### path

_Required_

Expression used to search for a value by Extractor.


### processors

Apply sequence of Processors to each value from column.
