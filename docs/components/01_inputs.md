
[Index](../index.md) : [Components](00_overview.md) : __Batchout Inputs__



# ConstInput

Source: [batchout.std.inputs.const](../../batchout/std/inputs/const.py)

## Use

In Python:

```python
from batchout.std.inputs.const import ConstInput
```

In YAML config:

```YAML
inputs:
    type: const
```

## Configuration


### data

_Required_

Payloads of data as list of string values.


# HttpInput

Source: [batchout.std.inputs.http](../../batchout/std/inputs/http.py)

## Use

In Python:

```python
from batchout.std.inputs.http import HttpInput
```

In YAML config:

```YAML
inputs:
    type: http
```

## Configuration


### url

_Required_

Universal Resource Locator.


### headers

A mapping of header names to header values.


### ignore_status_codes

Return None in case of response status code being one of theses.


### max_backoff_sec

_Default_: `60`

Maximum wait between retries.


### method

_Default_: `get`

_Choices_: One of `get`, `post`, `put`, `delete`, `head`

HTTP verb.


### params

Default values for arbitrary params.


### retries

_Default_: `3`

Retry request exact number of times in case of empty response.


### timeout_sec

_Default_: `60`

Define after how many seconds consider request had no answer.


# FileInput

Source: [batchout.std.inputs.file](../../batchout/std/inputs/file.py)

## Use

In Python:

```python
from batchout.std.inputs.file import FileInput
```

In YAML config:

```YAML
inputs:
    type: file
```

## Configuration


### path

_Required_

Path to a file to read from; can be a glob mask.


### chunk_bytes

Maximum number of bytes per chunk, used to split file in chunks.


### chunk_endswith

Sequence of bytes delimiting chunks of data, used to split file in chunks.


### recursive

_Choices_: One of `True`, `False`

Recursively scan all files matching given path.
