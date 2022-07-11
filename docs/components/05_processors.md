
[Index](../index.md) : [Components](00_overview.md) : __Batchout Processors__



# ReplaceProcessor

Source: [batchout.std.processors.pure](../../batchout/std/processors/pure.py)

## Use

In Python:

```python
from batchout.std.processors.pure import ReplaceProcessor
```

In YAML config:

```YAML
processors:
    type: replace
```

## Configuration


### new

_Required_

Text to insert instead.


### old

_Required_

Text to replace.


### count

_Default_: `-1`

Maximum number of replacements (replace all if -1).
