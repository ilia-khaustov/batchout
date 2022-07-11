
[Index](../index.md) : [Components](00_overview.md) : __Batchout Indexes__



# IndexForList

Source: [batchout.std.indexes.scalar](../../batchout/std/indexes/scalar.py)

## Use

In Python:

```python
from batchout.std.indexes.scalar import IndexForList
```

In YAML config:

```YAML
indexes:
    type: for_list
```

## Configuration


### extractor

_Required_

Type of [Extractor](02_extractors.md) to use.


### path

_Required_

Path expression to use for extracting index values.


# IndexForObject

Source: [batchout.std.indexes.scalar](../../batchout/std/indexes/scalar.py)

## Use

In Python:

```python
from batchout.std.indexes.scalar import IndexForObject
```

In YAML config:

```YAML
indexes:
    type: for_object
```

## Configuration


### extractor

_Required_

Type of [Extractor](02_extractors.md) to use.


### path

_Required_

Path expression to use for extracting index values.


# IndexFromList

Source: [batchout.std.indexes.scalar](../../batchout/std/indexes/scalar.py)

## Use

In Python:

```python
from batchout.std.indexes.scalar import IndexFromList
```

In YAML config:

```YAML
indexes:
    type: from_list
```

## Configuration


### extractor

_Required_

Type of [Extractor](02_extractors.md) to use.


### path

_Required_

Path expression to use for extracting index values.
