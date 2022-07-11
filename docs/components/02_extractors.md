
[Index](../index.md) : [Components](00_overview.md) : __Batchout Extractors__



# RegexExtractor

Source: [batchout.std.extractors.regex](../../batchout/std/extractors/regex.py)

## Use

In Python:

```python
from batchout.std.extractors.regex import RegexExtractor
```

In YAML config:

```YAML
extractors:
    type: regex
```

## Configuration


### decode_bytes

_Default_: `True`

_Choices_: One of `True`, `False`

Decode bytes to string or use regex on bytes.


### encoding

_Default_: `utf8`


### flags

Regex flags supported by Python.


### group

Capture group to extract from, starting from 0 (whole match).


### strategy

_Default_: `take_first`

_Choices_: One of `take_first`, `take_last`, `take_all`


# JsonpathExtractor

Source: [batchout.ext.jsonpath.extractors](../../batchout/ext/jsonpath/extractors.py)

## Use

In Python:

```python
from batchout.ext.jsonpath.extractors import JsonpathExtractor
```

In YAML config:

```YAML
extractors:
    type: jsonpath
```

## Configuration


### strategy

_Default_: `take_first`

_Choices_: One of `take_first`, `take_first_not_null`, `take_last`, `take_last_not_null`


# XPathExtractor

Source: [batchout.ext.xpath.extractors](../../batchout/ext/xpath/extractors.py)

## Use

In Python:

```python
from batchout.ext.xpath.extractors import XPathExtractor
```

In YAML config:

```YAML
extractors:
    type: xpath
```

## Configuration


### html

_Choices_: One of `True`, `False`


### strategy

_Default_: `take_first`

_Choices_: One of `take_first`, `take_last`, `take_all`
