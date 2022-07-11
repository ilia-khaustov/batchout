
[Index](../index.md) : __Batchout Components__

# Roles and types

Components are split in groups according to their role: [Inputs](#inputs), [Extractors](#extractors), [Columns](#columns) etc.

All roles except [Maps](#maps) and [Tasks](#tasks) can be extended by custom types. Available implementations are 
distributed with a [standard library](../../batchout/std/__init__.py) or one of [extra](../../batchout/ext/__init__.py) packages. 

Standard library doesn't require installation of any dependencies.

Using components from extra packages is only possible after installing Batchout with extras:

```bash
pip install batchout[jsonpath, xpath, postgres]
```

# Configuration

Examples of configuration are given in YAML format for the sake of readability:

```yaml
inputs:
  huge_xml:
    type: file
    path: "/path/to/some/huge.xml"
```

It is also possible to import component class in Python and initialize it with a Python dict:

```python3
from batchout import FileInput

huge_xml = FileInput(dict(path='/path/to/some/huge.xml'))
```

Another way to create a component is to use `Registry`:

```python3
from batchout.core.registry import Registry
from batchout import Input

huge_xml = Registry.create(Input, dict(type='file', path='/path/to/some/huge.xml'))
```

## Environment variables

Every component can be configured by optional config key `from_env`.

It is expected to be a mapping from regular config keys to environment variables.

When created via `Registry` or Batchout CLI (which uses `Registry` internally), corresponding values in component config will be substituted with values from env.

This can be handy when dealing with passwords, API tokens and other kinds of secrets:

```yaml
outputs:
  postgres_in_docker:
    type: postgres
    host: localhost
    port: 5432
    dbname: batchout
    from_env: # user, password, host and port are taken from env
      user: OUTPUTS_USER
      password: OUTPUTS_PASSWORD
      host: OUTPUTS_HOST
      port: OUTPUTS_PORT
    keys: [artist, title, liked_by]
    mode: upsert
    table: soundcloud_likes
```


# Who is who

## Tasks

Tasks organise reading and writing process.

Tasks don't work with data, they only connect other components that do the actual work.

Example of tasks config:
```yaml
tasks:
  read_pages:
    type: reader
    selector: unread_pages       # read unread pages
    threads: 8                   # read 8 pages in parallel
    inputs: [my_favourite_blog]  # from your favourite blog
  write_blog_posts:
    type: writer
    selector: blog_posts         # write scraped posts from your favourite blog
    outputs: [webscraper_db]     # into a Postgres db
```

### Reader

Task with `type: reader` controls reading from connected `inputs`.

Data from `selector` is used to fetch from [Input](#inputs) in the next iteration:

* After first batch, connected [Selector](#selectors) will produce rows of data;
* Every row will be sent as `params` to all connected **Inputs** in the next batch;

Number of `threads` allows fetching data for multiple sets of `params` in parallel.

### Writer

Task with `type: writer` maps `selector` to connected `outputs`.

Data from connected [Selector](#selectors) will be sent to all [Outputs](#outputs).

## Inputs

**Supported types**: [Batchout Inputs](01_inputs.md)

**Inputs** provide raw data in a form of `bytes` - payloads.

**Input** returns a payload on each `fetch(**params)` call.

`fetch(**params)` is called until it returns `None`:

* `None` indicates that all data for given parameters has been consumed;
* `params` are defined with their default values in Input config;
* It is possible to fetch from **Input** with multiple sets of params: check [reader Tasks](#reader).

`reset()` is called to signal that next `fetch()` is coming, so it's time to do all the necessary clean-up.

_After current batch finished processing_, `commit()` is called for **Input** to save its progress in external system:

* Notice that `commit()` is called after the whole chain has completed, including `commit()` by [Outputs](#outputs);
* It ensures that **Input** will save its progress only if fetched data has been successfully written;
* If processing of a batch caused an error, external system that **Input** reads from should not "move a cursor";
* **Input** implementations that read from such systems are responsible for saving their progress during `fetch()` and
  `reset()` calls and reporting it during `commit()`.

## Extractors

**Supported types**: [Batchout Extractors](02_extractors.md)

**Extractors** parse payload of bytes from [Inputs](#inputs) and produce values.

Every **Extractor** implements `extract(path: str, payload: bytes)` that returns a tuple of `path` and `value`:

* It is required for `extract()` to return `path` although it takes argument with the same name;
* The reason is that `path` in args can be an expression that unfolds into several actual `path`s inside a payload;
* Sometimes it is beneficial to know where exactly has the value been extracted from;
* Receiving result as `(path, value)` guarantees that it is always possible to map the value back to a payload.

Both `path` and `value` can be `None`:

* If `path` is `None` it means that nothing could be found in payload for given `path`;
* If `value` is None but `path` is not - something has been found, and it is empty/null value.

Currently, all **Extractor** implementations depend on `batchout.std.extractors.mixin.WithStrategy`:

* This mixin adds `strategy` to configuration, which can take different values for each implementation;
* `strategy` defines how to handle multiple matches inside `payload`: return first, last or all of them as a list.

## Indexes

**Supported types**: [Batchout Indexes](03_indexes.md)

**Index** implements `values(extractor: Extractor, payload: bytes, **parent_indexes: str|int)`.

Returned value is usually a list of numbers or text.

**Index** can be referenced in a `path` for [Column](#columns) or other **Index**.

Using **Indexes** helps to extract values from elements in collections using the same extraction `path`.

In some cases **Indexes** require an [Extractor](#extractors) with a `strategy: take_all`:
  * For example, when indexing over objects from different positions in a document.

## Columns

**Supported types**: [Batchout Columns](04_columns.md)

**Column** represents a value extracted from a payload for given `path` and cast to a required type.

**Column** implements `value(extractor: Extractor, payload: bytes, **indexes: str|int)`.

However, implementation of a **Column** with a complex type like JSON is possible.

Internally, **Columns** extracted from an [Input](#inputs) are stored as a table of in-memory SQLite database.

## Maps

**Maps** connect [Inputs](#inputs) to [Indexes](#indexes) and [Columns](#columns).

It is required for every **Input** to be matched with a **Map** of equal name.

**Map** is a recursive list with 2 possible types of items:

  * string - should match a **Column** name;
  * key-value mapping, where:
    * key - should match an **Index** name;
    * value is another *Map*.
    
The meaning of such format is to describe paths within tree-like structures containing sequences of variable length.

**Columns** "inside" **Indexes** are extracted N times for each combination of parent **Index** values.

Values from **Indexes** are passed to `Column.value()` as `**indexes`, each mapped to **Index** name.

In case there are 2 indexes on the same level (neither depends on values from other):

* Put them into the same key-value mapping to produce different **Columns** for each **Index**;
* Keep every **Index** in its own key-value mapping to have all combinations of **Columns** from each **Index**.

Example with fetching all combinations of **Columns** from each **Index**:

```yaml
maps:
  places_api:
    - place_name
    - location_lat
    - location_lon
    - origin_idx:
        - origin_place_name
        - origin_location_lat
        - origin_location_lon
    - destination_idx:
        - destination_place_name
        - destination_location_lat
        - destination_location_lon
```

With a `Map` like above, `Batchout` produces all combinations of possible routes: from origin to `place_name` and from `place_name` to destination.

Here is another example with fetching different sets of **Columns**:
```yaml
maps:
  departments_api:
    - dept_idx:
        - dept_name
        - empl_idx:
            - employee_email
            - employee_phone
          device_idx: # notice missing sign "-" before device_idx
            - device_type
            - device_serial
```

With a `Map` like above, `Batchout` produces 2 sets of rows in one batch:

* In one set, columns `employee_email` and `employee_phone` are always empty;
* In the other set, columns `device_type` and `device_serial` are always empty;
* Column `dept` contains extracted value for each set.

## Processors

**Supported types**: [Batchout Processors](05_processors.md)

**Processors** transform **Column** value into something else.

One **Column** can also host a chain of **Processors**, with every value from a batch going through a sequence of transformations:

```yaml
indexes:
  double_name:
    extractor: regex_all_matches
    type: from_list
    path: '[A-Z][a-z]+\-[A-Z][a-z]+'
columns:
  full_sentence:
    extractor: regex_first_match
    type: string
    path: '(?:[A-Z][^.?!]* )?{double_name}(?: [^.?!]*)[.?!]'
    processors:
      - type: replace
        old: '\n'
        new: ' '
      - type: replace
        old: '  '
        new: ' '
```

## Selectors

**Supported types**: [Batchout Selectors](06_selectors.md)

After all data from batch is extracted, put into [Columns](#columns) and processed, it's time to select what we want from it.

This is done by **Selectors** that operate on internal `Data` object representing all extracted **Columns**.

Currently, the only type of **Selectors** is available - `sql`:

* Given `query` is executed by cursor connected to in-memory SQLite database;
* Every [Input](#inputs) is represented by a table in database;
* Table contains **Columns** mapped to table's **Input** via [Map](#maps).

## Outputs

**Supported types**: [Batchout Outputs](07_outputs.md)

**Output** is the final destination of data.

Its purpose is to write data prepared by [Selector](#selectors) into an external system.

Every **Output** implements `ingest(columns: Collection[str], rows: Iterable[Collection[Any]])` returning number of rows written.

It can also implement `commit()` that is called in the end of `writer` [Task](#tasks) execution after all connected **Outputs** finished with `ingest()`.
