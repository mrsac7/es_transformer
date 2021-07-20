# ES Transfomer 
A Python module that allows users to extract document from a source, parse and ingest into Elasticsearch.

## Setting Up
To setup, install the dependencies with `pip`:

```console
python3 -m pip3 install -r requirements.txt
```

## Running the project

```console
usage: transformer [-h] [-c] [--source_ip SOURCE_IP]
                   [--source SOURCE] [--target_ip TARGET_IP]
                   --target TARGET

Parses documents, extracts required fields and inserts them to
the elasticsearch server.

optional arguments:
  -h, --help            show this help message and exit
  -c, --config          configure metadata for the indices
  --source_ip SOURCE_IP
                        ip of source
  --source SOURCE       name of the source index

required arguments:
  --target_ip TARGET_IP
                        ip of the target
  --target TARGET       name of the target index
```

Refer to the following example for more details:

### Initializing the metadata
```console
python3 src/transformer.py --config --target="sample"
```

### Reindexing
```console
python3 src/transformer.py --source="source" --target="sample"
```