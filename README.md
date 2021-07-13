# ES Transfomer 
A Python module that allows to extract document from a source, parse and ingest into Elasticsearch.

## Setting Up
To setup, install the dependencies with `pip`:

```console
python3 -m pip3 install -r requirements.txt
```

## Running the project
To run the project refer to the following example:

### Initializing the metadata
```python
import Transformer

ts = Transformer() #setting up ES client
index = "sample"
ts.create_index_config(index) #creates config file
ts.create_ingest_pipeline(index) #creates ingest pipeline
ts.set_index_mapping(index) #creates index template
```

### Reindexing
```python
import Transfomer

ts = Transformer()
source_index = "source"
target_index = "sample"
ts.reindex(source, sample)
```