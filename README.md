# questdb-beam-py

[Apache Beam python sink](https://beam.apache.org/) for writing data into [QuestDB](https://questdb.io) time-series database

# Basic usage

A pcollection of elements (of type `Dict`) can be passed to the QuestDB sink. `batch_size`, `tls`, and `auth` parameters
are optional

```
pcoll | WriteToQuestDB(table, symbols=[list_of_symbols], columns=[list_of_columns],
        host=host, port=port, batch_size=optionalSizeOfBatch, tls=optionalBoolean, auth=optionalAuthDict)
```

# Running the examples

Please refer to the [examples README](./examples/README.md)
