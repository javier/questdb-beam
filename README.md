# questdb-beam

[Apache Beam python sink](https://beam.apache.org/) for writing data into [QuestDB](https://questdb.io) time-series database

# Basic usage

A pcollection of elements (of type `Dict`) can be passed to the QuestDB sink. `batch_size`, `tls`, and `auth` parameters
are optional

```
pcoll | WriteToQuestDB(table, symbols=[list_of_symbols], columns=[list_of_columns],
        host=host, port=port, batch_size=optionalSizeOfBatch, tls=optionalBoolean, auth=optionalAuthDict)
```

# Running the example

This repository includes an example based on the [minimal wordcount BEAM example](https://beam.apache.org/get-started/wordcount-example/#minimalwordcount-example).
Running the example will read words from a file (it defaults to `./questdb_beam/example/kinglear.txt` and can be changed
via the `input` arg) and will output to a questdb instance running at `http://localhost:9009` (can be changed via the
`questdb-host` and `questdb-port` args). If you don't have an instance, you can start an ephemeral one using Docker with
 this command:

```
docker run --add-host=host.docker.internal:host-gateway -p 9000:9000 -p 9009:9009 -p 8812:8812 -p 9003:9003 questdb/questdb:latest
```

Please refer to the [QuestDB Documentation](https://questdb.io/docs/) for alternative ways of starting your QuestDB instance.

The example will insert data on a table named `beamt` (can be changed via the `questdb-table` arg). To run the example, execute:

```
python -m questdb_beam.example.wordcount_file_to_questdb
```
