# Running the examples

Unless `questdb-beam` has been installed via pip, all the examples need to be run from the root of this repository
(one folder above this one).

## Wordcount

This example is based on the [minimal wordcount BEAM example](https://beam.apache.org/get-started/wordcount-example/#minimalwordcount-example).
Running the example will read lines from a (configurable) file, split into words, and will output pairs of words and
counts to a (configurable) table on a questdb instance.

Available args and default values can be shown by running:
```
python -m examples.wordcount.wordcount_file_to_questdb --help
```

If you don't have a running QuestDB instance, you can start one with ephemeral storage with this command:

```
docker run --add-host=host.docker.internal:host-gateway -p 9000:9000 -p 9009:9009 -p 8812:8812 -p 9003:9003 questdb/questdb:latest
```

Please refer to the [QuestDB Documentation](https://questdb.io/docs/) for alternative ways of starting your QuestDB instance.


The following command will read from `kinglear.txt`, and insert into the table `beam_wc` at a localhost QuestDB instance.

```
python -m examples.wordcount.wordcount_file_to_questdb
```
