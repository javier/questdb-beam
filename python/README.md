# questdb-beam: Apache Beam Sink for QuestDB in Python

[Apache Beam python sink](https://beam.apache.org/) for writing data into [QuestDB](https://questdb.io) time-series
database.

_Note:_ The Sink is fully functional and has been tested with batch workloads in several runners. However, streaming
support woth Python is limited in some of the runners, and have found that, for example, when trying to read from Kafka using both
the Direct or the Flink portable runners, messages are never consumed from Kafka. In this case the messages are never
passed to the QuestDB sink. There are some reports of this problem at multiple links, with this one being quite
accurate as of June 2023 https://issues.apache.org/jira/browse/BEAM-11998. Please make sure you test your streaming
pipeline works fine on your supported runner outputting to plain text before trying to add the QuestDB connector. We
offer an alternative JAVA Sink which works without any issues in different runners.

# Basic usage

A pcollection of elements (of type `Dict`) can be passed to the QuestDB sink. `batch_size`, `tls`, and `auth` parameters
are optional

```
pcoll | WriteToQuestDB(table, symbols=[list_of_symbols], columns=[list_of_columns],
        host=host, port=port, batch_size=optionalSizeOfBatch, tls=optionalBoolean, auth=optionalAuthDict)
```

# Running the examples

Please refer to the [examples README](./examples/)
