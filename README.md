# questdb-beam: Apache Beam Sink for QuestDB in Python and Java

[Apache Beam python sink](https://beam.apache.org/) for writing data into [QuestDB](https://questdb.io) time-series
database.

We offer two versions:

* [Java Sink](./java/)
* [Python Sink](./python/). Please note that since Python streaming is not well supported in many of the Beam runners (including
the direct runner and the Flink portable one), you might prefer to use the JAVA version in streaming scenarios.


# Basic Python usage

```
pcoll | WriteToQuestDB(table, symbols=[list_of_symbols], columns=[list_of_columns],
        host=host, port=port, batch_size=optionalSizeOfBatch, tls=optionalBoolean, auth=optionalAuthDict)
```

# Basic JAVA usage

```
// pcoll needs to be a PCollection with QuestDbRow objects

pcoll.apply(ParDo.of(new LineToMapFn()));
        parsedLines.apply(QuestDbIO.write()
                .withUri("localhost:9009")
                .withTable("beam_demo")
                .withDeduplicationEnabled(true)
        );
```

# Running the examples

Please refer to the [examples README](./examples/)
