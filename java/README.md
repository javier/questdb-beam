# questdb-beam: Apache Beam Sink for QuestDB in JAVA

[Apache Beam JAVA sink](https://beam.apache.org/) for writing data into [QuestDB](https://questdb.io) time-series
database.

The Sink supports both batch and streaming.

## Basic usage

The QuestDB Sink is called by passing a PCollection of `QuestDbRow` elements to `QuestDbIO.write()`. `withUri` and 
`withTable` parameters are mandatory.  If SSL is needed, you can use `withSSLEnabled` as a boolean. For authentication 
you need to set the boolean `withAuthEnabled` and provide `withAuthUser` and `withAuthToken` strings.

The `QuestDbRow` has methods to put columns of the supported types. All the put methods accept either the native type 
(i.e. `Long` when using `putLong`) or a `String` that will be converted to the native type. `putTimestamp` expects 
the epoch in microseconds. For your convenience, if your epoch is in milliseconds you can call `putTimestampMs` and it
will be converted. 

If `setDesignatedTimesamp` is not called, the server will assign a timestamp on ingestion. Designated timestamp needs
to be [in nanoseconds](https://questdb.io/docs/reference/clients/java_ilp/). If your epoch for the designated timestamp
is in milliseconds, you can call `setDesignatedTimestampMs` and it will be converted.


```
static class LineToMapFn extends DoFn<String, QuestDbRow> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<QuestDbRow> receiver) throws Exception {
            String[] values = element.split(",");
            QuestDbRow row =
                    new QuestDbRow()
                    .putSymbol("user_id", values[0])
                    .putSymbol("team_id", values[1])
                    .putLong("score", values[2])
                    .putTimestampMs("timestampED", values[3])
                    .setDesignatedTimestampMs(values[3]);
            receiver.output(row);
        }
    }
    

(....)
    
pcoll.apply(ParDo.of(new LineToMapFn()));
        parsedLines.apply(QuestDbIO.write()
                .withUri("localhost:9009")
                .withTable("beam_demo")
        );
```

For authentication use:

```
pcoll.apply(ParDo.of(new LineToMapFn()));
    parsedLines.apply(QuestDbIO.write()
    .withUri("your-instance-host.questdb.com:YOUR_PORT")
    .withTable("beam_demo")
    .withSSLEnabled(true)
    .withAuthEnabled(true)
    .withAuthUser("admin")
    .withAuthToken("verySecretToken")
```

## Building the project

```sh
# To do a simple run.
mvn compile exec:java

# To run passing command line arguments.
mvn compile exec:java -Dexec.args=--inputTopic="echo-output"
```

To build a self-contained jar file

```sh
# Build a self-contained jar.
mvn clean package

# Run the jar application.
java -jar target/questdb-beam-1-jar-with-dependencies.jar -inputTopic="echo-output"
```


