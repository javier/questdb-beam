# questdb-beam: Apache Beam Sink for QuestDB in JAVA

[Apache Beam JAVA sink](https://beam.apache.org/) for writing data into [QuestDB](https://questdb.io) time-series
database.

The Sink supports both batch and streaming.

## Basic usage

A pcollection of elements (of type `Map`) can be passed to the QuestDB sink. `withUri` and `withTable` parameters are
mandatory. If `withDesignatedTimestampColumn` is defined, that column will be used as the designated timestamp (make
sure you have [the right epoch resolution](https://questdb.io/docs/reference/clients/java_ilp/)). Otherwise, the
timestamp will be assigned by the server on ingestion. You can pass the names and types of the columns you want the
Sink to output using the parameters `withSymbolColumns`, `withStringColumns`, `withLongColumns`, `withDoubleColumns`,
`withBoolColumns`, and `withTimestampColumns`. Please note not designated timestamps use epoch in milliseconds. If
SSL is needed, you can use `withSSLEnabled` as a boolean. The column values are constructed using the classes from the
`org.apache.beam.sdk.io.questdb.columns` package.

For Authentication you need to set the boolean
 `withAuthEnabled` and provide `withAuthUser` and `withAuthToken` strings.

```
static class LineToMapFn extends DoFn<String, Map<String, QuestDbColumn>> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<Map<String, QuestDbColumn>> receiver) throws Exception {
            Map<String, QuestDbColumn> elementMap = new HashMap<String, QuestDbColumn>();
            String[] values = element.split(",");
            elementMap.put("user_id", new SymbolColumn(values[0]));
            elementMap.put("team_id", new SymbolColumn(values[1]));
            elementMap.put("score", new LongColumn(Long.valueOf(values[2])));
            elementMap.put("timestampED", new TimestampColumn(Long.valueOf(values[3]) * 1000000));
            receiver.output(elementMap);
        }
    }
    

(....)
    
pcoll.apply(QuestDbIO.write()
  .withUri("localhost:9009")
  .withTable("author2")
  .withSymbolColumns(List.of("user_id", "team_id"))  
  .withLongColumns(List.of("score"))
  .withDesignatedTimestampColumn("timestampED")
  );
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


