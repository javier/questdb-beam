# questdb-beam: Apache Beam Sink for QuestDB in JAVA

[Apache Beam JAVA sink](https://beam.apache.org/) for writing data into [QuestDB](https://questdb.io) time-series
database.

The Sink supports both batch and streaming.

# Basic usage

A pcollection of elements (of type `Map`) can be passed to the QuestDB sink. `withUri` and `withTable` parameters are
mandatory. If `withDesignatedTimestampColumn` is defined, that column will be used as the designated timestamp (make
sure you have [the right epoch resolution](https://questdb.io/docs/reference/clients/java_ilp/)). Otherwise, the
timestamp will be assigned by the server on ingestion. You can pass the names and types of the columns you want the
Sink to output using the parameters `withSymbolColumns`, `withStringColumns`, `withLongColumns`, `withDoubleColumns`,
`withBoolColumns`, and `withTimestampColumns`. Please note not designated timestamps use epoch in milliseconds. If
SSL is needed, you can use `withSSLEnabled` as a boolean. For Authentication you need to set the boolean
 `withAuthEnabled` and provide `withAuthUser` and `withAuthToken` strings.

```
pcoll.apply(QuestDbIO.write()
						.withUri("localhost:9009")
						.withTable("author2")
						.withSymbolColumns(List.of("user_id"))
						.withStringColumns(List.of("team_id"))
						.withLongColumns(List.of("score"))
						.withDesignatedTimestampColumn("timestampED")
				);
```

# Running the examples

Please refer to the [examples README](./examples/)

```

### Option C: Apache Maven

[Apache Maven](http://maven.apache.org) is a project management and comprehension tool based on the concept of a project object model (POM).

This is by far the trickiest to configure, but many older projects still use it.

If you are starting a new project, we recommend using Gradle or `sbt` instead.

> ℹ️ If you have an existing Maven project, consider looking at a [Gradle vs Maven comparison](https://gradle.org/maven-vs-gradle), as well as Gradle's [Migrating builds from Apache Maven](https://docs.gradle.org/current/userguide/migrating_from_maven.html) guide.

```sh
sdk install maven
```

A basic Apache Maven setup consists of a [`pom.xml`](pom.xml) file written in [XML](https://www.w3schools.com/xml).

To run the app through Maven, we need to configure [`exec-maven-plugin`](http://www.mojohaus.org/exec-maven-plugin) in the [`pom.xml`](pom.xml) file.

```sh
# To do a simple run.
mvn compile exec:java

# To run passing command line arguments.
mvn compile exec:java -Dexec.args=--inputText="🎉"

# To run the tests.
mvn test
```

To build a self-contained jar file, we need to configure [`maven-assembly-plugin`](https://people.apache.org/~epunzalan/maven-assembly-plugin/index.html) in the [`pom.xml`](pom.xml) file.

```sh
# Build a self-contained jar.
mvn package

# Run the jar application.
java -jar target/beam-java-starter-1-jar-with-dependencies.jar --inputText="🎉"
```


