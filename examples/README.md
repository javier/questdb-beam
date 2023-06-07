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

## Game

This example simulates usage data for a mobile game, sends the data to Kafka, and inserts into QuestDB.
Heavily inspired by https://github.com/apache/beam/tree/master/examples/java/src/main/java/org/apache/beam/examples/complete/game

You will need a Kafka installation for this example. You can get one up and running following the
[Apache Kafka Quickstart](https://kafka.apache.org/quickstart). Once you have Kafka up and running, we will need to
create a topic, as in

```
bin/kafka-topics.sh --create --topic game-events --bootstrap-server localhost:9092
```

Now we will start the injector. The injector sends data to a text file, and we will then just start a shell to tail the
file and send directly into Kafka.

Change directory to `examples/game/injector` and compile the injector with

```
mvn clean package
```

Now to start generating demo data into a text file you can run

```
java -jar target/injector-1.0-SNAPSHOT.jar /path/to/file
```

The generator will keep generating data until stopped. To tail the file into our Kafka topic, open a new terminal
window and execute:

```
tail -f /tmp/game.txt|./bin/kafka-console-producer.sh --topic game-events --bootstrap-server localhost:9092
```

Data should be now being sent to the Kafka topic. If you want to double check you can read from Kafka using:

```
./bin/kafka-console-consumer.sh --topic game-events --bootstrap-server localhost:9092
```

Once you see data is entering Kafka, you can quit this last command and start the Beam pipeline. Change directory to the
root of the repository (a directory above the examples one) and execute:

```
python -m examples.game.raw_events_to_questdb
```
