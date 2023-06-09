// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package org.apache.beam.sdk.io.questdb;

import org.apache.beam.examples.WindowExample;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class App {
    public static void buildKafkaPipeline(Pipeline pipeline, String topic) {
        PCollection linesFromKafka = pipeline
                .apply(KafkaIO.<String, String>read()
                        .withBootstrapServers("localhost:9092")
                        .withTopic(topic)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withoutMetadata()
                )
                .apply(Values.create())
                //.apply(ParDo.of(new QuestDBOutFn()))
                .apply("Print elements",
                        MapElements.into(TypeDescriptors.strings()).via(x -> {
                            System.out.println(x);
                            return x;
                        })
                );


        PCollection parsedLines = (PCollection) linesFromKafka.apply(ParDo.of(new LineToMapFn()));


        parsedLines
                .apply(QuestDbIO.write()
                        .withUri("localhost:9009")
                        .withTable("beam_test")
                        .withDeduplicationEnabled(true)
                        .withDeduplicationByValue(false)
                        .withDeduplicationDurationMillis(10L)
                        .withSSLEnabled(false)
                        .withAuthEnabled(false)
                        .withAuthUser("admin")
                        .withAuthToken("ignore")
                );


    }

    public static void main(String[] args) {
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        var pipeline = Pipeline.create(options);

        App.buildKafkaPipeline(pipeline, options.getInputTopic());
        pipeline.run().waitUntilFinish();
    }

    public interface Options extends StreamingOptions {
        @Description("Kafka topic to read from")
        @Default.String("echo-output")
        String getInputTopic();

        void setInputTopic(String value);
    }

    static class LineToMapFn extends DoFn<String, QuestDbRow> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<QuestDbRow> receiver) throws Exception {
            String[] values = element.split(",");
            QuestDbRow row =
                    new QuestDbRow()
                            .putSymbol("user_id", values[0])
                            .putSymbol("team_id", values[1])
                            .putLong("score", values[2])
                            .putTimestamp("timestampED", values[3].substring(0,values[3].length() - 3))
                            .setDesignatedTimestamp(values[3]);
            receiver.outputWithTimestamp(row, Instant.ofEpochMilli(Long.valueOf(values[3].substring(0,values[3].length() - 3))));
        }
    }
}