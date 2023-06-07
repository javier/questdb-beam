// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package org.apache.beam.sdk.io.questdb;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.questdb.columns.LongColumn;
import org.apache.beam.sdk.io.questdb.columns.QuestDbColumn;
import org.apache.beam.sdk.io.questdb.columns.SymbolColumn;
import org.apache.beam.sdk.io.questdb.columns.TimestampColumn;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        parsedLines.apply(QuestDbIO.write()
                .withUri("localhost:9009")
                .withTable("author2")
                .withSymbolColumns(List.of("user_id", "team_id"))
                .withLongColumns(List.of("score"))
                .withDesignatedTimestampColumn("timestampED")
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
}