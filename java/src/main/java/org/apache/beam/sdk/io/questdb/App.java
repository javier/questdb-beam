// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package org.apache.beam.sdk.io.questdb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.kafka.common.serialization.StringDeserializer;

public class App {
	public interface Options extends StreamingOptions {
		@Description("Kafka topic to read from")
		@Default.String("echo-output")
		String getInputTopic();

		void setInputTopic(String value);
	}

	static class LineToMap extends DoFn<String, Map> {
		@ProcessElement
		public void processElement(@Element String element, OutputReceiver<Map> receiver) throws Exception {
			Map<String, Object> elementMap = new HashMap<String, Object>();
			String[] values = element.split(",");
			elementMap.put("user_id", values[0]);
			elementMap.put("team_id", values[1]);
			elementMap.put("score", Integer.valueOf(values[2]));
			elementMap.put("timestamp", Long.valueOf(values[2]) * 1000000L);
			receiver.output(elementMap);
		}
	}

	public static void buildKafkaPipeline(Pipeline pipeline, String topic) {
		  PCollection res = pipeline
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
				)
			  .apply("Convert elements",
					  MapElements.via(
							  new SimpleFunction<String, Map<String, String>>() {
								  @Override
								  public Map apply(String element) {
									  Map elementMap = new HashMap();
									  String[] values = element.split(",");
									  elementMap.put("user_id", values[0]);
									  elementMap.put("team_id", values[1]);
									  elementMap.put("score", values[2]);
									  elementMap.put("timestampED", values[3].concat("000000"));
									  return elementMap;
								  }
							  }
					  )
			  )  ;

		        res.apply(QuestDbIO.write()
						.withUri("localhost:9009")
						.withTable("author2")
						.withSymbolColumns(List.of("user_id"))
						.withStringColumns(List.of("team_id"))
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
}