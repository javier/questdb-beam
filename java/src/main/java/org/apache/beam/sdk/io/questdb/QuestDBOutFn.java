package org.apache.beam.sdk.io.questdb;


import org.apache.beam.sdk.transforms.DoFn;
import io.questdb.client.Sender;

public class QuestDBOutFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String word, OutputReceiver<String> out) {
        // Use OutputReceiver.output to emit the output element.
        out.output(word.toUpperCase());

        try (Sender sender = Sender.builder().address("localhost:9009").build()) {
            sender.table("inventors");
            sender.symbol("word", word);
            sender.longColumn("id", 100);
                    sender.stringColumn("name", word.toUpperCase());
            sender.atNow();

        }
    }
}
