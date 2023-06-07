package org.apache.beam.sdk.io.questdb;

import com.google.auto.value.AutoValue;
import io.questdb.client.Sender;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;


public class QuestDbIO {
    private static final Logger LOG = LoggerFactory.getLogger(QuestDbIO.class);

    private QuestDbIO() {
    }

    /**
     * Write data to QuestDB.
     */
    public static QuestDbIO.Write write() {
        return new AutoValue_QuestDbIO_Write.Builder()
                .setSslEnabled(false)
                .setAuthEnabled(false)
                .build();
    }

    /**
     * A {@link PTransform} to write to a QuestDB database.
     */
    @AutoValue
    public abstract static class Write extends PTransform<PCollection<QuestDbRow>, PDone> {

        @Pure
        abstract @Nullable String uri();

        @Pure
        abstract boolean sslEnabled();

        @Pure
        abstract boolean authEnabled();

        @Pure
        abstract @Nullable String table();

        @Pure
        abstract QuestDbIO.Write.Builder builder();

        public QuestDbIO.Write withUri(String uri) {
            checkArgument(uri != null, "uri can not be null");
            return builder().setUri(uri).build();
        }

        /**
         * Enable ssl for connection.
         */
        public QuestDbIO.Write withSSLEnabled(boolean sslEnabled) {
            return builder().setSslEnabled(sslEnabled).build();
        }

        /**
         * Enable auth for connection.
         */
        public QuestDbIO.Write withAuthEnabled(boolean authEnabled) {
            return builder().setAuthEnabled(authEnabled).build();
        }

        /**
         * Sets the collection where to write data in the database.
         */
        public QuestDbIO.Write withTable(String table) {
            checkArgument(table != null, "table can not be null");
            return builder().setTable(table).build();
        }


        @Override
        public PDone expand(PCollection<QuestDbRow> input) {
            checkArgument(uri() != null, "withUri() is required");
            checkArgument(table() != null, "withTable() is required");

            input.apply(ParDo.of(new QuestDbIO.Write.WriteFn(this)));
            return PDone.in(input.getPipeline());
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("uri", uri()));
            builder.add(DisplayData.item("sslEnable", sslEnabled()));
            builder.add(DisplayData.item("authEnable", authEnabled()));
        }

        @AutoValue.Builder
        abstract static class Builder {
            abstract QuestDbIO.Write.Builder setUri(String uri);

            abstract QuestDbIO.Write.Builder setSslEnabled(boolean value);

            abstract QuestDbIO.Write.Builder setAuthEnabled(boolean value);

            abstract QuestDbIO.Write.Builder setTable(String table);

            abstract QuestDbIO.Write build();
        }

        static class WriteFn extends DoFn<QuestDbRow, Void> {
            private final QuestDbIO.Write spec;
            private transient @Nullable Sender sender;

            WriteFn(QuestDbIO.Write spec) {
                this.spec = spec;
            }

            @Setup
            public void createQuestDbClient() {
                String uri = Preconditions.checkStateNotNull(spec.uri());
                sender = Sender.builder().address(spec.uri()).build(); //TODO
            }

            @StartBundle
            public void startBundle() {
                if (sender == null) createQuestDbClient();
            }

            @ProcessElement
            public void processElement(ProcessContext ctx) {
                sender.table(spec.table());
                QuestDbRow row = ctx.element();
                if (row.hasSymbolColumns()) {
                    for (Map.Entry<String, String> entry : row.getSymbolColumns().entrySet())
                        sender.symbol(entry.getKey(), entry.getValue());
                }
                if (row.hasStringColumns()) {
                    for (Map.Entry<String, String> entry : row.getStringColumns().entrySet())
                        sender.symbol(entry.getKey(), entry.getValue());
                }
                if (row.hasLongColumns()) {
                    for (Map.Entry<String, Long> entry : row.getLongColumns().entrySet())
                        sender.longColumn(entry.getKey(), entry.getValue());
                }
                if (row.hasDoubleColumns()) {
                    for (Map.Entry<String, Double> entry : row.getDoubleColumns().entrySet())
                        sender.doubleColumn(entry.getKey(), entry.getValue());
                }
                if (row.hasTimestampColumns()) {
                    for (Map.Entry<String, Long> entry : row.getTimestampColumns().entrySet())
                        sender.timestampColumn(entry.getKey(), entry.getValue());
                }
                if (row.hasBooleanColumns()) {
                    for (Map.Entry<String, Boolean> entry : row.getBooleanColumns().entrySet())
                        sender.boolColumn(entry.getKey(), entry.getValue());
                }
                if (row.hasDesignatedTimestamp()) {
                    sender.at( row.getDesignatedTimestamp());
                } else {
                    sender.atNow();
                }

            }

            @FinishBundle
            public void finishBundle() {
                flush();
            }

            private void flush() {
                sender.flush();
            }


            @Teardown
            public void closeQuestDBClient() {
                if (sender != null) {
                    sender.flush();
                    sender.close();
                    sender = null;
                }
            }
        }
    }
}
