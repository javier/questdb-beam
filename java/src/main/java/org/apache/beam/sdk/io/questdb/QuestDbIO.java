package org.apache.beam.sdk.io.questdb;

import com.google.auto.value.AutoValue;
import io.questdb.client.Sender;
import io.questdb.client.Sender.LineSenderBuilder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.joda.time.Duration;
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
                .setDeduplicationEnabled(false)
                .setDeduplicationByValue(false)
                .setDeduplicationDurationMillis(1500L)
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
        abstract @Nullable String table();

        @Pure
        abstract boolean sslEnabled();

        @Pure
        abstract boolean authEnabled();

        @Pure
        abstract @Nullable String authUser();

        @Pure
        abstract @Nullable String authToken();

        @Pure
        abstract @Nullable Boolean deduplicationEnabled();

        @Pure
        abstract @Nullable Boolean deduplicationByValue();

        @Pure
        abstract @Nullable Long deduplicationDurationMillis();


        @Pure
        abstract QuestDbIO.Write.Builder builder();

        /**
         * Sets the host and port of the database instance.
         */
        public QuestDbIO.Write withUri(String uri) {
            checkArgument(uri != null, "uri can not be null");
            return builder().setUri(uri).build();
        }

        /**
         * Sets the table where to write data in the database.
         */
        public QuestDbIO.Write withTable(String table) {
            checkArgument(table != null, "table can not be null");
            return builder().setTable(table).build();
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
         * Sets the user name for authenticated connections
         */
        public QuestDbIO.Write withAuthUser(String user) {
            return builder().setAuthUser(user).build();
        }

        /**
         * Sets the secret token for authenticated connections
         */
        public QuestDbIO.Write withAuthToken(String token) {
            return builder().setAuthToken(token).build();
        }

        /**
         * Enable deduplication for connection.
         */
        public QuestDbIO.Write withDeduplicationEnabled(boolean deduplicationEnabled) {
            return builder().setDeduplicationEnabled(deduplicationEnabled).build();
        }

        /**
         * Ignore designated timestamp when doing deduplocation. If true, all the columns
         * except designated timestamp will be used for deduplication. If false, designated
         * timestamp will also need to match to be considered a duplicate.
         */
        public QuestDbIO.Write withDeduplicationByValue(boolean deduplicationByValue) {
            return builder().setDeduplicationByValue(deduplicationByValue).build();
        }

        /**
         * Deduplication interval in milliseconds
         */
        public QuestDbIO.Write withDeduplicationDurationMillis(Long deduplicationDurationMillis) {
            return builder().setDeduplicationDurationMillis(deduplicationDurationMillis).build();
        }


        @Override
        public PDone expand(PCollection<QuestDbRow> input) {
            checkArgument(uri() != null, "withUri() is required");
            checkArgument(table() != null, "withTable() is required");

            if (deduplicationEnabled()) {

                PCollection keydAndWindowed = null;

                if (deduplicationByValue()) {
                    keydAndWindowed = (PCollection) input.apply(WithKeys.of(new SerializableFunction<QuestDbRow, Integer >() {
                        @Override
                        public Integer apply(QuestDbRow r) {
                            return r.hashCodeWithoutDesignatedTimestamp();
                        }
                    }));
                } else {
                    keydAndWindowed = (PCollection) input.apply(WithKeys.of(new SerializableFunction<QuestDbRow, Integer>() {
                        @Override
                        public Integer apply(QuestDbRow r) {
                            return r.hashCode();
                        }
                    }));
                }
                PCollection windowedItems = (PCollection)
                        keydAndWindowed.apply(
                                Window.
                                        <KV<Integer, QuestDbRow>>into(
                                                Sessions.
                                                        withGapDuration(
                                                                Duration.standardSeconds(deduplicationDurationMillis())
                                                        )
                                        )
                        );

                PCollection<QuestDbRow> uniqueRows = (PCollection<QuestDbRow>)
                        ((PCollection) keydAndWindowed.apply(
                                Deduplicate.keyedValues().withDuration(
                                        Duration.standardSeconds(deduplicationDurationMillis()))
                        )
                        ).apply(Values.create());

                uniqueRows.apply(ParDo.of(new QuestDbIO.Write.WriteFn(this)));

            } else {
                input.apply(ParDo.of(new QuestDbIO.Write.WriteFn(this)));

            }
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

            abstract QuestDbIO.Write.Builder setTable(String table);

            abstract QuestDbIO.Write.Builder setSslEnabled(boolean value);

            abstract QuestDbIO.Write.Builder setAuthEnabled(boolean value);

            abstract QuestDbIO.Write.Builder setAuthUser(String value);

            abstract QuestDbIO.Write.Builder setAuthToken(String value);

            abstract QuestDbIO.Write.Builder setDeduplicationEnabled(Boolean value);

            abstract QuestDbIO.Write.Builder setDeduplicationByValue(Boolean value);

            abstract QuestDbIO.Write.Builder setDeduplicationDurationMillis(Long value);

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
                LineSenderBuilder senderBuilder = Sender.builder().address(spec.uri());
                if (spec.sslEnabled())
                    senderBuilder.enableTls();

                if (spec.authEnabled() && !spec.authUser().isBlank() && !spec.authToken().isBlank())
                    senderBuilder.enableAuth(spec.authUser()).authToken(spec.authToken());

                sender = senderBuilder.build();
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
                    sender.at(row.getDesignatedTimestamp());
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
