package org.apache.beam.sdk.io.questdb;

import com.google.auto.value.AutoValue;
import io.questdb.client.Sender;
import org.apache.beam.sdk.io.questdb.columns.QuestDbColumn;
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

import java.util.List;
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
    public abstract static class Write extends PTransform<PCollection<Map<String, QuestDbColumn>>, PDone> {

        @Pure
        abstract @Nullable String uri();

        @Pure
        abstract boolean sslEnabled();

        @Pure
        abstract boolean authEnabled();

        @Pure
        abstract @Nullable String table();

        @Pure
        abstract @Nullable List<String> symbolColumns();

        @Pure
        abstract @Nullable List<String> stringColumns();

        @Pure
        abstract @Nullable List<String> longColumns();

        @Pure
        abstract @Nullable List<String> doubleColumns();

        @Pure
        abstract @Nullable List<String> boolColumns();

        @Pure
        abstract @Nullable List<String> timestampColumns();

        @Pure
        abstract @Nullable String designatedTimestampColumn();

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

        public QuestDbIO.Write withSymbolColumns(List<String> columns) {
            return builder().setSymbolColumns(columns).build();
        }

        public QuestDbIO.Write withStringColumns(List<String> columns) {
            return builder().setStringColumns(columns).build();
        }

        public QuestDbIO.Write withLongColumns(List<String> columns) {
            return builder().setLongColumns(columns).build();
        }

        public QuestDbIO.Write withDoubleColumns(List<String> columns) {
            return builder().setDoubleColumns(columns).build();
        }

        public QuestDbIO.Write withBoolColumns(List<String> columns) {
            return builder().setBoolColumns(columns).build();
        }

        public QuestDbIO.Write withTimestampColumns(List<String> columns) {
            return builder().setTimestampColumns(columns).build();
        }

        public QuestDbIO.Write withDesignatedTimestampColumn(String column) {
            return builder().setDesignatedTimestampColumn(column).build();
        }

        @Override
        public PDone expand(PCollection<Map<String, QuestDbColumn>> input) {
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

            abstract QuestDbIO.Write.Builder setSymbolColumns(List<String> columns);

            abstract QuestDbIO.Write.Builder setStringColumns(List<String> columns);

            abstract QuestDbIO.Write.Builder setLongColumns(List<String> columns);

            abstract QuestDbIO.Write.Builder setDoubleColumns(List<String> columns);

            abstract QuestDbIO.Write.Builder setBoolColumns(List<String> columns);

            abstract QuestDbIO.Write.Builder setTimestampColumns(List<String> columns);

            abstract QuestDbIO.Write.Builder setDesignatedTimestampColumn(String column);

            abstract QuestDbIO.Write build();
        }

        static class WriteFn extends DoFn<Map<String, QuestDbColumn>, Void> {
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
                if (spec.symbolColumns() != null) {
                    for (String column : spec.symbolColumns())
                        sender.symbol(column, (String) ctx.element().get(column).get());
                }
                if (spec.stringColumns() != null) {
                    for (String column : spec.stringColumns())
                        sender.stringColumn(column, (String) ctx.element().get(column).get());
                }
                if (spec.longColumns() != null) {
                    for (String column : spec.longColumns())
                        sender.longColumn(column, (Long) ctx.element().get(column).get());
                }
                if (spec.doubleColumns() != null) {
                    for (String column : spec.doubleColumns())
                        sender.doubleColumn(column, (Double) ctx.element().get(column).get());
                }
                if (spec.timestampColumns() != null) {
                    for (String column : spec.timestampColumns())
                        sender.timestampColumn(column, (Long) ctx.element().get(column).get());
                }
                if (spec.boolColumns() != null) {
                    for (String column : spec.boolColumns())
                        sender.boolColumn(column, (boolean) ctx.element().get(column).get());
                }

                if (spec.designatedTimestampColumn() != null) {
                    sender.at( (Long) ctx.element().get(spec.designatedTimestampColumn()).get());
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
