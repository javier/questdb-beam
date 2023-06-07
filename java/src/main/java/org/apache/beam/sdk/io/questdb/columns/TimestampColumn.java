package org.apache.beam.sdk.io.questdb.columns;

public class TimestampColumn implements QuestDbColumn<Long>{
    private Long value;

    public TimestampColumn(Long value) {
        this.value = value;
    }

    @Override
    public String stringValue() {
        return String.valueOf(value);
    }

    @Override
    public Object objectValue() {
        return value;
    }

    @Override
    public Long get() {
        return value;
    }
}
