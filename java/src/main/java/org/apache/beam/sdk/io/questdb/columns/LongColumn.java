package org.apache.beam.sdk.io.questdb.columns;

public class LongColumn implements QuestDbColumn<Long>{
    private Long value;

    public LongColumn(Long value) {
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
