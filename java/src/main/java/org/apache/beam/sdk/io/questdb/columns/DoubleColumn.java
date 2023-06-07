package org.apache.beam.sdk.io.questdb.columns;

public class DoubleColumn implements QuestDbColumn<Double>{
    private Double value;

    public DoubleColumn(Double value) {
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
    public Double get() {
        return value;
    }
}
