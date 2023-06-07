package org.apache.beam.sdk.io.questdb.columns;

public class BoolColumn implements QuestDbColumn<Boolean>{
    private boolean value;

    public BoolColumn(boolean value) {
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
    public Boolean get() {
        return value;
    }
}
