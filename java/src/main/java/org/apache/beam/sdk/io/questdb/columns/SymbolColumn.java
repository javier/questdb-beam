package org.apache.beam.sdk.io.questdb.columns;

public class SymbolColumn implements QuestDbColumn<String>{
    private String value;

    public SymbolColumn(String value) {
        this.value = value;
    }

    @Override
    public String stringValue() {
        return value;
    }

    @Override
    public Object objectValue() {
        return value;
    }

    @Override
    public String get() {
        return value;
    }
}
