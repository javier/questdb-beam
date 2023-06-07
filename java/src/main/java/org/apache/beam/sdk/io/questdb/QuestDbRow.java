package org.apache.beam.sdk.io.questdb;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class QuestDbRow implements Serializable {
    private final Map<String, String> symbolColumns = new HashMap<>();
    private final Map<String, String> stringColumns = new HashMap<>();
    private final Map<String, Long> longColumns = new HashMap<>();
    private final Map<String, Double> doubleColumns = new HashMap<>();
    private final Map<String, Boolean> booleanColumns = new HashMap<>();
    private final Map<String, Long> timestampColumns = new HashMap<>();
    private Long designatedTimestamp = null;

    public QuestDbRow() {}

    public boolean hasSymbolColumns() {
        return !symbolColumns.isEmpty();
    }

    public boolean hasStringColumns() {
        return !stringColumns.isEmpty();
    }

    public boolean hasLongColumns() {
        return !longColumns.isEmpty();
    }

    public boolean hasDoubleColumns() {
        return !doubleColumns.isEmpty();
    }

    public boolean hasBooleanColumns() {
        return !booleanColumns.isEmpty();
    }

    public boolean hasTimestampColumns() {
        return !timestampColumns.isEmpty();
    }

    public boolean hasDesignatedTimestamp() {
        return designatedTimestamp != null;
    }

    public QuestDbRow putSymbol(String name, String value) {
        symbolColumns.put(name, value);
        return this;
    }

    public QuestDbRow putString(String name, String value) {
        stringColumns.put(name, value);
        return this;
    }

    public QuestDbRow putLong(String name, Long value) {
        longColumns.put(name, value);
        return this;
    }

    public QuestDbRow putLong(String name, String value) {
        return putLong(name, Long.valueOf(value));
    }

    public QuestDbRow putDouble(String name, Double value) {
        doubleColumns.put(name, value);
        return this;
    }

    public QuestDbRow putDouble(String name, String value) {
        return putDouble(name, Double.valueOf(value));
    }

    public QuestDbRow putBoolean(String name, Boolean value) {
        booleanColumns.put(name, value);
        return this;
    }

    public QuestDbRow putBoolean(String name, String value) {
        return putBoolean(name, Boolean.valueOf(value.toLowerCase()));
    }

    public QuestDbRow putTimestamp(String name, Long value) {
        timestampColumns.put(name, value);
        return this;
    }

    public QuestDbRow putTimestamp(String name, String value) {
        return putTimestamp(name, Long.valueOf(value));
    }

    public QuestDbRow putTimestampMs(String name, Long value) {
        return putTimestamp(name, value * 1000L );
    }

    public QuestDbRow putTimestampMs(String name, String value) {
        return putTimestampMs(name, Long.valueOf(value) );
    }

    public QuestDbRow setDesignatedTimestamp(Long value) {
        designatedTimestamp = value;
        return this;
    }

    public QuestDbRow setDesignatedTimestamp(String value) {
        return setDesignatedTimestamp(Long.valueOf(value));
    }

    public QuestDbRow setDesignatedTimestampMs(Long value) {
        return setDesignatedTimestamp(value * 1000000L);
    }

    public QuestDbRow setDesignatedTimestampMs( String value) {
        return setDesignatedTimestampMs(Long.valueOf(value));
    }

    public Map<String, String> getSymbolColumns() {
        return symbolColumns;
    }

    public Map<String, String> getStringColumns() {
        return stringColumns;
    }

    public Map<String, Long> getLongColumns() {
        return longColumns;
    }

    public Map<String, Double> getDoubleColumns() {
        return doubleColumns;
    }

    public Map<String, Boolean> getBooleanColumns() {
        return booleanColumns;
    }

    public Map<String, Long> getTimestampColumns() {
        return timestampColumns;
    }

    public Long getDesignatedTimestamp() {
        return designatedTimestamp;
    }

    public boolean equals(Object otherObject) {
        if ( !(otherObject instanceof QuestDbRow)) return false;

        QuestDbRow other = (QuestDbRow) otherObject;
        return this.getBooleanColumns().equals(other.getBooleanColumns()) &&
                this.getLongColumns().equals(other.getLongColumns()) &&
                this.getDoubleColumns().equals(other.getDoubleColumns()) &&
                this.getTimestampColumns().equals(other.getDoubleColumns()) &&
                this.getSymbolColumns().equals(other.getSymbolColumns()) &&
                this.getStringColumns().equals(other.getStringColumns()) &&
                Objects.equals(this.getDesignatedTimestamp(), other.getDesignatedTimestamp())
                ;
    }

}
