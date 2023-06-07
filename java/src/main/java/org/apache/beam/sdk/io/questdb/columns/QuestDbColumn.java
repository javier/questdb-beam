package org.apache.beam.sdk.io.questdb.columns;

import java.io.Serializable;

public interface QuestDbColumn<T> extends Serializable {
    String stringValue();
    Object objectValue();
    T get();
}
