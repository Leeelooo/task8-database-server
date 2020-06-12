package ru.ifmo.database.server.logic.impl;

import lombok.Getter;

@Getter
public class DatabaseStoringUnit {

    private final byte[] key;
    private final int keySize;

    private final byte[] value;
    private final int valueSize;

    public DatabaseStoringUnit(String objectKey, String objectValue) {
        this(objectKey.getBytes(), objectValue.getBytes());
    }

    public DatabaseStoringUnit(byte[] key, byte[] value) {
        this.key = key;
        keySize = key.length;
        this.value = value;
        valueSize = value.length;
    }

    public int getUnitLength() {
        return valueSize + keySize + 2 * 4;
    }

}
