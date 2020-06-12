package ru.ifmo.database.server.logic.impl;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class DatabaseOutputStream extends DataOutputStream {

    public DatabaseOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    void write(DatabaseStoringUnit storingUnit) throws IOException {
        var newRecord = ByteBuffer.allocate(storingUnit.getUnitLength())
                .putInt(storingUnit.getKeySize())
                .put(storingUnit.getKey())
                .putInt(storingUnit.getValueSize())
                .put(storingUnit.getValue())
                .array();
        write(newRecord);
    }
}