package ru.ifmo.database.server.logic.impl;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

public class DatabaseInputStream extends DataInputStream {

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    public Optional<DatabaseStoringUnit> readDbUnit() throws IOException {
        var keyLength = ByteBuffer.wrap(readNBytes(4)).getInt();
        var key = readNBytes(keyLength);
        var valueLength = ByteBuffer.wrap(readNBytes(4)).getInt();
        var value = readNBytes(valueLength);
        return Optional.of(new DatabaseStoringUnit(key, value));
    }
}
