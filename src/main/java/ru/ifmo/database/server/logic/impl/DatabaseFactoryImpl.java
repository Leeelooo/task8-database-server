package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.DatabaseFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class DatabaseFactoryImpl implements DatabaseFactory {

    @Override
    public Database createNonExistent(String dbName, Path dbRoot) throws DatabaseException {
        var path = dbRoot.resolve(dbName);
        if (Files.isDirectory(path)) {
            throw new DatabaseException(String.format("Database with name %s already exists.", dbName));
        }

        try {
            Files.createDirectory(path);
        } catch (Exception exception) {
            throw new DatabaseException(exception);
        }
        return DatabaseImpl.create(dbName, dbRoot, new HashMap<>());
    }

}
