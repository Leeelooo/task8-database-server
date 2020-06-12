package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DatabaseImpl implements Database {
    private final String dbName;
    private final Path databaseRoot;
    private final Map<String, Table> tables;

    public static Database initializeFromContext(DatabaseInitializationContext context) throws DatabaseException {
        return create(context.getDbName(), context.getDatabasePath(), context.getTables());
    }

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        return create(dbName, databaseRoot, new HashMap<>());
    }

    public static Database create(String dbName, Path databaseRoot, Map<String, Table> tables) throws DatabaseException {
        return new DatabaseImpl(dbName, databaseRoot, tables);
    }

    private DatabaseImpl(String dbName, Path databaseRoot, Map<String, Table> tables) {
        this.dbName = dbName;
        this.databaseRoot = databaseRoot;
        this.tables = tables;
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        createTableIfNotExists(tableName, Segment.DEFAULT_SIZE_IN_BYTES);
    }

    @Override
    public void createTableIfNotExists(String tableName, int segmentSizeInBytes) throws DatabaseException {
        var path = databaseRoot.resolve(dbName).resolve(tableName);
        if (Files.isDirectory(path)) {
            throw new DatabaseException(String.format("Table with name %s already exists.", tableName));
        }

        try {
            Files.createDirectory(path);
        } catch (Exception exception) {
            throw new DatabaseException(exception);
        }

        var segment = TableImpl.createNewSegment(path);
        var table = TableImpl.create(
                tableName,
                path,
                new TableIndex(),
                segment
        );
        tables.put(tableName, table);
    }

    @Override
    public void write(String tableName, String objectKey, String objectValue) throws DatabaseException {
        var table = tables.get(tableName);
        if (table == null) {
            throw new DatabaseException(String.format("No such table %s", tableName));
        }
        table.write(objectKey, objectValue);
    }

    @Override
    public String read(String tableName, String objectKey) throws DatabaseException {
        var table = tables.get(tableName);
        if (table == null) {
            throw new DatabaseException(String.format("No such table %s", tableName));
        }
        return table.read(objectKey);
    }
}
