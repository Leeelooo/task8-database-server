package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {
    private final Map<String, Table> tables;
    private final String databaseName;
    private final Path databasePath;

    public DatabaseInitializationContextImpl(String databaseName, Path databaseRoot) {
        this.databaseName = databaseName;
        this.databasePath = databaseRoot;
        this.tables = new HashMap<>();
    }

    @Override
    public String getDbName() {
        return databaseName;
    }

    @Override
    public Path getDatabasePath() {
        return databasePath;
    }

    @Override
    public Map<String, Table> getTables() {
        return tables;
    }

    @Override
    public void addTable(Table table) {
        tables.put(table.getName(), table);
    }
}
