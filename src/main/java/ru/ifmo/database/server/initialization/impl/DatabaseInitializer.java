package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.logic.impl.DatabaseImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class DatabaseInitializer implements Initializer {
    private final Initializer tableInitializer;

    public DatabaseInitializer(Initializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        if (context.currentDbContext() == null) {
            throw new DatabaseException("Current database context is null.");
        }
        if (tableInitializer == null) {
            throw new DatabaseException("Table initializer haven't provided.");
        }

        for (var path : getDatabaseTables(context.currentDbContext().getDatabasePath())) {
            var tableInitializationContext = new TableInitializationContextImpl(
                    path.getFileName().toString(),
                    path,
                    new TableIndex()
            );
            tableInitializer.perform(
                    InitializationContextImpl.builder()
                            .executionEnvironment(context.executionEnvironment())
                            .databaseInitializationContext(context.currentDbContext())
                            .tableInitializationContext(tableInitializationContext)
                            .build()
            );
        }
        var database = DatabaseImpl.initializeFromContext(context.currentDbContext());
        context.executionEnvironment().addDatabase(database);
    }

    private List<Path> getDatabaseTables(Path databasePath) throws DatabaseException {
        try {
            return Files.list(databasePath)
                    .filter(Files::isDirectory)
                    .collect(Collectors.toList());
        } catch (IOException exception) {
            throw new DatabaseException(exception.getMessage());
        }
    }


}