package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class DatabaseServerInitializer implements Initializer {
    private final Initializer databaseInitializer;

    public DatabaseServerInitializer(Initializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        if (context.executionEnvironment() == null) {
            throw new DatabaseException("Execution environment haven't provided.");
        }
        if (databaseInitializer == null) {
            throw new DatabaseException("Database initializer haven't provided.");
        }

        for (var path : getDatabaseDirectories(context.executionEnvironment().getWorkingPath())) {
            var databaseInitializationContext = new DatabaseInitializationContextImpl(
                    path.getFileName().toString(),
                    path
            );
            databaseInitializer.perform(
                    InitializationContextImpl.builder()
                            .executionEnvironment(context.executionEnvironment())
                            .databaseInitializationContext(databaseInitializationContext)
                            .build()
            );
        }
    }

    private List<Path> getDatabaseDirectories(Path workingPath) throws DatabaseException {
        try {
            return Files.list(workingPath)
                    .filter(Files::isDirectory)
                    .collect(Collectors.toList());
        } catch (IOException exception) {
            throw new DatabaseException(exception.getMessage());
        }
    }

}