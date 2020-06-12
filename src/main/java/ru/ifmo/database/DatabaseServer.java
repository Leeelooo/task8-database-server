package ru.ifmo.database;

import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.DatabaseCommands;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.console.impl.ExecutionEnvironmentImpl;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.initialization.impl.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {
    private static ExecutorService executor = Executors.newSingleThreadExecutor();

    private final ExecutionEnvironment environment;

    public DatabaseServer(ExecutionEnvironment env, Initializer initializer) throws IOException, DatabaseException {
        environment = env;
        if (Files.isDirectory(env.getWorkingPath())) {
            var initializationContext = InitializationContextImpl.builder()
                    .executionEnvironment(env)
                    .build();
            initializer.perform(initializationContext);
        } else {
            Files.createDirectory(env.getWorkingPath());
        }
    }

    public static void main(String[] args) throws IOException, DatabaseException {
        Initializer initializer = new DatabaseServerInitializer(
                new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));
        DatabaseServer databaseServer = new DatabaseServer(new ExecutionEnvironmentImpl(), initializer);
    }

    public DatabaseCommandResult executeNextCommand(String commandText) {
        return executeNextCommand(DatabaseCommands.of(environment, commandText));
    }

    public DatabaseCommandResult executeNextCommand(DatabaseCommand command) {
        return command.execute();
    }
}
