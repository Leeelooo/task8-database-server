package ru.ifmo.database.server.console.impl;

import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.logic.DatabaseFactory;

public final class CreateDatabaseCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final DatabaseFactory databaseFactory;
    private final String databaseName;

    public CreateDatabaseCommand(
            ExecutionEnvironment env,
            DatabaseFactory factory,
            String databaseName
    ) {
        this.env = env;
        this.databaseFactory = factory;
        this.databaseName = databaseName;
    }

    @Override
    public DatabaseCommandResult execute() {
        try {
            env.addDatabase(databaseFactory.createNonExistent(databaseName, env.getWorkingPath()));
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e.getMessage());
        }
        return DatabaseCommandResult.success("Database: " + databaseName + "created");
    }
}
