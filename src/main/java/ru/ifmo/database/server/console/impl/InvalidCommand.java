package ru.ifmo.database.server.console.impl;

import ru.ifmo.database.server.console.DatabaseCommand;
import ru.ifmo.database.server.console.DatabaseCommandResult;

public class InvalidCommand implements DatabaseCommand {
    private final String cause;

    public InvalidCommand(String cause) {
        this.cause = cause;
    }

    @Override
    public DatabaseCommandResult execute() {
        return DatabaseCommandResult.error(cause);
    }

}
