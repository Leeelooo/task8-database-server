package ru.ifmo.database.server.initialization;

import ru.ifmo.database.server.exception.DatabaseException;

public interface Initializer {
    void perform(InitializationContext context) throws DatabaseException;
}
