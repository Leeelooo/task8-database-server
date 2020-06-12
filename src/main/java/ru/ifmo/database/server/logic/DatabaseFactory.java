package ru.ifmo.database.server.logic;

import ru.ifmo.database.server.exception.DatabaseException;

import java.nio.file.Path;

@FunctionalInterface
public interface DatabaseFactory {
    Database createNonExistent(String dbName, Path dbRoot) throws DatabaseException;
}
