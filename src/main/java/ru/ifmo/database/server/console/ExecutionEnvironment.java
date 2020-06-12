package ru.ifmo.database.server.console;

import ru.ifmo.database.server.logic.Database;

import java.nio.file.Path;
import java.util.Optional;

public interface ExecutionEnvironment {
    Path getWorkingPath();

    Optional<Database> getDatabase(String name);

    void addDatabase(Database db);
}
