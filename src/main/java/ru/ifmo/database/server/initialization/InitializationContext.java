package ru.ifmo.database.server.initialization;

import ru.ifmo.database.server.console.ExecutionEnvironment;

public interface InitializationContext {
    ExecutionEnvironment executionEnvironment();

    DatabaseInitializationContext currentDbContext();

    TableInitializationContext currentTableContext();

    SegmentInitializationContext currentSegmentContext();
}
