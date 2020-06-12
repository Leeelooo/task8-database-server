package ru.ifmo.database.server.initialization.impl;

import lombok.Builder;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.initialization.TableInitializationContext;

@Builder
public class InitializationContextImpl implements InitializationContext {
    private final ExecutionEnvironment executionEnvironment;
    private final DatabaseInitializationContext databaseInitializationContext;
    private final TableInitializationContext tableInitializationContext;
    private final SegmentInitializationContext segmentInitializationContext;

    private InitializationContextImpl(
            ExecutionEnvironment executionEnvironment,
            DatabaseInitializationContext currentDatabaseContext,
            TableInitializationContext currentTableContext,
            SegmentInitializationContext currentSegmentContext
    ) {
        this.executionEnvironment = executionEnvironment;
        this.databaseInitializationContext = currentDatabaseContext;
        this.tableInitializationContext = currentTableContext;
        this.segmentInitializationContext = currentSegmentContext;
    }

    @Override
    public ExecutionEnvironment executionEnvironment() {
        return executionEnvironment;
    }

    @Override
    public DatabaseInitializationContext currentDbContext() {
        return databaseInitializationContext;
    }

    @Override
    public TableInitializationContext currentTableContext() {
        return tableInitializationContext;
    }

    @Override
    public SegmentInitializationContext currentSegmentContext() {
        return segmentInitializationContext;
    }
}