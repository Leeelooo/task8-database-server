package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.logic.impl.TableImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TableInitializer implements Initializer {
    private final Initializer segmentInitializer;

    public TableInitializer(Initializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        if (context.currentTableContext() == null) {
            throw new DatabaseException("Current table context is null.");
        }
        if (segmentInitializer == null) {
            throw new DatabaseException("Segment initializer haven't provided.");
        }

        var segments = getTableSegmentsWithSize(context.currentTableContext().getTablePath());
        List<Path> paths = new ArrayList<>(segments.keySet());
        Collections.sort(paths);

        for (var path : paths) {
            var segmentInitializerContext = new SegmentInitializationContextImpl(
                    path.getFileName().toString(),
                    path,
                    segments.get(path),
                    new SegmentIndex()
            );
            segmentInitializer.perform(
                    InitializationContextImpl.builder()
                            .executionEnvironment(context.executionEnvironment())
                            .databaseInitializationContext(context.currentDbContext())
                            .tableInitializationContext(context.currentTableContext())
                            .segmentInitializationContext(segmentInitializerContext)
                            .build()
            );
        }
        var table = TableImpl.initializeFromContext(context.currentTableContext());
        context.currentDbContext().addTable(table);
    }

    private Map<Path, Integer> getTableSegmentsWithSize(Path tablePath) throws DatabaseException {
        try {
            return Files.list(tablePath)
                    .filter(path -> !Files.isDirectory(path))
                    .collect(
                            Collectors.toMap(
                                    Function.identity(),
                                    path -> {
                                        try {
                                            return (int) Files.size(path);
                                        } catch (IOException exception) {
                                            return -1;
                                        }
                                    }
                            )
                    );
        } catch (IOException exception) {
            throw new DatabaseException(exception.getMessage());
        }
    }

}
