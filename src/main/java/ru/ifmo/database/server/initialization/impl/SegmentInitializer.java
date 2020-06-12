package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndexInfoImpl;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.logic.impl.DatabaseInputStream;
import ru.ifmo.database.server.logic.impl.SegmentImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

public class SegmentInitializer implements Initializer {

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        int currentOffset = 0;
        Set<String> keysInSegment = new HashSet<>();

        try (final var inputStream = new DatabaseInputStream(Files
                .newInputStream(context.currentSegmentContext().getSegmentPath()))) {
            while (currentOffset < context.currentSegmentContext().getCurrentSize() - 1) {
                var storingUnit = inputStream.readDbUnit();
                if (storingUnit.isEmpty()) {
                    throw new DatabaseException("Error reading segment.");
                }

                var key = new String(storingUnit.get().getKey());
                context.currentSegmentContext()
                        .getIndex()
                        .onIndexedEntityUpdated(
                                key,
                                new SegmentIndexInfoImpl(currentOffset)
                        );

                keysInSegment.add(key);
                currentOffset += storingUnit.get().getUnitLength();
            }
        } catch (IOException exception) {
            throw new DatabaseException(exception.getMessage());
        }

        var segment = SegmentImpl.create(context.currentSegmentContext());
        context.currentTableContext().updateCurrentSegment(segment);
        for (var key : keysInSegment) {
            context.currentTableContext()
                    .getTableIndex()
                    .onIndexedEntityUpdated(
                            key,
                            segment
                    );
        }
    }

}
