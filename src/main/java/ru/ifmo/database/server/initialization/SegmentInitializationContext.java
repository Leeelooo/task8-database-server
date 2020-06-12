package ru.ifmo.database.server.initialization;

import ru.ifmo.database.server.index.impl.SegmentIndex;

import java.nio.file.Path;

public interface SegmentInitializationContext {
    String getSegmentName();

    Path getSegmentPath();

    SegmentIndex getIndex();

    int getCurrentSize(); // todo sukhoa should be long
}
