package ru.ifmo.database.server.initialization;

import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.logic.Segment;

import java.nio.file.Path;

public interface TableInitializationContext {
    String getTableName();

    Path getTablePath();

    TableIndex getTableIndex();

    Segment getCurrentSegment();

    void updateCurrentSegment(Segment segment); // todo sukhoa refactor?
}
