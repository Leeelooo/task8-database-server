package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {
    private final String tableName;
    private final Path tablePath;
    private final TableIndex tableIndex;
    private Segment currentSegment;

    public static Table initializeFromContext(TableInitializationContext context) throws DatabaseException {
        return create(
                context.getTableName(),
                context.getTablePath(),
                context.getTableIndex(),
                context.getCurrentSegment()
        );
    }

    static Table create(
            String tableName,
            Path tablePath,
            TableIndex tableIndex,
            Segment currentSegment
    ) throws DatabaseException {
        if (!Files.isDirectory(tablePath)) {
            throw new DatabaseException(
                    String.format(
                            "There is no %s table.",
                            tableName
                    )
            );
        }
        return new TableImpl(tableName, tablePath, tableIndex, currentSegment);
    }

    static Segment createNewSegment(Path tablePath) throws DatabaseException {
        var segmentName = SegmentImpl.createSegmentName(tablePath.getFileName().toString());
        var segmentPath = tablePath.resolve(segmentName);
        try {
            Files.createFile(segmentPath);
        } catch (IOException exception) {
            throw new DatabaseException(exception.getMessage());
        }

        return SegmentImpl.create(
                segmentName,
                segmentPath,
                0,
                new SegmentIndex()
        );
    }

    private TableImpl(
            String tableName,
            Path databasePath,
            TableIndex tableIndex,
            Segment currentSegment
    ) {
        this.tableName = tableName;
        this.tablePath = databasePath;
        this.tableIndex = tableIndex;
        this.currentSegment = currentSegment;
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        if (currentSegment == null || currentSegment.isReadOnly()) {
            currentSegment = createNewSegment(tablePath);
        }
        try {
            currentSegment.write(objectKey, objectValue);
        } catch (IOException exception) {
            throw new DatabaseException(exception.getMessage());
        }
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        var value =
                tableIndex.searchForKey(objectKey)
                        .map(segment -> {
                            try {
                                return segment.read(objectKey);
                            } catch (IOException exception) {
                                return null;
                            }
                        });
        if (value.isEmpty()) {
            throw new DatabaseException("No such key in table.");
        }
        return value.get();
    }

}