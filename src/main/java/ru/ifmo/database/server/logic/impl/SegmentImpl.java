package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.index.impl.SegmentIndexInfoImpl;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.logic.Segment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {
    private final SegmentIndex segmentIndex;
    private final String segmentName;
    private final Path segmentPath;
    private int segmentSize;

    private SegmentImpl(
            String segmentName,
            Path segmentPath,
            int segmentSize,
            SegmentIndex segmentIndex
    ) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.segmentSize = segmentSize;
        this.segmentIndex = segmentIndex;
    }

    public static Segment create(SegmentInitializationContext context) throws DatabaseException {
        return create(
                context.getSegmentName(),
                context.getSegmentPath(),
                context.getCurrentSize(),
                context.getIndex()
        );
    }

    static Segment create(
            String segmentName,
            Path segmentPath,
            int segmentSize,
            SegmentIndex segmentIndex
    ) throws DatabaseException {
        if (!Files.exists(segmentPath)) {
             throw new DatabaseException("There is no such segment.");
        }
        return new SegmentImpl(segmentName, segmentPath, segmentSize, segmentIndex);
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, String objectValue) throws IOException, DatabaseException {
        if (isReadOnly()) {
            throw new DatabaseException("Segment is full.");
        }

        var storingUnit = new DatabaseStoringUnit(objectKey, objectValue);
        try (final var outputStream = new DatabaseOutputStream(Files.newOutputStream(segmentPath))) {
            outputStream.write(storingUnit);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            return false;
        }
        segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentIndexInfoImpl(segmentSize - 1));
        segmentSize += storingUnit.getUnitLength();
        return true;
    }

    @Override
    public String read(String objectKey) throws IOException {
        var offset = segmentIndex.searchForKey(objectKey);
        if (offset.isEmpty()) {
            throw new IOException("No such key.");
        }

        try (final var inputStream = new DatabaseInputStream(Files.newInputStream(segmentPath))) {
            inputStream.skipNBytes(offset.get().getOffset());
            var storingUnit = inputStream.readDbUnit();
            if (storingUnit.isEmpty()) {
                throw new IOException("Indexing troubles.");
            }

            var key = new String(storingUnit.get().getKey());
            if (!key.equals(objectKey)) {
                throw new IOException("Indexing troubles.");
            }
            return new String(storingUnit.get().getValue());
        }
    }

    @Override
    public boolean isReadOnly() {
        return segmentSize >= Segment.DEFAULT_SIZE_IN_BYTES;
    }
}
