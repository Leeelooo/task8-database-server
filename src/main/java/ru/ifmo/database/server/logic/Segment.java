package ru.ifmo.database.server.logic;

import ru.ifmo.database.server.exception.DatabaseException;

import java.io.IOException;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public interface Segment {
    int DEFAULT_SIZE_IN_BYTES = 100_000;

    String getName();

    // todo sukhoa in future may return something like SegmentWriteResult .. with report and error details?
    // for new returns false if cannot allocate requested capacity
    // exception is questionable
    boolean write(String objectKey, String objectValue) throws IOException, DatabaseException;

    String read(String objectKey) throws IOException;

    boolean isReadOnly();
}