package ru.ifmo.database.server;

import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import ru.ifmo.database.server.cache.DatabaseCache;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.Table;
import ru.ifmo.database.server.logic.impl.CachingTable;
import ru.ifmo.database.server.logic.impl.DatabaseImpl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CacheTest {

    private static final String TEST_PATH = "TestDir";

    private static final String DB_NAME = "db_1";

    private static final String TABLE_NAME = "tb_1";

    private static final String KEY_NAME = "key";

    private static final String VALUE = "testValue";

    private static final String DB_PATH = TEST_PATH + "/" + DB_NAME;

    @Mock
    public ExecutionEnvironment env;

    @Mock
    public Database database;

    @Mock
    public Table mockTable;

    @Mock
    public DatabaseCache mockCache;

    @InjectMocks
    public Table cachingTable = new CachingTable(mockTable, mockCache);

    @Test
    @SneakyThrows
    public void test_usingCache() {
        when(mockCache.get(KEY_NAME)).thenReturn(VALUE);

        assertEquals(cachingTable.read(KEY_NAME), VALUE);
        verifyZeroInteractions(mockTable);
    }

    @Test
    @SneakyThrows
    public void test_noCache() {
        when(mockTable.read(KEY_NAME)).thenReturn(VALUE);

        assertEquals(cachingTable.read(KEY_NAME), VALUE);
        verify(mockCache).get(KEY_NAME);
    }

    @Test
    public void test_correctName() {
        String name = "name";
        when(mockTable.getName()).thenReturn(name);

        assertEquals(cachingTable.getName(), name);
    }


    @Test
    @SneakyThrows
    public void test_writeToCache() {
        cachingTable.write(KEY_NAME, VALUE);
        verify(mockCache).set(KEY_NAME, VALUE);
        verify(mockTable).write(KEY_NAME, VALUE);
    }

    @Test
    public void test_cache() {
        DatabaseCache cache = new DatabaseCache();
        String key = "testKey";
        List<String> values = List.of("testValue1", "testValue2", "");

        for (String value : values) {
            cache.set(key, value);
            assertEquals(value, cache.get(key));
        }
    }

    @Test
    @SneakyThrows
    public void test_dbUsesCache() throws DatabaseException {
        when(env.getWorkingPath()).thenReturn(Path.of(TEST_PATH));
        Files.createDirectories(Path.of(TEST_PATH));
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        database.createTableIfNotExists(TABLE_NAME);

        database.write(TABLE_NAME, KEY_NAME, VALUE);
        FileUtils.deleteDirectory(Path.of(DB_PATH).toFile());

        String value = database.read(TABLE_NAME, KEY_NAME);

        assertEquals(VALUE, value);
        FileUtils.deleteDirectory(Path.of(TEST_PATH).toFile());
    }
}