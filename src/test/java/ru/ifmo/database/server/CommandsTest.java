package ru.ifmo.database.server;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import ru.ifmo.database.DatabaseServer;
import ru.ifmo.database.server.console.DatabaseCommandResult;
import ru.ifmo.database.server.console.DatabaseCommands;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.impl.DatabaseInitializer;
import ru.ifmo.database.server.initialization.impl.DatabaseServerInitializer;
import ru.ifmo.database.server.initialization.impl.SegmentInitializer;
import ru.ifmo.database.server.initialization.impl.TableInitializer;
import ru.ifmo.database.server.logic.Database;
import ru.ifmo.database.server.logic.impl.DatabaseImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static ru.ifmo.database.server.console.DatabaseCommandResult.DatabaseCommandStatus.FAILED;
import static ru.ifmo.database.server.console.DatabaseCommandResult.DatabaseCommandStatus.SUCCESS;

@RunWith(MockitoJUnitRunner.class)
public class CommandsTest {

    private static final String DB_NAME = "db_1";

    private static final String TABLE_NAME = "tb_1";

    private static final String KEY_NAME = "key";

    private static final String VALUE = "value";

    private static final String TEST_PATH = "TestDir";

    private static final String DB_PATH = TEST_PATH + "/" + DB_NAME;

    @Mock
    public Database database;

    @Mock
    public ExecutionEnvironment env;

    public DatabaseServer server;

    public CommandsTest() {
    }

    @SneakyThrows
    @BeforeClass
    public static void createTestDir() {
        Files.createDirectories(Path.of(TEST_PATH));
    }

    @AfterClass
    public static void clearTestDir() throws IOException {
        FileUtils.deleteDirectory(Path.of(TEST_PATH).toFile());
    }

    @SneakyThrows
    @Before
    public void deleteDbDir() {
        when(env.getWorkingPath()).thenReturn(Path.of(TEST_PATH));
        server = new DatabaseServer(env,
                new DatabaseServerInitializer(new DatabaseInitializer(new TableInitializer(new SegmentInitializer()))));
        FileUtils.deleteDirectory(Path.of(DB_PATH).toFile());
    }

    // ================= update key tests =================

    @Test
    public void test_readKey_exception() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        var message = "Table already exists";
        doThrow(new DatabaseException(message)).when(database).read(TABLE_NAME, KEY_NAME);

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertEquals(message, result.getErrorMessage());
    }

    // ================= update key tests =================

    @Test
    public void test_updateKey_exception() throws DatabaseException {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        var message = "Table already exists";
        doThrow(new DatabaseException(message)).when(database).write(TABLE_NAME, KEY_NAME, VALUE);

        Command command = Command.builder()
                .name(DatabaseCommands.UPDATE_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertEquals(message, result.getErrorMessage());
    }

    // ================= create table tests =================

    @Test
    public void test_createTable_exception() throws DatabaseException {
        var message = "Table already exists";
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        doThrow(new DatabaseException(message)).when(database).createTableIfNotExists(TABLE_NAME);

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_TABLE.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertEquals(message, result.getErrorMessage());
    }

    @Test
    public void test_executeNext_nullCommandName() {
        DatabaseCommandResult databaseCommandResult = server.executeNextCommand((String) null);
        assertEquals(FAILED, databaseCommandResult.getStatus());
    }

    @Test
    public void test_executeNext_noCommandFound() {
        DatabaseCommandResult databaseCommandResult = server.executeNextCommand("fake_command_name");
        assertEquals(FAILED, databaseCommandResult.getStatus());
    }

    // ================= create database tests =================

    @Test
    public void test_createDb_success() {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_DATABASE.name())
                .dbName(DB_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
        assertTrue(Files.exists(Path.of(DB_PATH)));
    }

    @Test
    public void test_createDb_dbExists() throws IOException {
        Files.createDirectories(Path.of(DB_PATH));
        assertTrue(Files.exists(Path.of(DB_PATH)));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_DATABASE.name())
                .dbName(DB_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(StringUtils.isEmpty(result.getErrorMessage()));
    }

    @Test
    public void test_createDb_wrongName() throws IOException {
        String testPath = TEST_PATH + "/WrongName/" + DB_NAME;
        FileUtils.deleteDirectory(Path.of(testPath).toFile());
        when(env.getDatabase("WrongName/" + DB_NAME)).thenReturn(Optional.of(database));

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_DATABASE.name())
                .dbName("WrongName/" + DB_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(StringUtils.isEmpty(result.getErrorMessage()));
        assertFalse(Files.exists(Path.of(testPath)));
    }

    // ================= create table tests =================

    @Test
    public void test_createTable_success() throws DatabaseException {
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_TABLE.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
    }

    @Test
    public void test_createTable_noSuchDb() {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_TABLE.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(StringUtils.isEmpty(result.getErrorMessage()));
    }

    @Test
    public void test_createTable_tableExists() throws DatabaseException {
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        database.createTableIfNotExists(TABLE_NAME);

        Command command = Command.builder()
                .name(DatabaseCommands.CREATE_TABLE.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(StringUtils.isEmpty(result.getErrorMessage()));
    }

    // ================= update key tests =================

    @Test
    public void test_updateKey_success() throws DatabaseException {
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        database.createTableIfNotExists(TABLE_NAME);

        Command command = Command.builder()
                .name(DatabaseCommands.UPDATE_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
        assertEquals(VALUE, database.read(TABLE_NAME, KEY_NAME));
    }

    @Test
    public void test_updateKey_noSuchDb() {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.UPDATE_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(StringUtils.isEmpty(result.getErrorMessage()));
    }

    @Test
    public void test_updateKey_noSuchTable() throws DatabaseException {
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));

        Command command = Command.builder()
                .name(DatabaseCommands.UPDATE_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .value(VALUE)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(StringUtils.isEmpty(result.getErrorMessage()));
    }

    // ================= read key tests =================

    @Test
    public void test_readKey_success() throws DatabaseException {
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        database.createTableIfNotExists(TABLE_NAME);
        database.write(TABLE_NAME, KEY_NAME, VALUE);

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(SUCCESS, result.getStatus());
        assertEquals(Optional.of(VALUE), result.getResult());
    }

    @Test
    public void test_readKey_noSuchDb() {
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.empty());

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(StringUtils.isEmpty(result.getErrorMessage()));
    }

    @Test
    public void test_readKey_noSuchTable() throws DatabaseException {
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(StringUtils.isEmpty(result.getErrorMessage()));
    }

    @Test
    public void test_readKey_noSuchKey() throws DatabaseException {
        database = DatabaseImpl.create(DB_NAME, Path.of(TEST_PATH));
        when(env.getDatabase(DB_NAME)).thenReturn(Optional.of(database));
        database.createTableIfNotExists(TABLE_NAME);

        Command command = Command.builder()
                .name(DatabaseCommands.READ_KEY.name())
                .dbName(DB_NAME)
                .tableName(TABLE_NAME)
                .key(KEY_NAME)
                .build();

        DatabaseCommandResult result = server.executeNextCommand(command.toString());
        assertEquals(FAILED, result.getStatus());
        assertFalse(StringUtils.isEmpty(result.getErrorMessage()));
    }

    @Builder
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    static class Command {

        private String name;
        private String dbName;
        private String tableName;
        private String key;
        private String value;

        @Override
        public String toString() {
            return Stream.of(name, dbName, tableName, key, value)
                    .filter(Objects::nonNull)
                    .collect(Collectors.joining(" "));
        }
    }
}