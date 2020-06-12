package ru.ifmo.database.server.console;

import ru.ifmo.database.server.console.impl.*;
import ru.ifmo.database.server.logic.impl.DatabaseFactoryImpl;
import ru.ifmo.database.server.utils.StringUtils;

import java.util.stream.Stream;

public enum DatabaseCommands {
    CREATE_DATABASE() {
        @Override
        protected DatabaseCommand createCommand(ExecutionEnvironment environment, String... options) {
            return new CreateDatabaseCommand(environment, new DatabaseFactoryImpl(), options[1]);
        }

        @Override
        public int getOptionsCount() {
            return 2;
        }
    },
    CREATE_TABLE() {
        @Override
        protected DatabaseCommand createCommand(ExecutionEnvironment environment, String... options) {
            return new CreateTableCommand(environment, options[1], options[2]);
        }

        @Override
        public int getOptionsCount() {
            return 3;
        }
    },
    READ_KEY() {
        @Override
        protected DatabaseCommand createCommand(ExecutionEnvironment environment, String... options) {
            return new ReadKeyCommand(environment, options[1], options[2], options[3]);
        }

        @Override
        protected int getOptionsCount() {
            return 4;
        }
    },
    UPDATE_KEY() {
        @Override
        protected DatabaseCommand createCommand(ExecutionEnvironment environment, String... options) {
            return new UpdateKeyCommand(environment, options[1], options[2], options[3], options[4]);
        }

        @Override
        protected int getOptionsCount() {
            return 5;
        }
    };

    public static DatabaseCommand of(
            ExecutionEnvironment environment,
            String input
    ) {
        if (StringUtils.isEmptyOrNull(input)) {
            return new InvalidCommand("Empty or null input.");
        }
        var options = input.split(" ");
        if (Stream.of(DatabaseCommands.values())
                .noneMatch(s -> s.name().equals(options[0]))) {
            return new InvalidCommand(String.format("Unknown command %s passed.", options[0]));
        }
        return DatabaseCommands.valueOf(options[0]).getCommand(environment, options);
    }

    private static final DatabaseCommand WRONG_PARAMS_COUNT = new InvalidCommand("Wrong params count.");

    protected abstract DatabaseCommand createCommand(ExecutionEnvironment environment, String... options);
    protected abstract int getOptionsCount();

    public DatabaseCommand getCommand(ExecutionEnvironment environment, String... options) {
        return isOptionsCountCorrect(options.length)
                ? createCommand(environment, options)
                : WRONG_PARAMS_COUNT;
    }

    protected boolean isOptionsCountCorrect(int optionsCount) {
        return getOptionsCount() == optionsCount;
    }

}
