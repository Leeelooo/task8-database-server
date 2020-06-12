package ru.ifmo.database.server.cache;

public interface Cache {
    String get(String key);

    void set(String key, String value);
}
