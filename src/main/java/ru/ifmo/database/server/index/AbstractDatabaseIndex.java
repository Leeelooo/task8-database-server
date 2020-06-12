package ru.ifmo.database.server.index;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class AbstractDatabaseIndex<K, V> implements DatabaseIndex<K, V> {
    private final Map<K, V> indexes = new HashMap<>();

    @Override
    public void onIndexedEntityUpdated(K key, V value) {
        indexes.put(key, value);
    }

    @Override
    public Optional<V> searchForKey(K key) {
        return Optional.ofNullable(indexes.get(key));
    }
}
