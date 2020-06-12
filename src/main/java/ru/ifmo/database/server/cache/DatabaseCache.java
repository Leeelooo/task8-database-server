package ru.ifmo.database.server.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCache implements Cache {
    private static final float LOAD_FACTOR = 0.75f;

    private final Map<String, String> cache;

    public DatabaseCache() {
        this(10);
    }

    public DatabaseCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity should be more than zero.");
        }
        cache = new LinkedHashMap<>(capacity, LOAD_FACTOR, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return cache.size() >= capacity;
            }
        };
    }

    @Override
    public String get(String key) {
        return cache.get(key);
    }

    @Override
    public void set(String key, String value) {
        cache.put(key, value);
    }
}
