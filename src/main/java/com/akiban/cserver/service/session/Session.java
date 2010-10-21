package com.akiban.cserver.service.session;

public interface Session {
    <T> T get(String module, Object key);
    <T> T put(String module, Object key, T item);
    <T> T remove(String module, Object key);
}
